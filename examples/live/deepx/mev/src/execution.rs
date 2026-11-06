// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! One Spot opportunity with durable submission and finalized fill reconciliation.

use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};

use alloy_primitives::U256;
use anyhow::Context;
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    enums::DeepXEnvironment,
    rpc::{
        DeepXRpcClient,
        event_storage::get_system_events,
        events::DeepXFinalizedSpotTrade,
        runtime_snapshot::{observe_current_runtime_snapshot, observe_finalized_checkpoint},
        spot_place::DeepXSpotPlaceExpected,
        timestamp::query_timestamp_now_at_block,
        trade_event_decode::decode_finalized_spot_trade_events,
    },
    signing::{
        DeepXPostOnlyParam, DeepXRuntimeSnapshotService, DeepXSpotOrderType, DeepXSpotPlaceParams,
        DeepXTimeInForce, RuntimeSnapshot, derive_signer_account_id, sign_spot_place_order,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use subxt_core::{
    Metadata,
    blocks::Extrinsics,
    dynamic,
    events::{Events, Phase},
    ext::scale_value::{At, Value as ScaleValue, scale::decode_as_type},
    storage,
};

use super::{
    journal::{Inclusion, Journal, Operation, State, amount},
    ordering, spot,
};

const NONCE_PAST_RANGE: u64 = 3_600_000;

mod delegation;
mod native;

/// Inspect one wallet at a single finalized block without loading a signing key.
pub(super) async fn inspect_wallet(rpc: &DeepXRpcClient, wallet: [u8; 20]) -> anyhow::Result<()> {
    let checkpoint = observe_finalized_checkpoint(rpc).await?;
    let head = encoded(checkpoint.block_hash());
    let raw_metadata = super::get_metadata(rpc, &head).await?;
    let bytes = super::decode_runtime_metadata(&raw_metadata).context("invalid wallet metadata")?;
    let metadata = subxt_core::metadata::decode_from(&bytes[..])?;
    let stats = read_storage(rpc, &metadata, &head, "Subaccount", "UserStatsFor", wallet).await?;
    let subaccounts = stats
        .at("subaccounts")
        .context("missing wallet subaccounts")?;
    let subxt_core::ext::scale_value::ValueDef::Composite(subaccounts) = &subaccounts.value else {
        anyhow::bail!("invalid wallet subaccounts");
    };
    anyhow::ensure!(
        subaccounts.len() <= 256,
        "wallet directory exceeds inspection bound"
    );
    let mut addresses = std::collections::BTreeSet::new();
    let mut accounts = Vec::new();
    for subaccount in subaccounts.values() {
        let address = super::fixed_bytes::<20>(subaccount)?;
        anyhow::ensure!(addresses.insert(address), "duplicate wallet subaccount");
        let account = read_storage(
            rpc,
            &metadata,
            &head,
            "Subaccount",
            "SubaccountInfo",
            address,
        )
        .await?;
        anyhow::ensure!(
            super::fixed_bytes::<20>(account.at("authority").context("missing authority")?)?
                == wallet,
            "wallet directory authority mismatch"
        );
        anyhow::ensure!(
            super::fixed_bytes::<20>(account.at("address").context("missing address")?)? == address,
            "wallet directory address mismatch"
        );
        // SCALE display retains full integer precision, including u128/U256 balances.
        let positions = deepx_rust_sdk::rpc::lending_positions::get_lending_positions_at_block(
            rpc,
            checkpoint.block_hash(),
            address,
        )
        .await?;
        let positions = positions
            .into_iter()
            .map(|position| {
                json!({
                    "market_id":position.market_id, "asset_hex":encoded(&position.asset),
                    "scaled_deposit":position.scaled_deposit.to_string(),
                    "scaled_borrow":position.scaled_borrow.to_string()
                })
            })
            .collect::<Vec<_>>();
        accounts.push(
            json!({"subaccount":encoded(address), "decoded_scale":account.to_string(),
            "lending_positions":positions}),
        );
    }
    let delegates = read_storage(
        rpc,
        &metadata,
        &head,
        "Subaccount",
        "DelegateAccountsFor",
        wallet,
    )
    .await?;
    let system = read_storage(rpc, &metadata, &head, "System", "Account", wallet).await?;
    let timestamp = query_timestamp_now_at_block(rpc, checkpoint.block_hash()).await?;
    anyhow::ensure!(
        rpc.get_block_hash_required_once(u64::from(checkpoint.block_number()))
            .await?
            == head,
        "wallet inspection finalized checkpoint changed"
    );
    println!(
        "{}",
        json!({
            "event":"finalized_wallet_inspection", "wallet":encoded(wallet),
            "block_number":checkpoint.block_number(), "block_hash":head, "chain_time_ms":timestamp,
            "delegates_decoded_scale":delegates.to_string(),
            "system_account_decoded_scale":system.to_string(), "subaccounts":accounts
        })
    );
    Ok(())
}

pub(super) struct Settings {
    pub(super) journal: PathBuf,
    pub(super) subaccount: [u8; 20],
    pub(super) wallet: Option<[u8; 20]>,
    pub(super) maximum_loss: u128,
    pub(super) exit_after_blocks: u64,
}

pub(super) struct Execution {
    journal: Journal,
    settings: Settings,
    key: DeepXPrivateKey,
    signer: [u8; 20],
    genesis: [u8; 32],
    pair: [u8; 32],
    _account_lock: File,
    _wallet_lock: Option<File>,
}

impl Execution {
    pub(super) fn open(
        settings: Settings,
        genesis: [u8; 32],
        pair: [u8; 32],
    ) -> anyhow::Result<Self> {
        let journal = Journal::open(&settings.journal)?;
        let key =
            DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
        let signer = derive_signer_account_id(&key)?;
        let lock_path = std::env::temp_dir().join(format!(
            "deepx-mev-{}-{}.lock",
            hex::encode(genesis),
            hex::encode(signer)
        ));
        let account_lock = lock_account(&lock_path)?;
        let wallet_lock = if let Some(wallet) = settings.wallet.filter(|wallet| *wallet != signer) {
            Some(lock_account(&std::env::temp_dir().join(format!(
                "deepx-mev-{}-{}.lock",
                hex::encode(genesis),
                hex::encode(wallet)
            )))?)
        } else {
            None
        };
        anyhow::ensure!(
            settings.exit_after_blocks > 0,
            "exit deadline must be positive"
        );
        Ok(Self {
            journal,
            settings,
            key,
            signer,
            genesis,
            pair,
            _account_lock: account_lock,
            _wallet_lock: wallet_lock,
        })
    }

    pub(super) fn wallet(&self) -> [u8; 20] {
        self.settings.wallet.unwrap_or(self.signer)
    }

    /// Validate signing identity and cash-account prerequisites without signing or submitting.
    pub(super) async fn check_account(&self, rpc: &DeepXRpcClient) -> anyhow::Result<()> {
        let observed =
            observe_current_runtime_snapshot(rpc, &DeepXEnvironment::Testnet, self.genesis).await?;
        let head = encoded(observed.block_hash());
        let snapshot = observed.into_snapshot();
        validate_storage_interface(snapshot.metadata())?;
        let account = read_storage(
            rpc,
            snapshot.metadata(),
            &head,
            "Subaccount",
            "SubaccountInfo",
            self.settings.subaccount,
        )
        .await?;
        anyhow::ensure!(
            super::fixed_bytes::<20>(account.at("authority").context("missing authority")?)?
                == self.wallet()
                && super::fixed_bytes::<20>(account.at("address").context("missing address")?)?
                    == self.settings.subaccount,
            "execution account identity mismatch"
        );
        require_idle_account(&account)?;
        anyhow::ensure!(
            account
                .at("spot_margin_trading_enabled")
                .and_then(ScaleValue::as_bool)
                == Some(false),
            "execution requires Spot margin disabled"
        );
        let pending = rpc.get_pending_extrinsics().await?;
        if self.wallet() != self.signer {
            self.check_delegation(rpc, snapshot.metadata(), &head, &pending)
                .await?;
        } else {
            delegation::require_no_shared_pending(
                snapshot.metadata(),
                &pending,
                &std::collections::BTreeSet::from([self.signer]),
            )?;
            let address = dynamic::storage(
                "Subaccount",
                "DelegateAccountsFor",
                vec![ScaleValue::from_bytes(self.signer)],
            );
            if let Some(raw) = rpc
                .get_storage(
                    &encoded(storage::get_address_bytes(&address, snapshot.metadata())?),
                    &head,
                )
                .await?
            {
                anyhow::ensure!(
                    decode_hex(&raw)? == [0],
                    "execution account must have no configured delegates"
                );
            }
        }
        native::require_idle(rpc, snapshot.metadata(), &head, self.settings.subaccount).await?;
        let positions = deepx_rust_sdk::rpc::lending_positions::get_lending_positions_at_block(
            rpc,
            super::parse_hash(&head)?,
            self.settings.subaccount,
        )
        .await?;
        anyhow::ensure!(
            positions.iter().all(|p| p.scaled_borrow == 0),
            "execution account has lending borrows"
        );
        let timestamp = query_timestamp_now_at_block(rpc, super::parse_hash(&head)?).await?;
        let nonce = ordering::observe(rpc, snapshot.metadata(), &head, self.wallet(), &pending)
            .await?
            .allocate_exit(timestamp)?;
        println!(
            "{}",
            json!({"event":"execution_account_checked", "observed_head":head,
            "wallet":encoded(self.wallet()), "signer":encoded(self.signer),
            "subaccount":encoded(self.settings.subaccount), "quota_nonce_account":encoded(self.wallet()),
            "available_exit_nonce":nonce.to_string(), "lending_positions":positions.len()})
        );
        Ok(())
    }

    pub(super) async fn resume(&self, rpc: &DeepXRpcClient, poll_ms: u64) -> anyhow::Result<bool> {
        if let Some(state) = self.journal.load()? {
            self.validate_identity(&state)?;
            eprintln!(
                "{}",
                json!({"event":"execution_resuming", "target_hash":state.target_hash,
                "entry_hash":state.entry.hash, "cursor_number":state.cursor_number})
            );
            self.run(rpc, state, poll_ms).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) async fn consider(
        &self,
        rpc: &DeepXRpcClient,
        order: &Value,
        settings: &spot::Settings,
        poll_ms: u64,
    ) -> anyhow::Result<bool> {
        if order["spot_plan"]["conditional_opportunity"] != true {
            eprintln!(
                "{}",
                json!({"event":"candidate_skipped", "reason":"spot_plan_rejected",
                "spot_plan":order["spot_plan"]})
            );
            return Ok(false);
        }
        eprintln!(
            "{}",
            json!({"event":"candidate_rechecking", "observed_head":order["observed_head"]})
        );
        anyhow::ensure!(
            self.journal.load()?.is_none(),
            "journal already contains an opportunity"
        );
        let observed =
            observe_current_runtime_snapshot(rpc, &DeepXEnvironment::Testnet, self.genesis).await?;
        let head = encoded(observed.block_hash());
        let snapshot = observed.into_snapshot();
        let metadata = snapshot.metadata();
        validate_storage_interface(metadata)?;
        let pending = rpc.get_pending_extrinsics().await?;
        for encoded in &pending {
            let extrinsics = Extrinsics::<super::DeepXRuntimeConfig>::decode_from(
                vec![decode_hex(encoded)?],
                metadata.clone(),
            )?;
            let details = extrinsics
                .iter()
                .next()
                .context("missing pending operation")?;
            anyhow::ensure!(
                details.address_bytes() != Some(&self.signer[..]),
                "execution signer has another pending operation"
            );
        }
        let Some((target_bytes, fresh_order)) = find_target(metadata, &pending, self.pair, order)?
        else {
            eprintln!(
                "{}",
                json!({"event":"candidate_skipped", "reason":"target_left_pool"})
            );
            return Ok(false);
        };
        anyhow::ensure!(
            fresh_order["signer"] != encoded(self.signer),
            "cannot target own signer"
        );
        anyhow::ensure!(
            fresh_order["subaccount"] != encoded(self.settings.subaccount),
            "cannot target own subaccount"
        );
        let book = spot::observe(rpc, metadata, &head, self.pair).await?;
        let plan = match book.plan(&fresh_order, settings) {
            Ok(plan) => plan,
            Err(e) => {
                eprintln!("Opportunity no longer meets entry bounds: {e}");
                return Ok(false);
            }
        };
        let target_nonce: u64 = text_field(&fresh_order, "nonce")?.parse()?;
        let nonces = ordering::observe(rpc, metadata, &head, self.wallet(), &pending)
            .await?
            .allocate(target_nonce)?;
        let account = read_storage(
            rpc,
            metadata,
            &head,
            "Subaccount",
            "SubaccountInfo",
            self.settings.subaccount,
        )
        .await?;
        anyhow::ensure!(
            super::fixed_bytes::<20>(
                account
                    .at("authority")
                    .context("missing subaccount authority")?
            )? == self.wallet(),
            "execution subaccount authority does not match configured wallet"
        );
        anyhow::ensure!(
            super::fixed_bytes::<20>(
                account
                    .at("address")
                    .context("missing subaccount address")?
            )? == self.settings.subaccount,
            "subaccount address mismatch"
        );
        anyhow::ensure!(
            account
                .at("spot_margin_trading_enabled")
                .and_then(ScaleValue::as_bool)
                == Some(false),
            "execution requires Spot margin disabled"
        );
        require_idle_account(&account)?;
        native::require_idle(rpc, metadata, &head, self.settings.subaccount).await?;
        let positions = deepx_rust_sdk::rpc::lending_positions::get_lending_positions_at_block(
            rpc,
            super::parse_hash(&head)?,
            self.settings.subaccount,
        )
        .await?;
        anyhow::ensure!(
            positions.iter().all(|p| p.scaled_borrow == 0),
            "execution account has lending borrows"
        );
        if self.wallet() != self.signer {
            self.check_delegation(rpc, metadata, &head, &pending)
                .await?;
        } else {
            let delegates = dynamic::storage(
                "Subaccount",
                "DelegateAccountsFor",
                vec![dynamic::Value::from_bytes(self.signer)],
            );
            let delegates_key = encoded(storage::get_address_bytes(&delegates, metadata)?);
            if let Some(raw) = rpc.get_storage(&delegates_key, &head).await? {
                anyhow::ensure!(
                    decode_hex(&raw)? == [0],
                    "execution account must have no configured delegates"
                );
            }
        }
        let target_account = read_storage(
            rpc,
            metadata,
            &head,
            "Subaccount",
            "SubaccountInfo",
            parse_account(text_field(&fresh_order, "subaccount")?)?,
        )
        .await?;
        anyhow::ensure!(
            super::fixed_bytes::<20>(
                target_account
                    .at("authority")
                    .context("missing target authority")?
            )? != self.wallet(),
            "target is owned by the execution account"
        );
        let code_hash = super::runtime_code_hash(rpc, &head).await?;
        let (base_symbol, quote_symbol) = market_symbols(rpc, metadata, &head, self.pair).await?;
        let checkpoint = observe_finalized_checkpoint(rpc).await?;
        let quote = exact_quote(plan.front_size, plan.front_price, book.base_scale)?;
        anyhow::ensure!(
            quote <= plan.front_quote_budget,
            "entry mapping exceeds quote budget"
        );
        let entry = self.sign(snapshot, true, plan.front_size, quote, nonces.front)?;
        let state = State {
            version: 1,
            genesis: encoded(self.genesis),
            signer: encoded(self.signer),
            wallet: (self.wallet() != self.signer).then(|| encoded(self.wallet())),
            subaccount: encoded(self.settings.subaccount),
            pair: encoded(self.pair),
            runtime_code_hash: encoded(code_hash),
            target_bytes,
            target_hash: text_field(&fresh_order, "transaction_hash")?.to_owned(),
            target_nonce,
            target_subaccount: text_field(&fresh_order, "subaccount")?.to_owned(),
            target_outcome: None,
            target_first_trade: None,
            entry_first_trade: None,
            entry_last_trade: None,
            cursor_number: u64::from(checkpoint.block_number()),
            cursor_hash: encoded(checkpoint.block_hash()),
            exit_after_blocks: self.settings.exit_after_blocks,
            maximum_loss: self.settings.maximum_loss.to_string(),
            cost_budget: settings.cost_budget.to_string(),
            minimum_profit: settings.minimum_profit.to_string(),
            exit_price: plan.back_price.to_string(),
            base_scale: book.base_scale.to_string(),
            base_symbol,
            quote_symbol,
            acquired_base: "0".into(),
            spent_quote: "0".into(),
            sold_base: "0".into(),
            received_quote: "0".into(),
            entry,
            exits: Vec::new(),
            complete: false,
            result: None,
        };

        // Reserve exact signed bytes before any network write, including a possible crash
        self.journal.store(&state)?;
        eprintln!(
            "{}",
            json!({"event":"entry_prepared", "target_hash":state.target_hash,
            "entry_hash":state.entry.hash, "raw_base":state.entry.base,
            "raw_quote":state.entry.quote, "nonce":state.entry.nonce})
        );
        self.run(rpc, state, poll_ms).await?;
        Ok(true)
    }

    fn validate_identity(&self, state: &State) -> anyhow::Result<()> {
        anyhow::ensure!(
            state.genesis == encoded(self.genesis)
                && state.signer == encoded(self.signer)
                && state.wallet.as_deref().unwrap_or(&state.signer) == encoded(self.wallet())
                && state.subaccount == encoded(self.settings.subaccount)
                && state.pair == encoded(self.pair),
            "journal chain/account/market identity mismatch"
        );
        anyhow::ensure!(
            state.maximum_loss == self.settings.maximum_loss.to_string()
                && state.exit_after_blocks == self.settings.exit_after_blocks,
            "journal risk settings mismatch"
        );
        Ok(())
    }

    fn sign(
        &self,
        snapshot: RuntimeSnapshot,
        is_buy: bool,
        base: u128,
        quote: u128,
        nonce: u64,
    ) -> anyhow::Result<Operation> {
        let service = DeepXRuntimeSnapshotService::new(snapshot);
        let permit = service.acquire()?;
        let signed =
            sign_spot_place_order(&permit, &self.key, self.params(is_buy, base, quote), nonce)?;
        permit.ensure_active()?;
        Ok(Operation {
            nonce,
            bytes: encoded(signed.bytes()),
            hash: encoded(signed.extrinsic_hash()),
            base: base.to_string(),
            quote: quote.to_string(),
            attempted: false,
            submission_error: None,
            included: None,
            expired: false,
            terminal: false,
        })
    }

    fn params(&self, is_buy: bool, base: u128, quote: u128) -> DeepXSpotPlaceParams {
        DeepXSpotPlaceParams {
            subaccount: self.settings.subaccount,
            pair: self.pair,
            is_buy,
            base_amount: U256::from(base).to_le_bytes(),
            quote_amount: U256::from(quote).to_le_bytes(),
            order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc),
            post_only: DeepXPostOnlyParam::None,
            reduce_only: false,
        }
    }

    async fn run(
        &self,
        rpc: &DeepXRpcClient,
        mut state: State,
        poll_ms: u64,
    ) -> anyhow::Result<()> {
        tokio::select! {
            result = self.run_inner(rpc, &mut state, poll_ms) => result,
            result = tokio::signal::ctrl_c() => {
                result?;
                anyhow::bail!("execution interrupted; restart with the same journal to reconcile")
            }
        }
    }

    async fn run_inner(
        &self,
        rpc: &DeepXRpcClient,
        state: &mut State,
        poll_ms: u64,
    ) -> anyhow::Result<()> {
        loop {
            self.validate_identity(state)?;
            if state.complete {
                println!(
                    "{}",
                    json!({"event": "execution_complete", "result": state.result,
                    "net_quote_after_cost_budget": state.profit()?, "remaining_raw_base": state.remaining()?.to_string(),
                    "sandwich_fill_order_observed": state.entry_last_trade.zip(state.target_first_trade).is_some_and(|(entry, target)| entry < target),
                    "entry_first_trade": state.entry_first_trade, "target_first_trade": state.target_first_trade})
                );
                return Ok(());
            }
            self.reconcile(rpc, state).await?;
            if state.entry.settled()
                && state.exits.iter().all(Operation::settled)
                && state.remaining()? == 0
            {
                state.complete = true;
                state.result = Some(
                    if state.entry_first_trade.is_some() {
                        "flat"
                    } else {
                        "entry_unfilled"
                    }
                    .into(),
                );
                self.journal.store(state)?;
                continue;
            }
            let outstanding = if !state.entry.settled() {
                Some((true, 0))
            } else {
                state
                    .exits
                    .iter()
                    .enumerate()
                    .find(|(_, op)| !op.settled())
                    .map(|(i, _)| (false, i))
            };
            if let Some((is_buy, index)) = outstanding {
                let operation = if is_buy {
                    &state.entry
                } else {
                    &state.exits[index]
                };
                if !operation.attempted {
                    let bytes = decode_hex(&operation.bytes)?;
                    let hash = super::parse_hash(&operation.hash)?;
                    let head = rpc.get_best_block_hash_once().await?;
                    anyhow::ensure!(
                        encoded(super::runtime_code_hash(rpc, &head).await?)
                            == state.runtime_code_hash,
                        "runtime changed before submission"
                    );
                    if is_buy {
                        let pending = rpc.get_pending_extrinsics().await?;
                        if !pending
                            .iter()
                            .any(|item| item.eq_ignore_ascii_case(&state.target_bytes))
                        {
                            state.complete = true;
                            state.result = Some("target_left_pool_before_entry".into());
                            self.journal.store(state)?;
                            continue;
                        }
                        if !self.revalidate_entry(rpc, state, &head, &pending).await? {
                            state.complete = true;
                            state.result = Some("entry_bounds_changed".into());
                            self.journal.store(state)?;
                            continue;
                        }
                    } else {
                        let raw = super::get_metadata(rpc, &head).await?;
                        let bytes = super::decode_runtime_metadata(&raw)
                            .context("invalid exit metadata")?;
                        let metadata = subxt_core::metadata::decode_from(&bytes[..])?;
                        let pending = rpc.get_pending_extrinsics().await?;
                        if self.wallet() != self.signer {
                            self.check_delegation(rpc, &metadata, &head, &pending)
                                .await?;
                        }
                        ordering::observe(rpc, &metadata, &head, self.wallet(), &pending)
                            .await?
                            .require_available(operation.nonce)?;
                    }

                    // A crash after this checkpoint is ambiguous; recovery never blindly resends
                    if is_buy {
                        state.entry.attempted = true;
                    } else {
                        state.exits[index].attempted = true;
                    }
                    self.journal.store(state)?;
                    eprintln!(
                        "{}",
                        json!({"event":"submission_started", "is_buy":is_buy,
                        "extrinsic_hash":encoded(hash)})
                    );
                    let result = rpc.submit_verified_extrinsic_once(&bytes, hash).await;
                    eprintln!(
                        "{}",
                        json!({"event":"submission_returned", "is_buy":is_buy,
                        "extrinsic_hash":encoded(hash), "rpc_accepted":result.is_ok(),
                        "requires_finalized_reconciliation":true})
                    );
                    if let Err(e) = result {
                        let message = e.to_string();
                        eprintln!("Submission requires finalized reconciliation: {message}");
                        if is_buy {
                            state.entry.submission_error = Some(message);
                        } else {
                            state.exits[index].submission_error = Some(message);
                        }
                        self.journal.store(state)?;
                    }
                }
            } else {
                self.prepare_exit(rpc, state).await?;
            }
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;
        }
    }

    async fn prepare_exit(&self, rpc: &DeepXRpcClient, state: &mut State) -> anyhow::Result<()> {
        let entry_number = state
            .entry
            .included
            .as_ref()
            .context("inventory without finalized entry")?
            .number;
        let deadline = entry_number
            .checked_add(state.exit_after_blocks)
            .context("exit deadline overflow")?;
        let emergency = state.target_outcome == Some(false)
            || state.cursor_number >= deadline
            || state
                .target_first_trade
                .zip(state.entry_last_trade)
                .is_some_and(|(target, entry)| target <= entry);
        if state.target_outcome != Some(true) && !emergency {
            return Ok(());
        }
        let observed =
            observe_current_runtime_snapshot(rpc, &DeepXEnvironment::Testnet, self.genesis).await?;
        let head = encoded(observed.block_hash());
        anyhow::ensure!(
            encoded(super::runtime_code_hash(rpc, &head).await?) == state.runtime_code_hash,
            "runtime changed with outstanding inventory"
        );
        let snapshot = observed.into_snapshot();
        let book = spot::observe(rpc, snapshot.metadata(), &head, self.pair).await?;
        anyhow::ensure!(
            book.base_scale == amount(&state.base_scale)?,
            "base units changed"
        );
        let base = book.executable_exit_size(state.remaining()?)?;
        if base < book.minimum_size {
            state.complete = true;
            state.result = Some("residual_below_executable_lot".into());
            self.journal.store(state)?;
            return Ok(());
        }
        let price = if emergency {
            let allowed_loss = amount(&state.maximum_loss)?;
            let costs = amount(&state.spent_quote)?
                .checked_add(amount(&state.cost_budget)?)
                .context("exit cost overflow")?;
            let required = costs
                .saturating_sub(allowed_loss)
                .saturating_sub(amount(&state.received_quote)?);
            anyhow::ensure!(
                book.taker_fee < 1_000_000,
                "exit taker fee consumes all proceeds"
            );
            let numerator = U256::from(required) * U256::from(1_000_000);
            let denominator = U256::from(1_000_000 - book.taker_fee);
            let gross = narrow(
                numerator / denominator + U256::from(numerator % denominator != U256::ZERO),
            )?;
            loss_floor(gross, base, book.base_scale, book.tick)?
        } else {
            amount(&state.exit_price)?
        };
        if base < book.minimum_size || price == 0 {
            anyhow::bail!(
                "remaining inventory is below the executable lot; journal retained for recovery"
            );
        }
        if !book.has_bid(price) {
            return Ok(());
        }
        let quote = exact_quote(base, price, book.base_scale)?;
        anyhow::ensure!(
            quote >= book.minimum_notional,
            "exit is below minimum notional; journal retained"
        );
        let pending = rpc.get_pending_extrinsics().await?;
        let anchor = state
            .target_nonce
            .max(state.exits.last().map_or(state.entry.nonce, |op| op.nonce));
        if self.wallet() != self.signer {
            self.check_delegation(rpc, snapshot.metadata(), &head, &pending)
                .await?;
        }
        let nonce = ordering::observe(rpc, snapshot.metadata(), &head, self.wallet(), &pending)
            .await?
            .allocate_exit(anchor)?;
        state
            .exits
            .push(self.sign(snapshot, false, base, quote, nonce)?);
        self.journal.store(state)?;
        println!(
            "{}",
            json!({"event": "exit_prepared", "emergency": emergency,
            "raw_base": base.to_string(), "raw_price": price.to_string()})
        );
        Ok(())
    }

    async fn revalidate_entry(
        &self,
        rpc: &DeepXRpcClient,
        state: &State,
        head: &str,
        pending: &[String],
    ) -> anyhow::Result<bool> {
        let metadata_hex = super::get_metadata(rpc, head).await?;
        let bytes =
            super::decode_runtime_metadata(&metadata_hex).context("invalid entry metadata")?;
        let metadata = subxt_core::metadata::decode_from(&bytes[..])?;
        let target = super::decode_order(
            &metadata,
            decode_hex(&state.target_bytes)?,
            &super::Market::Spot(self.pair),
            1,
        )?
        .context("journal target is not a Spot order")?;
        let book = spot::observe(rpc, &metadata, head, self.pair).await?;
        let settings = spot::Settings {
            maximum_front_size: amount(&state.entry.base)?,
            minimum_profit: amount(&state.minimum_profit)?,
            cost_budget: amount(&state.cost_budget)?,
        };
        let plan = match book.plan(&target, &settings) {
            Ok(plan) => plan,
            Err(_) => return Ok(false),
        };
        let required = amount(&state.entry.quote)?
            .checked_add(settings.cost_budget)
            .and_then(|n| n.checked_add(settings.minimum_profit))
            .context("entry profit bound overflow")?;
        if plan.front_size != settings.maximum_front_size
            || plan.minimum_back_proceeds < required
            || plan.front_quote_budget > amount(&state.entry.quote)?
            || book.base_scale != amount(&state.base_scale)?
        {
            return Ok(false);
        }
        if self.wallet() != self.signer {
            self.check_delegation(rpc, &metadata, head, pending).await?;
        }
        let pair = ordering::observe(rpc, &metadata, head, self.wallet(), pending)
            .await?
            .allocate(state.target_nonce)?;
        Ok(pair.front == state.entry.nonce)
    }

    async fn reconcile(&self, rpc: &DeepXRpcClient, state: &mut State) -> anyhow::Result<()> {
        let finalized = observe_finalized_checkpoint(rpc).await?;
        anyhow::ensure!(
            u64::from(finalized.block_number()) >= state.cursor_number,
            "finality regressed"
        );
        anyhow::ensure!(
            rpc.get_block_hash_required_once(state.cursor_number)
                .await?
                == state.cursor_hash,
            "finalized journal checkpoint is no longer canonical"
        );
        while state.cursor_number < u64::from(finalized.block_number()) {
            let number = state
                .cursor_number
                .checked_add(1)
                .context("block number overflow")?;
            let hash = rpc.get_block_hash_required_once(number).await?;
            let response: BlockResponse = rpc.get_block(&hash).await?;
            anyhow::ensure!(
                response.block.header.parent_hash == state.cursor_hash,
                "noncontiguous finalized history"
            );
            anyhow::ensure!(
                u64::from_str_radix(response.block.header.number.trim_start_matches("0x"), 16)?
                    == number,
                "finalized block number mismatch"
            );
            anyhow::ensure!(
                encoded(super::runtime_code_hash(rpc, &state.cursor_hash).await?)
                    == state.runtime_code_hash,
                "runtime changed during recovery; journal retained"
            );
            let observed = super::get_metadata(rpc, &state.cursor_hash).await?;
            let metadata_bytes =
                super::decode_runtime_metadata(&observed).context("invalid recovery metadata")?;
            let version: deepx_rust_sdk::rpc::DeepXRpcRuntimeVersion = rpc.execute_rpc_call(json!({"jsonrpc":"2.0","id":1,"method":"state_getRuntimeVersion","params":[state.cursor_hash]})).await?;
            let snapshot = RuntimeSnapshot::approved_testnet(
                &DeepXEnvironment::Testnet,
                self.genesis,
                version.spec_version,
                version.transaction_version,
                &metadata_bytes,
            )?;
            let events = get_system_events(rpc, &hash, number).await?;
            let mut next = state.clone();
            self.bind_inclusions(
                &mut next,
                &snapshot,
                &response.block.extrinsics,
                &events,
                number,
                &hash,
            )?;
            for trade in decode_finalized_spot_trade_events(
                snapshot.interfaces(),
                snapshot.metadata(),
                super::parse_hash(&hash)?,
                number,
                &events,
            )? {
                apply_trade(&mut next, &trade)?;
            }
            let pending_ids: Vec<_> = std::iter::once(&next.entry)
                .chain(next.exits.iter())
                .filter(|op| !op.terminal && op.included.as_ref().is_some_and(|i| i.success))
                .map(|op| op.nonce)
                .collect();
            if !pending_ids.is_empty() {
                let active = native::active_ioc_ids(
                    rpc,
                    snapshot.metadata(),
                    &hash,
                    self.settings.subaccount,
                    self.pair,
                    &pending_ids,
                )
                .await?;
                let entry_nonce = next.entry.nonce;
                for op in std::iter::once(&mut next.entry).chain(next.exits.iter_mut()) {
                    if pending_ids.contains(&op.nonce) {
                        if let Some(is_buy) = active.get(&op.nonce) {
                            anyhow::ensure!(
                                *is_buy == (op.nonce == entry_nonce),
                                "active IOC side mismatch"
                            );
                        } else {
                            op.terminal = true;
                        }
                    }
                }
            }
            let timestamp = query_timestamp_now_at_block(rpc, super::parse_hash(&hash)?).await?;
            for operation in std::iter::once(&mut next.entry).chain(next.exits.iter_mut()) {
                if operation.included.is_none()
                    && operation
                        .nonce
                        .checked_add(NONCE_PAST_RANGE)
                        .is_some_and(|expiry| timestamp >= expiry)
                {
                    operation.expired = true;
                }
            }
            next.cursor_number = number;
            next.cursor_hash = hash;
            self.journal.store(&next)?;
            *state = next;
            eprintln!(
                "{}",
                json!({"event":"finalized_reconciled", "number":state.cursor_number,
                "hash":state.cursor_hash, "entry_terminal":state.entry.settled(),
                "target_outcome":state.target_outcome, "acquired_raw_base":state.acquired_base,
                "sold_raw_base":state.sold_base})
            );
        }
        anyhow::ensure!(
            state.cursor_hash == encoded(finalized.block_hash()),
            "finalized checkpoint hash mismatch"
        );
        Ok(())
    }

    fn bind_inclusions(
        &self,
        state: &mut State,
        snapshot: &RuntimeSnapshot,
        extrinsics: &[String],
        events: &[u8],
        number: u64,
        hash: &str,
    ) -> anyhow::Result<()> {
        for (index, encoded) in extrinsics.iter().enumerate() {
            let index = u32::try_from(index)?;
            if encoded.eq_ignore_ascii_case(&state.target_bytes) {
                anyhow::ensure!(
                    state.target_outcome.is_none(),
                    "target included more than once"
                );
                state.target_outcome = Some(dispatch_success(snapshot.metadata(), events, index)?);
            }
            for (is_buy, operation) in std::iter::once((true, &mut state.entry))
                .chain(state.exits.iter_mut().map(|op| (false, op)))
            {
                if encoded.eq_ignore_ascii_case(&operation.bytes) {
                    anyhow::ensure!(
                        operation.included.is_none(),
                        "operation included more than once"
                    );
                    let expected = DeepXSpotPlaceExpected {
                        params: self.params(
                            is_buy,
                            amount(&operation.base)?,
                            amount(&operation.quote)?,
                        ),
                        order_id: operation.nonce,
                        extrinsic_index: index,
                    };
                    let success = native::inclusion(snapshot.metadata(), expected, events)?;
                    operation.included = Some(Inclusion {
                        number,
                        hash: hash.to_owned(),
                        index,
                        success,
                    });
                }
            }
        }
        Ok(())
    }
}

fn apply_trade(state: &mut State, trade: &DeepXFinalizedSpotTrade) -> anyhow::Result<()> {
    if encoded(trade.pair) != state.pair {
        return Ok(());
    }
    let position = (trade.block_number, trade.event_index);
    if encoded(trade.buyer) == state.target_subaccount && trade.buy_id == state.target_nonce {
        state.target_first_trade.get_or_insert(position);
    }
    let entry_fill = encoded(trade.buyer) == state.subaccount && trade.buy_id == state.entry.nonce;
    let exit_fill = encoded(trade.seller) == state.subaccount
        && state.exits.iter().any(|op| op.nonce == trade.sell_id);
    anyhow::ensure!(
        !(entry_fill && exit_fill),
        "self trade in execution evidence"
    );
    if !entry_fill && !exit_fill {
        return Ok(());
    }
    let base = narrow(U256::from_le_bytes(trade.trade_amount_b))?;
    let quote = narrow(U256::from_le_bytes(trade.trade_amount_u))?;
    if entry_fill {
        anyhow::ensure!(
            state.entry.included.as_ref().is_some_and(|i| i.success),
            "entry fill without successful inclusion"
        );
        let fee = received_fee(trade, true, &state.base_symbol)?;
        add(&mut state.acquired_base, net_received(base, fee)?)?;
        add(&mut state.spent_quote, quote)?;
        anyhow::ensure!(
            amount(&state.spent_quote)? <= amount(&state.entry.quote)?,
            "entry exceeds signed quote budget"
        );
        state.entry_first_trade.get_or_insert(position);
        state.entry_last_trade = Some(position);
    }
    if exit_fill {
        anyhow::ensure!(
            state
                .exits
                .iter()
                .any(|op| op.nonce == trade.sell_id
                    && op.included.as_ref().is_some_and(|i| i.success)),
            "exit fill without successful inclusion"
        );
        let fee = received_fee(trade, false, &state.quote_symbol)?;
        add(&mut state.sold_base, base)?;
        add(&mut state.received_quote, net_received(quote, fee)?)?;
        state.remaining()?;
    }
    Ok(())
}

fn received_fee(
    trade: &DeepXFinalizedSpotTrade,
    buyer: bool,
    symbol: &[u8],
) -> anyhow::Result<i128> {
    let taker = match trade.taker.as_str() {
        "Buyer" => buyer,
        "Seller" => !buyer,
        _ => anyhow::bail!("unknown taker role"),
    };
    let (fee, asset) = if taker {
        (trade.taker_fee, &trade.taker_fee_asset)
    } else {
        (trade.maker_fee, &trade.maker_fee_asset)
    };
    anyhow::ensure!(asset == symbol, "fill fee asset mismatch");
    Ok(fee)
}

fn require_idle_account(account: &ScaleValue<u32>) -> anyhow::Result<()> {
    let status = account.at("status").context("missing account status")?;
    let subxt_core::ext::scale_value::ValueDef::Variant(status) = &status.value else {
        anyhow::bail!("invalid account status");
    };
    anyhow::ensure!(
        status.name == "Active",
        "execution subaccount is not active"
    );
    let positions = account
        .at("spot_positions")
        .context("missing Spot positions")?;
    let subxt_core::ext::scale_value::ValueDef::Composite(positions) = &positions.value else {
        anyhow::bail!("invalid Spot positions");
    };
    for position in positions.values() {
        anyhow::ensure!(
            position.at("open_orders").and_then(ScaleValue::as_u128) == Some(0),
            "execution subaccount has existing Spot orders"
        );
        let balance_type = position
            .at("balance_type")
            .context("missing Spot balance type")?;
        let subxt_core::ext::scale_value::ValueDef::Variant(balance_type) = &balance_type.value
        else {
            anyhow::bail!("invalid Spot balance type");
        };
        anyhow::ensure!(
            balance_type.name == "Deposit",
            "execution subaccount has a Spot borrow"
        );
    }
    Ok(())
}

fn net_received(gross: u128, fee: i128) -> anyhow::Result<u128> {
    if fee >= 0 {
        gross
            .checked_sub(fee.unsigned_abs())
            .context("fee exceeds received amount")
    } else {
        gross
            .checked_add(fee.unsigned_abs())
            .context("rebate overflows balance")
    }
}

fn add(total: &mut String, value: u128) -> anyhow::Result<()> {
    *total = amount(total)?
        .checked_add(value)
        .context("fill total overflow")?
        .to_string();
    Ok(())
}

fn exact_quote(base: u128, price: u128, scale: u128) -> anyhow::Result<u128> {
    anyhow::ensure!(base > 0 && price > 0 && scale > 0, "zero order term");
    let product = U256::from(base) * U256::from(price);
    anyhow::ensure!(
        product % U256::from(scale) == U256::ZERO,
        "order quote mapping would change the limit price"
    );
    narrow(product / U256::from(scale))
}

fn loss_floor(required_quote: u128, base: u128, scale: u128, tick: u128) -> anyhow::Result<u128> {
    anyhow::ensure!(base > 0 && tick > 0, "zero exit lot or tick");
    let divisor = U256::from(base) * U256::from(tick);
    let numerator = U256::from(required_quote) * U256::from(scale);
    let ticks =
        (numerator / divisor + U256::from(numerator % divisor != U256::ZERO)).max(U256::from(1));
    narrow(
        ticks
            .checked_mul(U256::from(tick))
            .context("exit floor overflow")?,
    )
}

fn narrow(value: U256) -> anyhow::Result<u128> {
    u128::try_from(value).map_err(|_| anyhow::anyhow!("execution amount exceeds u128"))
}

fn encoded(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn lock_account(path: &Path) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options.open(path)?;
    lock.try_lock()
        .context("another MEV execution process holds this signer")?;
    Ok(lock)
}

pub(super) fn validate_storage_interface(metadata: &Metadata) -> anyhow::Result<()> {
    native::validate(metadata)
}

fn decode_hex(value: &str) -> anyhow::Result<Vec<u8>> {
    Ok(hex::decode(
        value
            .strip_prefix("0x")
            .context("missing hexadecimal prefix")?,
    )?)
}

fn text_field<'a>(value: &'a Value, name: &str) -> anyhow::Result<&'a str> {
    value[name]
        .as_str()
        .with_context(|| format!("missing order field {name}"))
}

pub(super) fn parse_account(value: &str) -> anyhow::Result<[u8; 20]> {
    decode_hex(value)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("account must be exactly 20 bytes"))
}

fn find_target(
    metadata: &Metadata,
    pending: &[String],
    pair: [u8; 32],
    order: &Value,
) -> anyhow::Result<Option<(String, Value)>> {
    let market = super::Market::Spot(pair);
    for candidate in pending {
        if let Some(fresh) = super::decode_order(metadata, decode_hex(candidate)?, &market, 1)?
            && fresh["transaction_hash"] == order["transaction_hash"]
        {
            return Ok(Some((candidate.clone(), fresh)));
        }
    }
    Ok(None)
}

async fn read_storage(
    rpc: &DeepXRpcClient,
    metadata: &Metadata,
    head: &str,
    pallet: &str,
    item: &str,
    key: [u8; 20],
) -> anyhow::Result<ScaleValue<u32>> {
    let address = dynamic::storage(pallet, item, vec![dynamic::Value::from_bytes(key)]);
    let key = encoded(storage::get_address_bytes(&address, metadata)?);
    let raw = rpc
        .get_storage(&key, head)
        .await?
        .context("required account storage unavailable")?;
    let bytes = decode_hex(&raw)?;
    let ty = metadata
        .pallet_by_name(pallet)
        .context("missing pallet")?
        .storage()
        .context("missing storage")?
        .entry_by_name(item)
        .context("missing storage item")?
        .entry_type()
        .value_ty();
    let mut remaining = &bytes[..];
    let value = decode_as_type(&mut remaining, ty, metadata.types())?;
    anyhow::ensure!(remaining.is_empty(), "trailing storage bytes");
    Ok(value)
}

async fn market_symbols(
    rpc: &DeepXRpcClient,
    metadata: &Metadata,
    head: &str,
    pair: [u8; 32],
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let key = deepx_rust_sdk::rpc::storage_decode::spot_market_storage_key(pair);
    let raw = rpc
        .get_storage(&key, head)
        .await?
        .context("missing Spot market")?;
    let bytes = decode_hex(&raw)?;
    let ty = metadata
        .pallet_by_name("SpotMarket")
        .context("missing Spot pallet")?
        .storage()
        .context("missing Spot storage")?
        .entry_by_name("SpotMarkets")
        .context("missing market storage")?
        .entry_type()
        .value_ty();
    let mut remaining = &bytes[..];
    let value = decode_as_type(&mut remaining, ty, metadata.types())?;
    anyhow::ensure!(remaining.is_empty(), "trailing market bytes");
    let symbol = |name| -> anyhow::Result<Vec<u8>> {
        let field = value.at(name).context("missing asset symbol")?;
        let subxt_core::ext::scale_value::ValueDef::Composite(values) = &field.value else {
            anyhow::bail!("invalid asset symbol")
        };
        values
            .values()
            .map(|v| Ok(u8::try_from(v.as_u128().context("invalid symbol byte")?)?))
            .collect()
    };
    Ok((symbol("base_symbol")?, symbol("quote_symbol")?))
}

fn dispatch_success(metadata: &Metadata, bytes: &[u8], index: u32) -> anyhow::Result<bool> {
    let mut success = None;
    for event in
        Events::<super::DeepXRuntimeConfig>::decode_from(bytes.to_vec(), metadata.clone()).iter()
    {
        let event = event?;
        if event.phase() == Phase::ApplyExtrinsic(index) && event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => true,
                "ExtrinsicFailed" => false,
                _ => continue,
            };
            anyhow::ensure!(
                success.replace(outcome).is_none(),
                "duplicate target dispatch evidence"
            );
        }
    }
    success.context("missing target dispatch evidence")
}

#[derive(Deserialize)]
struct BlockResponse {
    block: Block,
}
#[derive(Deserialize)]
struct Block {
    header: Header,
    extrinsics: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Header {
    parent_hash: String,
    number: String,
}

#[cfg(test)]
mod tests;
