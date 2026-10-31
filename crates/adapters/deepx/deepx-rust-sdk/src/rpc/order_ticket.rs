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

//! One signed perpetual placement tracked from pool acceptance to finality.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};

use super::{
    DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
    chain_decode::{decode_block_number_u64, decode_prefixed_hash},
    event_storage::get_system_events,
    get_metadata, get_runtime_version,
    perp_place::{
        DeepXPerpPlaceExpected, DeepXPerpPlaceInclusionOutcome, decode_perp_place_inclusion,
    },
    submission::{
        DeepXSignedExtrinsicDecodeError, DeepXSubmittedExtrinsic, decode_signed_extrinsic_identity,
        find_extrinsic_by_hash,
    },
};
use crate::{
    credential::DeepXPrivateKey,
    deployment::DEEPX_TESTNET_GENESIS_HASH,
    enums::DeepXEnvironment,
    signing::{
        DeepXPermittedPalletExtrinsic, DeepXPerpPlaceParams, DeepXRuntimeSnapshotPermit,
        RuntimeSnapshot, sign_perp_place_order,
    },
};

const MAX_SCAN_BLOCKS: u64 = 256;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// A perpetual order and signed bytes created together from one approved runtime permit.
pub struct DeepXPreparedPerpOrder {
    signed: DeepXPermittedPalletExtrinsic,
    params: DeepXPerpPlaceParams,
}

impl DeepXPreparedPerpOrder {
    /// Signs exact order terms at the caller's reserved timestamp nonce.
    ///
    /// # Errors
    ///
    /// Returns an error when the approved runtime cannot sign the order.
    pub fn sign(
        permit: &DeepXRuntimeSnapshotPermit,
        key: &DeepXPrivateKey,
        params: DeepXPerpPlaceParams,
        nonce: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            signed: sign_perp_place_order(permit, key, params, nonce)?,
            params,
        })
    }

    /// Returns the complete signed extrinsic for an explicit dry run.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        self.signed.bytes()
    }

    /// Returns the exact extrinsic hash to use for reconciliation.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.signed.extrinsic_hash()
    }
}

/// One observed inclusion of the exact signed perpetual order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpOrderInclusion {
    /// Canonical block hash at observation time.
    pub block_hash: String,
    /// Block height.
    pub block_number: u64,
    /// Index of the exact signed extrinsic.
    pub extrinsic_index: u32,
    /// Order ID reserved by the timestamp nonce.
    pub order_id: u64,
}

/// Known lifecycle state of one submitted perpetual order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXPerpOrderTicketState {
    /// The submission node acknowledged the exact bytes and hash.
    Accepted,
    /// Placement succeeded in a best-chain block but is not yet final.
    Executed(DeepXPerpOrderInclusion),
    /// The same successful inclusion is finalized.
    Finalized(DeepXPerpOrderInclusion),
    /// A dispatch failed in a best-chain block and is not yet final.
    Failed(DeepXPerpOrderInclusion),
    /// The same failed dispatch was observed in a finalized block.
    FinalizedFailed(DeepXPerpOrderInclusion),
    /// Observed evidence became inconsistent and requires hash-based reconciliation.
    ActionRequired,
}

/// One observed state or wait error for a tracked perpetual order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXPerpOrderEvent {
    /// Hash of the exact signed extrinsic.
    pub tx_hash: [u8; 32],
    /// Timestamp nonce and order ID.
    pub nonce: u64,
    /// Ticket state after the observation.
    pub state: DeepXPerpOrderTicketState,
    /// Wait error, when the state alone does not describe the outcome.
    pub error: Option<String>,
}

/// Tracks submitted perpetual orders by hash and reports monitored waits to one listener.
pub struct DeepXPerpOrderMonitor {
    tickets: HashMap<[u8; 32], DeepXPerpOrderTicket>,
    listener: Box<dyn FnMut(&DeepXPerpOrderEvent) + Send>,
}

impl DeepXPerpOrderMonitor {
    /// Creates a monitor whose listener receives events for every tracked order.
    #[must_use]
    pub fn new(listener: impl FnMut(&DeepXPerpOrderEvent) + Send + 'static) -> Self {
        Self {
            tickets: HashMap::new(),
            listener: Box::new(listener),
        }
    }

    /// Returns a tracked ticket by its exact extrinsic hash.
    #[must_use]
    pub fn get(&self, tx_hash: &[u8; 32]) -> Option<&DeepXPerpOrderTicket> {
        self.tickets.get(tx_hash)
    }

    /// Submits and tracks an order after hash-verified pool acceptance.
    ///
    /// A submission error may be uncertain; reconcile the hash in the error before retrying.
    ///
    /// # Errors
    ///
    /// Returns the ticket submission error without registering an unaccepted order.
    pub async fn submit(
        &mut self,
        rpc: &DeepXRpcClient,
        prepared: DeepXPreparedPerpOrder,
    ) -> anyhow::Result<[u8; 32]> {
        let hash = prepared.extrinsic_hash();
        ensure!(
            !self.tickets.contains_key(&hash),
            "order hash is already tracked: 0x{}",
            hex::encode(hash)
        );
        let ticket = DeepXPerpOrderTicket::submit(rpc, prepared).await?;
        self.tickets.insert(hash, ticket);
        let ticket = &self.tickets[&hash];
        (self.listener)(&DeepXPerpOrderEvent {
            tx_hash: hash,
            nonce: ticket.nonce(),
            state: ticket.state().clone(),
            error: None,
        });
        Ok(hash)
    }

    /// Waits for provisional execution and reports its observed state or error.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown hash or the ticket execution wait.
    pub async fn executed(
        &mut self,
        rpc: &DeepXRpcClient,
        tx_hash: &[u8; 32],
        timeout: Duration,
    ) -> anyhow::Result<DeepXPerpOrderInclusion> {
        let ticket = self
            .tickets
            .get_mut(tx_hash)
            .context("order hash is not tracked")?;
        let before = ticket.state().clone();
        let result = ticket.executed(rpc, timeout).await;
        let event = Self::event_after_wait(ticket, &before, &result);
        if let Some(event) = event {
            (self.listener)(&event);
        }
        result
    }

    /// Waits for finality and reports its observed state or error.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown hash or the ticket finality wait.
    pub async fn finalized(
        &mut self,
        rpc: &DeepXRpcClient,
        tx_hash: &[u8; 32],
        timeout: Duration,
    ) -> anyhow::Result<DeepXPerpOrderInclusion> {
        let timeout = if self
            .tickets
            .get(tx_hash)
            .is_some_and(|ticket| ticket.state() == &DeepXPerpOrderTicketState::Accepted)
        {
            let deadline = Instant::now()
                .checked_add(timeout)
                .context("timeout overflow")?;
            self.executed(rpc, tx_hash, timeout).await?;
            deadline.saturating_duration_since(Instant::now())
        } else {
            timeout
        };
        let ticket = self
            .tickets
            .get_mut(tx_hash)
            .context("order hash is not tracked")?;
        let before = ticket.state().clone();
        let result = ticket.finalized(rpc, timeout).await;
        let event = Self::event_after_wait(ticket, &before, &result);
        if let Some(event) = event {
            (self.listener)(&event);
        }
        result
    }

    fn event_after_wait(
        ticket: &DeepXPerpOrderTicket,
        before: &DeepXPerpOrderTicketState,
        result: &anyhow::Result<DeepXPerpOrderInclusion>,
    ) -> Option<DeepXPerpOrderEvent> {
        if before == ticket.state() && result.is_ok() {
            return None;
        }
        Some(DeepXPerpOrderEvent {
            tx_hash: ticket.tx_hash(),
            nonce: ticket.nonce(),
            state: ticket.state().clone(),
            error: result.as_ref().err().map(ToString::to_string),
        })
    }
}

/// An accepted perpetual order whose inclusion and finality can be awaited separately.
pub struct DeepXPerpOrderTicket {
    signed: DeepXPermittedPalletExtrinsic,
    params: DeepXPerpPlaceParams,
    scan_start: u64,
    state: DeepXPerpOrderTicketState,
}

impl DeepXPerpOrderTicket {
    /// Submits one signed order and returns after hash-verified pool acceptance.
    ///
    /// A submission error may be uncertain. Reconcile by the hash in the error before retrying.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid endpoint or runtime identity, pool collision, or submission.
    pub async fn submit(
        rpc: &DeepXRpcClient,
        prepared: DeepXPreparedPerpOrder,
    ) -> anyhow::Result<Self> {
        let DeepXPreparedPerpOrder { signed, params } = prepared;
        let expected_hash = format!("0x{}", hex::encode(signed.extrinsic_hash()));
        let genesis = rpc.get_block_hash_required_once(0).await?;
        ensure!(
            genesis.eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
            "RPC endpoint is not the approved DeepX testnet"
        );
        let best_hash = rpc.get_best_block_hash_once().await?;
        let best: DeepXRpcBlockResponse = rpc.get_block(&best_hash).await?;
        let best_height = decode_block_number_u64(&best.block.header.number)
            .context("invalid best block height")?;
        let scan_start = best_height.checked_add(1).context("best height overflow")?;
        let snapshot = read_runtime(rpc, &best_hash).await?;
        ensure!(
            snapshot.identity() == signed.runtime(),
            "runtime changed after perpetual order signing"
        );
        let encoded = format!("0x{}", hex::encode(signed.bytes()));
        for pending in rpc.get_pending_extrinsics().await? {
            ensure!(
                !pending.eq_ignore_ascii_case(&encoded),
                "order is already pending; reconcile hash={expected_hash}"
            );
            let bytes = hex::decode(
                pending
                    .strip_prefix("0x")
                    .context("invalid pending extrinsic hex")?,
            )?;
            match decode_signed_extrinsic_identity(snapshot.metadata(), &bytes) {
                Ok(identity) => ensure!(
                    identity.signer != signed.signer() || identity.nonce != signed.nonce(),
                    "another extrinsic has the signer and nonce; reconcile hash={expected_hash}"
                ),
                Err(DeepXSignedExtrinsicDecodeError::MissingIdentity) => {}
                Err(e) => return Err(e.into()),
            }
        }
        ensure!(
            rpc.get_block_hash_required_once(best_height)
                .await?
                .eq_ignore_ascii_case(&best_hash),
            "best block changed before submission; refresh the signed order"
        );
        let returned = rpc
            .submit_encoded_extrinsic(&encoded)
            .await
            .with_context(|| format!("submission uncertain; reconcile hash={expected_hash}"))?;
        let returned_hash = decode_prefixed_hash(&returned)
            .with_context(|| format!("invalid submission hash; reconcile hash={expected_hash}"))?;
        DeepXSubmittedExtrinsic::verified(signed.bytes(), signed.extrinsic_hash(), returned_hash)?;
        Ok(Self {
            signed,
            params,
            scan_start,
            state: DeepXPerpOrderTicketState::Accepted,
        })
    }

    /// Returns the exact signed extrinsic hash used for reconciliation.
    #[must_use]
    pub const fn tx_hash(&self) -> [u8; 32] {
        self.signed.extrinsic_hash()
    }

    /// Returns the timestamp nonce and order ID.
    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.signed.nonce()
    }

    /// Returns the currently established lifecycle state.
    #[must_use]
    pub const fn state(&self) -> &DeepXPerpOrderTicketState {
        &self.state
    }

    /// Waits for a successful placement event in a canonical best-chain block.
    ///
    /// The result is provisional until `finalized` succeeds. A timeout leaves the state intact.
    ///
    /// # Errors
    ///
    /// Returns an error for timeout, RPC failure, failed dispatch, or uncertain chain evidence.
    pub async fn executed(
        &mut self,
        rpc: &DeepXRpcClient,
        timeout: Duration,
    ) -> anyhow::Result<DeepXPerpOrderInclusion> {
        match &self.state {
            DeepXPerpOrderTicketState::Executed(inclusion)
            | DeepXPerpOrderTicketState::Finalized(inclusion) => return Ok(inclusion.clone()),
            DeepXPerpOrderTicketState::Accepted => {}
            _ => anyhow::bail!("order ticket requires reconciliation before execution wait"),
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("timeout overflow")?;
        loop {
            let best_hash = rpc.get_best_block_hash_once().await?;
            let header: DeepXRpcBlockHeader = rpc.get_header_once(&best_hash).await?;
            let best_height =
                decode_block_number_u64(&header.number).context("invalid best block height")?;
            if best_height >= self.scan_start {
                if best_height - self.scan_start >= MAX_SCAN_BLOCKS {
                    self.state = DeepXPerpOrderTicketState::ActionRequired;
                    anyhow::bail!(
                        "execution scan exceeded {MAX_SCAN_BLOCKS} blocks; reconcile hash=0x{}",
                        hex::encode(self.tx_hash())
                    );
                }
                for height in self.scan_start..=best_height {
                    if let Some((inclusion, success)) = self.observe_at_height(rpc, height).await? {
                        self.state = if success {
                            DeepXPerpOrderTicketState::Executed(inclusion.clone())
                        } else {
                            DeepXPerpOrderTicketState::Failed(inclusion.clone())
                        };
                        ensure!(
                            success,
                            "perpetual placement dispatch failed: hash=0x{}",
                            hex::encode(self.tx_hash())
                        );
                        return Ok(inclusion);
                    }
                }
            }
            ensure!(
                Instant::now() < deadline,
                "execution wait timed out; reconcile hash=0x{}",
                hex::encode(self.tx_hash())
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Waits for a provisional success or failure to finalize without changing a timed-out state.
    ///
    /// # Errors
    ///
    /// Returns an error for timeout, RPC failure, reorg, or changed inclusion evidence.
    pub async fn finalized(
        &mut self,
        rpc: &DeepXRpcClient,
        timeout: Duration,
    ) -> anyhow::Result<DeepXPerpOrderInclusion> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("timeout overflow")?;
        if self.state == DeepXPerpOrderTicketState::Accepted {
            self.executed(rpc, timeout).await?;
        }
        let inclusion = match &self.state {
            DeepXPerpOrderTicketState::Finalized(inclusion) => return Ok(inclusion.clone()),
            DeepXPerpOrderTicketState::Executed(inclusion)
            | DeepXPerpOrderTicketState::Failed(inclusion) => inclusion.clone(),
            _ => anyhow::bail!("order ticket requires reconciliation before finality wait"),
        };
        let expected_success = matches!(self.state, DeepXPerpOrderTicketState::Executed(_));
        loop {
            let finalized_hash = rpc.get_finalized_head_once().await?;
            let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_hash).await?;
            let height = decode_block_number_u64(&header.number)
                .context("invalid finalized block height")?;
            if height >= inclusion.block_number {
                let canonical = rpc
                    .get_block_hash_required_once(inclusion.block_number)
                    .await?;
                if !canonical.eq_ignore_ascii_case(&inclusion.block_hash) {
                    self.state = DeepXPerpOrderTicketState::ActionRequired;
                    anyhow::bail!(
                        "provisional inclusion was retracted; reconcile hash=0x{}",
                        hex::encode(self.tx_hash())
                    );
                }
                let Some((observed, success)) =
                    self.observe_at_height(rpc, inclusion.block_number).await?
                else {
                    self.state = DeepXPerpOrderTicketState::ActionRequired;
                    anyhow::bail!(
                        "finalized inclusion is missing; reconcile hash=0x{}",
                        hex::encode(self.tx_hash())
                    );
                };
                if observed != inclusion || success != expected_success {
                    self.state = DeepXPerpOrderTicketState::ActionRequired;
                    anyhow::bail!(
                        "finalized inclusion differs from execution; reconcile hash=0x{}",
                        hex::encode(self.tx_hash())
                    );
                }
                if success {
                    self.state = DeepXPerpOrderTicketState::Finalized(inclusion.clone());
                    return Ok(inclusion);
                }
                self.state = DeepXPerpOrderTicketState::FinalizedFailed(inclusion);
                anyhow::bail!(
                    "perpetual placement failed in a finalized block: hash=0x{}",
                    hex::encode(self.tx_hash())
                );
            }
            ensure!(
                Instant::now() < deadline,
                "finality wait timed out; reconcile hash=0x{}",
                hex::encode(self.tx_hash())
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn observe_at_height(
        &self,
        rpc: &DeepXRpcClient,
        height: u64,
    ) -> anyhow::Result<Option<(DeepXPerpOrderInclusion, bool)>> {
        let block_hash = rpc.get_block_hash_required_once(height).await?;
        let block: DeepXRpcBlockResponse = rpc.get_block(&block_hash).await?;
        ensure!(
            decode_block_number_u64(&block.block.header.number) == Some(height),
            "order block height mismatch; reconcile hash=0x{}",
            hex::encode(self.tx_hash())
        );
        let Some(index) = find_extrinsic_by_hash(&block.block.extrinsics, self.tx_hash())? else {
            return Ok(None);
        };
        let encoded = format!("0x{}", hex::encode(self.signed.bytes()));
        ensure!(
            block.block.extrinsics[index as usize].eq_ignore_ascii_case(&encoded),
            "order block bytes differ; reconcile hash=0x{}",
            hex::encode(self.tx_hash())
        );
        let snapshot = read_runtime(rpc, &block_hash).await?;
        ensure!(
            snapshot.identity() == self.signed.runtime(),
            "order inclusion runtime differs from signing runtime"
        );
        let events = get_system_events(rpc, &block_hash, height).await?;
        ensure!(
            events == get_system_events(rpc, &block_hash, height).await?,
            "order event storage changed; reconcile hash=0x{}",
            hex::encode(self.tx_hash())
        );
        let evidence = decode_perp_place_inclusion(
            snapshot.interfaces(),
            snapshot.metadata(),
            DeepXPerpPlaceExpected {
                params: self.params,
                order_id: self.nonce(),
                block_number: height,
                extrinsic_index: index,
            },
            &events,
        )?;
        ensure!(
            rpc.get_block_hash_required_once(height)
                .await?
                .eq_ignore_ascii_case(&block_hash),
            "order block changed during verification; reconcile hash=0x{}",
            hex::encode(self.tx_hash())
        );
        Ok(Some((
            DeepXPerpOrderInclusion {
                block_hash,
                block_number: height,
                extrinsic_index: index,
                order_id: self.nonce(),
            },
            evidence.outcome == DeepXPerpPlaceInclusionOutcome::Success,
        )))
    }
}

async fn read_runtime(rpc: &DeepXRpcClient, block_hash: &str) -> anyhow::Result<RuntimeSnapshot> {
    let version: DeepXRpcRuntimeVersion = get_runtime_version(rpc, block_hash).await?;
    let metadata = get_metadata(rpc, block_hash).await?;
    let metadata = hex::decode(
        metadata
            .strip_prefix("0x")
            .context("invalid metadata hex")?,
    )?;
    let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
        .context("invalid approved genesis hash")?;
    RuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        genesis,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use axum::{Json, Router, extract::State, routing::post};
    use parity_scale_codec::{Compact, Encode};
    use serde_json::{Value, json};
    use subxt_core::{events::Phase, ext::scale_value::Value as ScaleValue};

    use super::*;
    use crate::{
        credential::{DeepXKeyScheme, DeepXPrivateKey},
        signing::{
            DeepXPerpOrderType, DeepXPostOnlyParam, DeepXRuntimeSnapshotService, DeepXTimeInForce,
        },
    };

    #[derive(Clone)]
    struct MockRpc {
        metadata: String,
        encoded: String,
        expected_hash: String,
        events: String,
        failed_events: String,
        included: Arc<AtomicBool>,
        reorged: Arc<AtomicBool>,
        failed: Arc<AtomicBool>,
    }

    async fn rpc_response(State(state): State<MockRpc>, Json(request): Json<Value>) -> Json<Value> {
        let method = request["method"].as_str().unwrap();
        let result = match method {
            "chain_getBlockHash" => {
                let params = request["params"].as_array().unwrap();
                if params.first() == Some(&json!(0)) {
                    json!(DEEPX_TESTNET_GENESIS_HASH)
                } else if params.first() == Some(&json!(101))
                    && state.reorged.load(Ordering::SeqCst)
                {
                    json!(format!("0x{}", hex::encode([0x33; 32])))
                } else if params.first() == Some(&json!(100))
                    || !state.included.load(Ordering::SeqCst)
                {
                    json!(format!("0x{}", hex::encode([0x11; 32])))
                } else {
                    json!(format!("0x{}", hex::encode([0x22; 32])))
                }
            }
            "chain_getBlock" => {
                if request["params"][0] == format!("0x{}", hex::encode([0x22; 32]))
                    || request["params"][0] == format!("0x{}", hex::encode([0x33; 32]))
                {
                    json!({"block": {"header": {"number": "0x65"}, "extrinsics": [state.encoded]}})
                } else {
                    json!({"block": {"header": {"number": "0x64"}, "extrinsics": []}})
                }
            }
            "chain_getHeader" => {
                if request["params"][0] == format!("0x{}", hex::encode([0x22; 32]))
                    || request["params"][0] == format!("0x{}", hex::encode([0x33; 32]))
                {
                    json!({"number": "0x65"})
                } else {
                    json!({"number": "0x64"})
                }
            }
            "chain_getFinalizedHead" => {
                let bytes = if state.reorged.load(Ordering::SeqCst) {
                    [0x33; 32]
                } else {
                    [0x22; 32]
                };
                json!(format!("0x{}", hex::encode(bytes)))
            }
            "state_getRuntimeVersion" => {
                json!({"specVersion": 376, "transactionVersion": 1})
            }
            "state_getMetadata" => json!(state.metadata),
            "state_getStorage" => {
                if state.failed.load(Ordering::SeqCst) {
                    json!(state.failed_events)
                } else {
                    json!(state.events)
                }
            }
            "author_pendingExtrinsics" => json!([]),
            "author_submitExtrinsic" => {
                assert_eq!(request["params"][0], state.encoded);
                json!(state.expected_hash)
            }
            _ => panic!("unexpected RPC method: {method}"),
        };
        Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
    }

    fn record(
        runtime: &RuntimeSnapshot,
        pallet_name: &str,
        event_name: &str,
        values: &[ScaleValue<()>],
    ) -> Vec<u8> {
        let pallet = runtime.metadata().pallet_by_name(pallet_name).unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == event_name)
            .unwrap();
        assert_eq!(event.fields.len(), values.len());
        let mut bytes = Phase::ApplyExtrinsic(0).encode();
        bytes.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                value,
                field.ty.id,
                runtime.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    fn placement_events(
        runtime: &RuntimeSnapshot,
        params: DeepXPerpPlaceParams,
        nonce: u64,
    ) -> String {
        let place = record(
            runtime,
            "PerpMarket",
            "OrderPlaced",
            &[
                ScaleValue::u128(u128::from(nonce)),
                ScaleValue::named_composite([
                    ("order_id", ScaleValue::u128(u128::from(nonce))),
                    ("owner", ScaleValue::from_bytes(params.subaccount)),
                    ("market_id", ScaleValue::u128(u128::from(params.market_id))),
                    ("is_long", ScaleValue::bool(params.is_long)),
                    ("size", ScaleValue::u128(params.size)),
                    ("price", ScaleValue::u128(params.price)),
                    (
                        "order_type",
                        ScaleValue::unnamed_variant(
                            "Limit",
                            [ScaleValue::unnamed_variant("GTC", [])],
                        ),
                    ),
                    ("create_time", ScaleValue::u128(101)),
                    ("leverage", ScaleValue::u128(25_000)),
                    ("status", ScaleValue::unnamed_variant("Open", [])),
                    ("size_filled", ScaleValue::u128(0)),
                    ("size_remain", ScaleValue::u128(params.size)),
                    ("take_profit", ScaleValue::unnamed_variant("None", [])),
                    ("stop_loss", ScaleValue::unnamed_variant("None", [])),
                    ("reduce_only", ScaleValue::bool(false)),
                    ("post_only", ScaleValue::unnamed_variant("MustPostOnly", [])),
                ]),
            ],
        );
        let dispatch_info = ScaleValue::named_composite([
            (
                "weight",
                ScaleValue::named_composite([
                    ("ref_time", ScaleValue::u128(0)),
                    ("proof_size", ScaleValue::u128(0)),
                ]),
            ),
            (
                "call_type",
                ScaleValue::unnamed_variant("Timestamp", [ScaleValue::u128(0)]),
            ),
            (
                "priority",
                ScaleValue::unnamed_composite([ScaleValue::u128(0), ScaleValue::u128(0)]),
            ),
            ("class", ScaleValue::unnamed_variant("Normal", [])),
            ("pays_fee", ScaleValue::unnamed_variant("Yes", [])),
        ]);
        let dispatch = record(runtime, "System", "ExtrinsicSuccess", &[dispatch_info]);
        let mut bytes = Compact(2_u32).encode();
        bytes.extend(place);
        bytes.extend(dispatch);
        format!("0x{}", hex::encode(bytes))
    }

    fn failed_events(runtime: &RuntimeSnapshot) -> String {
        let dispatch_info = ScaleValue::named_composite([
            (
                "weight",
                ScaleValue::named_composite([
                    ("ref_time", ScaleValue::u128(0)),
                    ("proof_size", ScaleValue::u128(0)),
                ]),
            ),
            (
                "call_type",
                ScaleValue::unnamed_variant("Timestamp", [ScaleValue::u128(0)]),
            ),
            (
                "priority",
                ScaleValue::unnamed_composite([ScaleValue::u128(0), ScaleValue::u128(0)]),
            ),
            ("class", ScaleValue::unnamed_variant("Normal", [])),
            ("pays_fee", ScaleValue::unnamed_variant("Yes", [])),
        ]);
        let dispatch = record(
            runtime,
            "System",
            "ExtrinsicFailed",
            &[ScaleValue::unnamed_variant("Other", []), dispatch_info],
        );
        let mut bytes = Compact(1_u32).encode();
        bytes.extend(dispatch);
        format!("0x{}", hex::encode(bytes))
    }

    #[tokio::test]
    async fn order_ticket_tracks_acceptance_execution_finality_reorg_and_failure() {
        let runtime = RuntimeSnapshot::built_in_testnet().unwrap();
        let service = DeepXRuntimeSnapshotService::new(runtime.clone());
        let permit = service.acquire().unwrap();
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let params = DeepXPerpPlaceParams {
            subaccount: [0x22; 20],
            market_id: 3,
            is_long: true,
            size: 123,
            price: 456,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::MustPostOnly,
        };
        let prepared =
            DeepXPreparedPerpOrder::sign(&permit, &key, params, 1_725_000_000_125).unwrap();
        let hash = prepared.extrinsic_hash();
        let response: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let state = MockRpc {
            metadata: response["result"].as_str().unwrap().to_string(),
            encoded: format!("0x{}", hex::encode(prepared.bytes())),
            expected_hash: format!("0x{}", hex::encode(hash)),
            events: placement_events(&runtime, params, 1_725_000_000_125),
            failed_events: failed_events(&runtime),
            included: Arc::new(AtomicBool::new(false)),
            reorged: Arc::new(AtomicBool::new(false)),
            failed: Arc::new(AtomicBool::new(false)),
        };
        let included = state.included.clone();
        let reorged = state.reorged.clone();
        let failed = state.failed.clone();
        let app = Router::new()
            .route("/", post(rpc_response))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let rpc = DeepXRpcClient::new(format!("http://{address}"));
        let events = Arc::new(Mutex::new(Vec::<DeepXPerpOrderEvent>::new()));
        let observed = events.clone();
        let mut monitor = DeepXPerpOrderMonitor::new(move |event| {
            observed.lock().unwrap().push(event.clone());
        });
        assert_eq!(monitor.submit(&rpc, prepared).await.unwrap(), hash);
        let ticket = monitor.get(&hash).unwrap();
        assert_eq!(ticket.nonce(), 1_725_000_000_125);
        assert_eq!(ticket.state(), &DeepXPerpOrderTicketState::Accepted);
        assert!(monitor.executed(&rpc, &hash, Duration::ZERO).await.is_err());
        assert_eq!(
            monitor.get(&hash).unwrap().state(),
            &DeepXPerpOrderTicketState::Accepted
        );
        included.store(true, Ordering::SeqCst);
        let execution = monitor
            .executed(&rpc, &hash, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(execution.block_number, 101);
        assert_eq!(execution.extrinsic_index, 0);
        assert!(matches!(
            monitor.get(&hash).unwrap().state(),
            DeepXPerpOrderTicketState::Executed(_)
        ));
        let finalized = monitor
            .finalized(&rpc, &hash, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(finalized, execution);
        assert!(matches!(
            monitor.get(&hash).unwrap().state(),
            DeepXPerpOrderTicketState::Finalized(_)
        ));
        {
            let events = events.lock().unwrap();
            assert_eq!(events.len(), 4);
            assert!(events.iter().all(|event| event.tx_hash == hash));
            assert_eq!(events[0].state, DeepXPerpOrderTicketState::Accepted);
            assert!(events[1].error.as_ref().unwrap().contains("timed out"));
            assert!(matches!(
                events[2].state,
                DeepXPerpOrderTicketState::Executed(_)
            ));
            assert!(matches!(
                events[3].state,
                DeepXPerpOrderTicketState::Finalized(_)
            ));
        }

        included.store(false, Ordering::SeqCst);
        let second =
            DeepXPreparedPerpOrder::sign(&permit, &key, params, 1_725_000_000_125).unwrap();
        let mut retracted = DeepXPerpOrderTicket::submit(&rpc, second).await.unwrap();
        included.store(true, Ordering::SeqCst);
        retracted
            .executed(&rpc, Duration::from_secs(1))
            .await
            .unwrap();
        reorged.store(true, Ordering::SeqCst);
        assert!(
            retracted
                .finalized(&rpc, Duration::from_secs(1))
                .await
                .is_err()
        );
        assert_eq!(
            retracted.state(),
            &DeepXPerpOrderTicketState::ActionRequired
        );

        reorged.store(false, Ordering::SeqCst);
        included.store(false, Ordering::SeqCst);
        failed.store(true, Ordering::SeqCst);
        let third = DeepXPreparedPerpOrder::sign(&permit, &key, params, 1_725_000_000_125).unwrap();
        let failure_events = Arc::new(Mutex::new(Vec::<DeepXPerpOrderEvent>::new()));
        let observed = failure_events.clone();
        let mut rejected = DeepXPerpOrderMonitor::new(move |event| {
            observed.lock().unwrap().push(event.clone());
        });
        let rejected_hash = rejected.submit(&rpc, third).await.unwrap();
        included.store(true, Ordering::SeqCst);
        assert!(
            rejected
                .executed(&rpc, &rejected_hash, Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(matches!(
            rejected.get(&rejected_hash).unwrap().state(),
            DeepXPerpOrderTicketState::Failed(_)
        ));
        assert!(
            rejected
                .finalized(&rpc, &rejected_hash, Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(matches!(
            rejected.get(&rejected_hash).unwrap().state(),
            DeepXPerpOrderTicketState::FinalizedFailed(_)
        ));
        {
            let events = failure_events.lock().unwrap();
            assert_eq!(events.len(), 3);
            assert!(matches!(
                events[1].state,
                DeepXPerpOrderTicketState::Failed(_)
            ));
            assert!(matches!(
                events[2].state,
                DeepXPerpOrderTicketState::FinalizedFailed(_)
            ));
            assert!(events[1].error.is_some() && events[2].error.is_some());
        }
        server.abort();
    }
}
