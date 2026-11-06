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

//! Durable single-opportunity execution state, locked for the process lifetime.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use subxt_core::config::{Hasher, substrate::BlakeTwo256};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    checksum: String,
    state: State,
}

fn checksum(state: &State) -> anyhow::Result<String> {
    Ok(hex::encode(BlakeTwo256.hash(&serde_json::to_vec(state)?)))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub(super) version: u8,
    pub(super) genesis: String,
    pub(super) signer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) wallet: Option<String>,
    pub(super) subaccount: String,
    pub(super) pair: String,
    pub(super) runtime_code_hash: String,
    pub(super) target_bytes: String,
    pub(super) target_hash: String,
    pub(super) target_nonce: u64,
    pub(super) target_subaccount: String,
    pub(super) target_outcome: Option<bool>,
    pub(super) target_first_trade: Option<(u64, u32)>,
    pub(super) entry_first_trade: Option<(u64, u32)>,
    pub(super) entry_last_trade: Option<(u64, u32)>,
    pub(super) cursor_number: u64,
    pub(super) cursor_hash: String,
    pub(super) exit_after_blocks: u64,
    pub(super) maximum_loss: String,
    pub(super) cost_budget: String,
    pub(super) minimum_profit: String,
    pub(super) exit_price: String,
    pub(super) base_scale: String,
    pub(super) base_symbol: Vec<u8>,
    pub(super) quote_symbol: Vec<u8>,
    pub(super) acquired_base: String,
    pub(super) spent_quote: String,
    pub(super) sold_base: String,
    pub(super) received_quote: String,
    pub(super) entry: Operation,
    pub(super) exits: Vec<Operation>,
    pub(super) complete: bool,
    pub(super) result: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Operation {
    pub(super) nonce: u64,
    pub(super) bytes: String,
    pub(super) hash: String,
    pub(super) base: String,
    pub(super) quote: String,
    pub(super) attempted: bool,
    pub(super) submission_error: Option<String>,
    pub(super) included: Option<Inclusion>,
    pub(super) expired: bool,
    pub(super) terminal: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Inclusion {
    pub(super) number: u64,
    pub(super) hash: String,
    pub(super) index: u32,
    pub(super) success: bool,
}

impl Operation {
    pub(super) fn settled(&self) -> bool {
        self.expired
            || self
                .included
                .as_ref()
                .is_some_and(|i| !i.success || self.terminal)
    }
}

impl State {
    pub(super) fn remaining(&self) -> anyhow::Result<u128> {
        amount(&self.acquired_base)?
            .checked_sub(amount(&self.sold_base)?)
            .context("exit fills exceed acquired base")
    }

    pub(super) fn profit(&self) -> anyhow::Result<String> {
        let costs = amount(&self.spent_quote)?
            .checked_add(amount(&self.cost_budget)?)
            .context("total cost overflow")?;
        let received = amount(&self.received_quote)?;
        Ok(if received >= costs {
            (received - costs).to_string()
        } else {
            format!("-{}", costs - received)
        })
    }
}

pub(super) fn amount(value: &str) -> anyhow::Result<u128> {
    anyhow::ensure!(
        !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()),
        "invalid journal financial integer"
    );
    Ok(value.parse()?)
}

pub(super) struct Journal {
    path: PathBuf,
    _lock: File,
}

impl Journal {
    pub(super) fn open(path: &Path) -> anyhow::Result<Self> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = parent
            .canonicalize()
            .context("journal directory must exist")?;
        let name = path.file_name().context("journal requires a filename")?;
        let path = parent.join(name);
        let lock_path = parent.join(format!("{}.lock", name.to_string_lossy()));
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock()
            .context("execution journal is already locked")?;
        Ok(Self { path, _lock: lock })
    }

    pub(super) fn load(&self) -> anyhow::Result<Option<State>> {
        match fs::read(&self.path) {
            Ok(bytes) => {
                let checkpoint: Checkpoint =
                    serde_json::from_slice(&bytes).context("invalid execution journal")?;
                let state = checkpoint.state;
                anyhow::ensure!(
                    checkpoint.checksum == checksum(&state)?,
                    "execution journal checksum mismatch"
                );
                anyhow::ensure!(state.version == 1, "unsupported execution journal version");
                Ok(Some(state))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) fn store(&self, state: &State) -> anyhow::Result<()> {
        let parent = self.path.parent().context("journal directory missing")?;
        let temporary = self.path.with_file_name(format!(
            ".{}.{}.tmp",
            self.path
                .file_name()
                .context("journal filename missing")?
                .to_string_lossy(),
            std::process::id(),
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .context("cannot create journal checkpoint")?;
        let result = (|| -> anyhow::Result<()> {
            file.write_all(&serde_json::to_vec_pretty(&Checkpoint {
                checksum: checksum(state)?,
                state: state.clone(),
            })?)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.context("execution checkpoint was not durably committed")
    }
}
