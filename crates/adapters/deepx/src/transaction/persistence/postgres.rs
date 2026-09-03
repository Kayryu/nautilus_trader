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

//! PostgreSQL transaction persistence over the Nautilus general cache table.

use std::fmt::{Debug, Formatter};

use sqlx::{PgConnection, PgPool, Row};
use subxt_core::config::{Hasher, substrate::BlakeTwo256};
use tokio::sync::Mutex;

use super::{
    DeepXCommittedTransactionRecord, DeepXRestoredTransactionRecord, DeepXSignerLease,
    DeepXTransactionPersistenceError, DeepXTransactionRevision, DeepXTransactionStore,
};
use crate::transaction::{DEEPX_TRANSACTION_CACHE_KEY_PREFIX, DeepXTransactionRecord};

const ENVELOPE_MAGIC: &[u8; 4] = b"DXTX";
const ENVELOPE_VERSION: u8 = 1;
const ENVELOPE_HEADER_LEN: usize = ENVELOPE_MAGIC.len() + 1 + size_of::<u64>();
const ADVISORY_LOCK_DOMAIN: &[u8] = b"nautilus:deepx:signer-lease:v1";

/// PostgreSQL-backed DeepX transaction store using the Nautilus `general` cache table.
#[derive(Clone, Debug)]
pub struct DeepXPostgresTransactionStore {
    pool: PgPool,
}

impl DeepXPostgresTransactionStore {
    /// Creates a store from the same pool used by the Nautilus PostgreSQL cache database.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Connection-scoped exclusive ownership of one DeepX signer nonce domain.
///
/// Dropping this value closes the detached PostgreSQL connection and releases its session-level
/// advisory lock. The connection is never returned to the pool with the lock still held.
pub struct DeepXPostgresSignerLease {
    signer: [u8; 20],
    generation: u64,
    connection: Mutex<PgConnection>,
}

impl Debug for DeepXPostgresSignerLease {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct(stringify!(DeepXPostgresSignerLease))
            .field("signer", &self.signer)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl DeepXSignerLease for DeepXPostgresSignerLease {
    fn signer(&self) -> [u8; 20] {
        self.signer
    }

    fn generation(&self) -> u64 {
        self.generation
    }
}

#[async_trait::async_trait]
impl DeepXTransactionStore for DeepXPostgresTransactionStore {
    type Lease = DeepXPostgresSignerLease;

    async fn acquire_signer_lease(
        &self,
        signer: [u8; 20],
    ) -> Result<Self::Lease, DeepXTransactionPersistenceError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| {
                DeepXTransactionPersistenceError::LeaseUnavailable(format!(
                    "failed to acquire PostgreSQL connection: {error}"
                ))
            })?
            .detach();
        let lock_keys = signer_lock_keys(signer);
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1, $2)")
            .bind(lock_keys.0)
            .bind(lock_keys.1)
            .fetch_one(&mut connection)
            .await
            .map_err(|error| {
                DeepXTransactionPersistenceError::LeaseUnavailable(format!(
                    "failed to acquire PostgreSQL advisory lock: {error}"
                ))
            })?;
        if !acquired {
            return Err(DeepXTransactionPersistenceError::LeaseUnavailable(
                "another process owns this signer".to_string(),
            ));
        }
        let generation = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(&mut connection)
            .await
            .map_err(|error| {
                DeepXTransactionPersistenceError::LeaseUnavailable(format!(
                    "failed to identify PostgreSQL signer lease: {error}"
                ))
            })? as u32 as u64;

        Ok(DeepXPostgresSignerLease {
            signer,
            generation,
            connection: Mutex::new(connection),
        })
    }

    async fn verify_signer_lease(
        &self,
        lease: &Self::Lease,
    ) -> Result<(), DeepXTransactionPersistenceError> {
        let mut connection = lease.connection.lock().await;
        verify_connection(lease, &mut connection).await
    }

    async fn load_committed_for_signer(
        &self,
        lease: &Self::Lease,
    ) -> Result<Vec<DeepXRestoredTransactionRecord>, DeepXTransactionPersistenceError> {
        let mut connection = lease.connection.lock().await;
        verify_connection(lease, &mut connection).await?;
        let rows = sqlx::query(
            "SELECT id, value FROM general WHERE left(id, length($1)) = $1 ORDER BY id",
        )
        .bind(DEEPX_TRANSACTION_CACHE_KEY_PREFIX)
        .fetch_all(&mut *connection)
        .await
        .map_err(before_commit("load DeepX transaction records"))?;
        let mut restored = Vec::new();
        for row in rows {
            let cache_key: String = row
                .try_get("id")
                .map_err(before_commit("decode DeepX transaction cache key"))?;
            let envelope: Vec<u8> = row
                .try_get("value")
                .map_err(before_commit("decode DeepX transaction envelope"))?;
            let (revision, encoded_record) = decode_envelope(&envelope)?;
            let record = DeepXTransactionRecord::decode(encoded_record).map_err(|error| {
                DeepXTransactionPersistenceError::BeforeCommit(error.to_string())
            })?;
            if cache_key != record.cache_key_for_record() {
                return Err(DeepXTransactionPersistenceError::AcknowledgementMismatch);
            }
            if record.identity().signer() == lease.signer {
                let committed =
                    DeepXCommittedTransactionRecord::acknowledge_committed(&record, revision)?;
                restored.push(DeepXRestoredTransactionRecord::new(record, committed)?);
            }
        }
        Ok(restored)
    }

    async fn create_committed(
        &self,
        lease: &Self::Lease,
        record: &DeepXTransactionRecord,
    ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
        verify_record_signer(lease, record)?;
        let revision = DeepXTransactionRevision::new(1);
        let encoded_record = record
            .encode()
            .map_err(|error| DeepXTransactionPersistenceError::BeforeCommit(error.to_string()))?;
        let envelope = encode_envelope(revision, &encoded_record);
        let mut connection = lease.connection.lock().await;
        verify_connection(lease, &mut connection).await?;
        let result = sqlx::query("INSERT INTO general (id, value) VALUES ($1, $2)")
            .bind(record.cache_key_for_record())
            .bind(envelope)
            .execute(&mut *connection)
            .await;
        match result {
            Ok(result) if result.rows_affected() == 1 => {
                DeepXCommittedTransactionRecord::acknowledge_committed(record, revision)
            }
            Ok(_) => Err(DeepXTransactionPersistenceError::CommitOutcomeUnknown(
                "PostgreSQL insert did not report one affected row".to_string(),
            )),
            Err(error) if is_unique_violation(&error) => {
                Err(DeepXTransactionPersistenceError::RevisionConflict)
            }
            Err(error) => Err(DeepXTransactionPersistenceError::CommitOutcomeUnknown(
                format!("PostgreSQL insert acknowledgement failed: {error}"),
            )),
        }
    }

    async fn compare_and_set_committed(
        &self,
        lease: &Self::Lease,
        expected: &DeepXCommittedTransactionRecord,
        record: &DeepXTransactionRecord,
    ) -> Result<DeepXCommittedTransactionRecord, DeepXTransactionPersistenceError> {
        verify_record_signer(lease, record)?;
        if expected.cache_key != record.cache_key_for_record() {
            return Err(DeepXTransactionPersistenceError::AcknowledgementMismatch);
        }
        let revision = DeepXTransactionRevision::new(
            expected.revision.value().checked_add(1).ok_or_else(|| {
                DeepXTransactionPersistenceError::BeforeCommit(
                    "DeepX transaction revision overflow".to_string(),
                )
            })?,
        );
        let encoded_record = record
            .encode()
            .map_err(|error| DeepXTransactionPersistenceError::BeforeCommit(error.to_string()))?;
        let expected_envelope = encode_envelope(expected.revision, &expected.encoded_record);
        let replacement_envelope = encode_envelope(revision, &encoded_record);
        let mut connection = lease.connection.lock().await;
        verify_connection(lease, &mut connection).await?;
        let result = sqlx::query("UPDATE general SET value = $1 WHERE id = $2 AND value = $3")
            .bind(replacement_envelope)
            .bind(&expected.cache_key)
            .bind(expected_envelope)
            .execute(&mut *connection)
            .await
            .map_err(|error| {
                DeepXTransactionPersistenceError::CommitOutcomeUnknown(format!(
                    "PostgreSQL compare-and-set acknowledgement failed: {error}"
                ))
            })?;
        if result.rows_affected() != 1 {
            return Err(DeepXTransactionPersistenceError::RevisionConflict);
        }
        DeepXCommittedTransactionRecord::acknowledge_committed(record, revision)
    }
}

async fn verify_connection(
    lease: &DeepXPostgresSignerLease,
    connection: &mut PgConnection,
) -> Result<(), DeepXTransactionPersistenceError> {
    let generation = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(connection)
        .await
        .map_err(|error| {
            DeepXTransactionPersistenceError::LeaseUnavailable(format!(
                "PostgreSQL signer lease connection is unavailable: {error}"
            ))
        })? as u32 as u64;
    if generation != lease.generation {
        return Err(DeepXTransactionPersistenceError::LeaseUnavailable(
            "PostgreSQL signer lease generation changed".to_string(),
        ));
    }
    Ok(())
}

fn verify_record_signer(
    lease: &DeepXPostgresSignerLease,
    record: &DeepXTransactionRecord,
) -> Result<(), DeepXTransactionPersistenceError> {
    if record.identity().signer() == lease.signer {
        Ok(())
    } else {
        Err(DeepXTransactionPersistenceError::LeaseMismatch)
    }
}

fn signer_lock_keys(signer: [u8; 20]) -> (i32, i32) {
    let mut input = Vec::with_capacity(ADVISORY_LOCK_DOMAIN.len() + signer.len());
    input.extend_from_slice(ADVISORY_LOCK_DOMAIN);
    input.extend_from_slice(&signer);
    let digest = BlakeTwo256.hash(&input);
    (
        i32::from_be_bytes(digest[..4].try_into().expect("four-byte lock key")),
        i32::from_be_bytes(digest[4..8].try_into().expect("four-byte lock key")),
    )
}

fn encode_envelope(revision: DeepXTransactionRevision, encoded_record: &[u8]) -> Vec<u8> {
    let mut envelope = Vec::with_capacity(ENVELOPE_HEADER_LEN + encoded_record.len());
    envelope.extend_from_slice(ENVELOPE_MAGIC);
    envelope.push(ENVELOPE_VERSION);
    envelope.extend_from_slice(&revision.value().to_be_bytes());
    envelope.extend_from_slice(encoded_record);
    envelope
}

fn decode_envelope(
    envelope: &[u8],
) -> Result<(DeepXTransactionRevision, &[u8]), DeepXTransactionPersistenceError> {
    if envelope.len() <= ENVELOPE_HEADER_LEN
        || &envelope[..ENVELOPE_MAGIC.len()] != ENVELOPE_MAGIC
        || envelope[ENVELOPE_MAGIC.len()] != ENVELOPE_VERSION
    {
        return Err(DeepXTransactionPersistenceError::BeforeCommit(
            "invalid DeepX PostgreSQL transaction envelope".to_string(),
        ));
    }
    let revision_start = ENVELOPE_MAGIC.len() + 1;
    let revision = u64::from_be_bytes(
        envelope[revision_start..ENVELOPE_HEADER_LEN]
            .try_into()
            .expect("eight-byte revision"),
    );
    if revision == 0 {
        return Err(DeepXTransactionPersistenceError::BeforeCommit(
            "invalid zero DeepX transaction revision".to_string(),
        ));
    }
    Ok((
        DeepXTransactionRevision::new(revision),
        &envelope[ENVELOPE_HEADER_LEN..],
    ))
}

fn before_commit(
    operation: &'static str,
) -> impl FnOnce(sqlx::Error) -> DeepXTransactionPersistenceError {
    move |error| {
        DeepXTransactionPersistenceError::BeforeCommit(format!("failed to {operation}: {error}"))
    }
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "23505")
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn envelope_round_trips_exact_record_bytes_and_revision() {
        let encoded_record = br#"{"version":1}"#;
        let revision = DeepXTransactionRevision::new(42);

        let envelope = encode_envelope(revision, encoded_record);
        let (decoded_revision, decoded_record) = decode_envelope(&envelope).unwrap();

        assert_eq!(decoded_revision, revision);
        assert_eq!(decoded_record, encoded_record);
    }

    #[rstest]
    fn envelope_rejects_unknown_version_and_zero_revision() {
        let mut unknown_version = encode_envelope(DeepXTransactionRevision::new(1), b"record");
        unknown_version[ENVELOPE_MAGIC.len()] = 2;
        assert!(decode_envelope(&unknown_version).is_err());

        let zero_revision = encode_envelope(DeepXTransactionRevision::new(0), b"record");
        assert!(decode_envelope(&zero_revision).is_err());
    }

    #[rstest]
    fn signer_lock_keys_are_stable_and_signer_scoped() {
        assert_eq!(signer_lock_keys([1; 20]), signer_lock_keys([1; 20]));
        assert_ne!(signer_lock_keys([1; 20]), signer_lock_keys([2; 20]));
    }

    #[cfg(target_os = "linux")]
    mod serial_tests {
        use std::{
            env,
            time::{Duration, SystemTime},
        };

        use nautilus_model::{
            enums::OrderSide,
            identifiers::{ClientOrderId, InstrumentId},
        };
        use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

        use super::*;
        use crate::transaction::{
            DeepXDirectRuntimeIdentity, DeepXNonceReservation, DeepXTransactionIdentity,
            DeepXTransactionObservation,
        };

        #[tokio::test]
        #[ignore = "requires a local PostgreSQL service"]
        async fn postgres_store_enforces_leases_and_exact_committed_cas() {
            let options = test_postgres_options();
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect_with(options.clone())
                .await
                .expect("PostgreSQL service is required for this ignored integration test");
            let unique = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let schema = format!("deepx_transaction_{}_{}", std::process::id(), unique);
            sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
                .execute(&admin_pool)
                .await
                .unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {schema}.general (id TEXT PRIMARY KEY NOT NULL, value BYTEA NOT NULL)"
            )))
            .execute(&admin_pool)
            .await
            .unwrap();

            let scoped_options = options.options([("search_path", schema.clone())]);
            let pool_a = PgPoolOptions::new()
                .max_connections(3)
                .connect_with(scoped_options.clone())
                .await
                .unwrap();
            let pool_b = PgPoolOptions::new()
                .max_connections(3)
                .connect_with(scoped_options)
                .await
                .unwrap();
            let store_a = DeepXPostgresTransactionStore::new(pool_a);
            let store_b = DeepXPostgresTransactionStore::new(pool_b);
            let record_a = record([7; 20], "O-19700101-000000-001-001-1", 42);
            let record_b = record([8; 20], "O-19700101-000000-001-001-2", 43);

            let lease_a = store_a.acquire_signer_lease([7; 20]).await.unwrap();
            assert!(matches!(
                store_b.acquire_signer_lease([7; 20]).await,
                Err(DeepXTransactionPersistenceError::LeaseUnavailable(_)),
            ));
            let lease_b = store_b.acquire_signer_lease([8; 20]).await.unwrap();

            let committed_a = store_a.create_committed(&lease_a, &record_a).await.unwrap();
            store_b.create_committed(&lease_b, &record_b).await.unwrap();
            assert!(matches!(
                store_a.create_committed(&lease_a, &record_b).await,
                Err(DeepXTransactionPersistenceError::LeaseMismatch),
            ));
            let restored = store_a.load_committed_for_signer(&lease_a).await.unwrap();
            assert_eq!(restored.len(), 1);
            assert_eq!(restored[0].record(), &record_a);

            let mut replacement = record_a.clone();
            replacement
                .apply_observation(DeepXTransactionObservation::ActionRequired)
                .unwrap();
            let committed_replacement = store_a
                .compare_and_set_committed(&lease_a, &committed_a, &replacement)
                .await
                .unwrap();
            assert_eq!(committed_replacement.revision().value(), 2);
            assert!(matches!(
                store_a
                    .compare_and_set_committed(&lease_a, &committed_a, &replacement)
                    .await,
                Err(DeepXTransactionPersistenceError::RevisionConflict),
            ));

            drop(lease_a);
            let released_lease = store_b.acquire_signer_lease([7; 20]).await.unwrap();
            drop(released_lease);
            drop(lease_b);
            sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
                .execute(&admin_pool)
                .await
                .unwrap();
        }

        fn test_postgres_options() -> PgConnectOptions {
            PgConnectOptions::new()
                .host(&env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()))
                .port(
                    env::var("POSTGRES_PORT")
                        .map_or(5432, |port| port.parse().expect("valid POSTGRES_PORT")),
                )
                .username(&env::var("POSTGRES_USERNAME").unwrap_or_else(|_| "nautilus".to_string()))
                .password(&env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "pass".to_string()))
                .database(&env::var("POSTGRES_DATABASE").unwrap_or_else(|_| "nautilus".to_string()))
        }

        fn record(signer: [u8; 20], client_order_id: &str, nonce: u64) -> DeepXTransactionRecord {
            DeepXTransactionRecord::created(DeepXTransactionIdentity::new(
                ClientOrderId::new(client_order_id),
                signer,
                InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
                OrderSide::Buy,
                DeepXNonceReservation::TimestampOrderId { value: nonce },
                DeepXDirectRuntimeIdentity {
                    genesis_hash: [1; 32],
                    metadata_sha256: [2; 32],
                    spec_version: 366,
                    transaction_version: 1,
                    signed_extensions: vec!["CheckNonce".to_string()],
                },
            ))
        }
    }
}
