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

//! Runtime pallet and variant identities from DeepX metadata.

use std::collections::BTreeSet;

use scale_info::{Variant, form::PortableForm};
use subxt_core::metadata::Metadata;
use thiserror::Error;

/// Errors produced while constructing or querying a runtime interface catalog.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DeepXRuntimeInterfaceError {
    /// A pallet name or index is duplicated in the runtime metadata.
    #[error("duplicate DeepX runtime pallet identity: {0}")]
    DuplicatePallet(String),
    /// A call name or index is duplicated within a pallet.
    #[error("duplicate DeepX runtime call identity: {0}.{1}")]
    DuplicateCall(String, String),
    /// An event name or index is duplicated within a pallet.
    #[error("duplicate DeepX runtime event identity: {0}.{1}")]
    DuplicateEvent(String, String),
    /// An error name or index is duplicated within a pallet.
    #[error("duplicate DeepX runtime error identity: {0}.{1}")]
    DuplicateError(String, String),
    /// The requested SCALE pallet index is absent from the approved runtime metadata.
    #[error("DeepX runtime pallet index is unavailable: {0}")]
    PalletIndexUnavailable(u8),
    /// The requested SCALE error index is absent from the specified pallet.
    #[error("DeepX runtime error index is unavailable: {0}.{1}")]
    ErrorIndexUnavailable(u8, u8),
    /// The requested pallet is absent from the approved runtime metadata.
    #[error("DeepX runtime pallet is unavailable: {0}")]
    PalletUnavailable(String),
    /// The requested call is absent from the approved runtime metadata.
    #[error("DeepX runtime call is unavailable: {0}.{1}")]
    CallUnavailable(String, String),
    /// The requested event is absent from the approved runtime metadata.
    #[error("DeepX runtime event is unavailable: {0}.{1}")]
    EventUnavailable(String, String),
}

/// Immutable metadata identity for one runtime call, event, or error variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRuntimeVariantIdentity {
    /// Variant name declared by the runtime metadata.
    name: String,
    /// SCALE variant index declared by the runtime metadata.
    index: u8,
}

impl DeepXRuntimeVariantIdentity {
    /// Returns the variant name declared by the runtime metadata.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SCALE variant index declared by the runtime metadata.
    #[must_use]
    pub const fn index(&self) -> u8 {
        self.index
    }
}

/// Immutable metadata identity and variants for one runtime pallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRuntimePalletInterface {
    /// Pallet name declared by the runtime metadata.
    name: String,
    /// SCALE pallet index declared by the runtime metadata.
    index: u8,
    /// Call variants declared by the runtime metadata.
    calls: Vec<DeepXRuntimeVariantIdentity>,
    /// Event variants declared by the runtime metadata.
    events: Vec<DeepXRuntimeVariantIdentity>,
    /// Error variants declared by the runtime metadata.
    errors: Vec<DeepXRuntimeVariantIdentity>,
}

impl DeepXRuntimePalletInterface {
    /// Returns the pallet name declared by the runtime metadata.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SCALE pallet index declared by the runtime metadata.
    #[must_use]
    pub const fn index(&self) -> u8 {
        self.index
    }

    /// Returns the call variants declared by the runtime metadata.
    #[must_use]
    pub fn calls(&self) -> &[DeepXRuntimeVariantIdentity] {
        &self.calls
    }

    /// Returns the event variants declared by the runtime metadata.
    #[must_use]
    pub fn events(&self) -> &[DeepXRuntimeVariantIdentity] {
        &self.events
    }

    /// Returns the error variants declared by the runtime metadata.
    #[must_use]
    pub fn errors(&self) -> &[DeepXRuntimeVariantIdentity] {
        &self.errors
    }
}

/// Immutable pallet, call, event, and error identities from runtime metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRuntimeInterfaceCatalog {
    pallets: Vec<DeepXRuntimePalletInterface>,
}

impl DeepXRuntimeInterfaceCatalog {
    /// Collects pallet and variant identities from runtime metadata.
    ///
    /// This catalog does not approve a runtime for signing.
    ///
    /// # Errors
    ///
    /// Returns an error when a pallet or variant name or index is duplicated.
    pub fn from_metadata(metadata: &Metadata) -> Result<Self, DeepXRuntimeInterfaceError> {
        let mut pallet_names = BTreeSet::new();
        let mut pallet_indices = BTreeSet::new();
        let mut pallets = Vec::with_capacity(metadata.pallets().len());

        for pallet in metadata.pallets() {
            if !pallet_names.insert(pallet.name()) || !pallet_indices.insert(pallet.index()) {
                return Err(DeepXRuntimeInterfaceError::DuplicatePallet(
                    pallet.name().to_string(),
                ));
            }
            let calls = collect_variants(
                pallet.name(),
                pallet.call_variants().unwrap_or_default(),
                DeepXRuntimeInterfaceError::DuplicateCall,
            )?;
            let events = collect_variants(
                pallet.name(),
                pallet.event_variants().unwrap_or_default(),
                DeepXRuntimeInterfaceError::DuplicateEvent,
            )?;
            let errors = collect_variants(
                pallet.name(),
                pallet.error_variants().unwrap_or_default(),
                DeepXRuntimeInterfaceError::DuplicateError,
            )?;
            pallets.push(DeepXRuntimePalletInterface {
                name: pallet.name().to_string(),
                index: pallet.index(),
                calls,
                events,
                errors,
            });
        }

        Ok(Self { pallets })
    }

    /// Returns all pallet interfaces in metadata order.
    #[must_use]
    pub fn pallets(&self) -> &[DeepXRuntimePalletInterface] {
        &self.pallets
    }

    /// Returns a pallet identity from the approved runtime metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the pallet is absent.
    pub fn pallet(
        &self,
        pallet: &str,
    ) -> Result<&DeepXRuntimePalletInterface, DeepXRuntimeInterfaceError> {
        self.pallets
            .iter()
            .find(|candidate| candidate.name == pallet)
            .ok_or_else(|| DeepXRuntimeInterfaceError::PalletUnavailable(pallet.to_string()))
    }

    /// Returns a call identity from the approved runtime metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the pallet or call is absent.
    pub fn call(
        &self,
        pallet: &str,
        call: &str,
    ) -> Result<&DeepXRuntimeVariantIdentity, DeepXRuntimeInterfaceError> {
        self.pallet(pallet)?
            .calls
            .iter()
            .find(|candidate| candidate.name == call)
            .ok_or_else(|| {
                DeepXRuntimeInterfaceError::CallUnavailable(pallet.to_string(), call.to_string())
            })
    }

    /// Returns pallet and error identities by their declared SCALE indices.
    ///
    /// This lookup does not decode a `DispatchError` or classify a transaction outcome.
    ///
    /// # Errors
    ///
    /// Returns distinct errors when the pallet index or its error index is absent.
    pub fn error_by_index(
        &self,
        pallet_index: u8,
        error_index: u8,
    ) -> Result<
        (&DeepXRuntimePalletInterface, &DeepXRuntimeVariantIdentity),
        DeepXRuntimeInterfaceError,
    > {
        let pallet = self
            .pallets
            .iter()
            .find(|candidate| candidate.index == pallet_index)
            .ok_or(DeepXRuntimeInterfaceError::PalletIndexUnavailable(
                pallet_index,
            ))?;
        let error = pallet
            .errors
            .iter()
            .find(|candidate| candidate.index == error_index)
            .ok_or(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(
                pallet_index,
                error_index,
            ))?;
        Ok((pallet, error))
    }

    /// Returns an event identity from the approved runtime metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the pallet or event is absent.
    pub fn event(
        &self,
        pallet: &str,
        event: &str,
    ) -> Result<&DeepXRuntimeVariantIdentity, DeepXRuntimeInterfaceError> {
        self.pallet(pallet)?
            .events
            .iter()
            .find(|candidate| candidate.name == event)
            .ok_or_else(|| {
                DeepXRuntimeInterfaceError::EventUnavailable(pallet.to_string(), event.to_string())
            })
    }
}

fn collect_variants(
    pallet: &str,
    variants: &[Variant<PortableForm>],
    duplicate_error: fn(String, String) -> DeepXRuntimeInterfaceError,
) -> Result<Vec<DeepXRuntimeVariantIdentity>, DeepXRuntimeInterfaceError> {
    let mut names = BTreeSet::new();
    let mut indices = BTreeSet::new();
    let mut identities = Vec::with_capacity(variants.len());
    for variant in variants {
        if !names.insert(variant.name.clone()) || !indices.insert(variant.index) {
            return Err(duplicate_error(pallet.to_string(), variant.name.clone()));
        }
        identities.push(DeepXRuntimeVariantIdentity {
            name: variant.name.clone(),
            index: variant.index,
        });
    }
    Ok(identities)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("duplicate name", "first", 1, "first", 2)]
    #[case("duplicate index", "first", 1, "second", 1)]
    fn runtime_interface_rejects_duplicate_variant_identity(
        #[case] _description: &str,
        #[case] first_name: &str,
        #[case] first_index: u8,
        #[case] second_name: &str,
        #[case] second_index: u8,
    ) {
        let variants = [
            Variant::<PortableForm> {
                name: first_name.to_string(),
                fields: Vec::new(),
                index: first_index,
                docs: Vec::new(),
            },
            Variant::<PortableForm> {
                name: second_name.to_string(),
                fields: Vec::new(),
                index: second_index,
                docs: Vec::new(),
            },
        ];

        assert_eq!(
            collect_variants(
                "System",
                &variants,
                DeepXRuntimeInterfaceError::DuplicateCall,
            ),
            Err(DeepXRuntimeInterfaceError::DuplicateCall(
                "System".to_string(),
                second_name.to_string(),
            )),
        );
    }
}
