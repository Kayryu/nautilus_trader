//! Exact DeepX wire identities shared by REST, WebSocket, and adapter boundaries.

/// Decodes a `0x`-prefixed AccountId20 address.
#[must_use]
pub fn decode_account_id20(value: &str) -> Option<[u8; 20]> {
    let encoded = value.strip_prefix("0x")?;
    let mut address = [0; 20];
    hex::decode_to_slice(encoded, &mut address).ok()?;
    Some(address)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc", true)]
    #[case("0x4DED31CB63949B52F9DFC9BCFADE4EAB7017EADC", true)]
    #[case("4ded31cb63949b52f9dfc9bcfade4eab7017eadc", false)]
    #[case("0X4ded31cb63949b52f9dfc9bcfade4eab7017eadc", false)]
    #[case("0x00", false)]
    #[case("0x4ded31cb63949b52f9dfc9bcfade4eab7017eadg", false)]
    fn account_id20_requires_exact_prefixed_hex(#[case] value: &str, #[case] valid: bool) {
        assert_eq!(decode_account_id20(value).is_some(), valid);
    }
}
