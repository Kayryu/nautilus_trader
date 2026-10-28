"""Verify pinned DeepX SDK signing against captured runtime and Rust regression bytes.

No RPC, submission, or user credentials are used. Inputs are raw SDK Python files
or GitHub contents API responses at the pinned blobs below.
"""

import argparse
import ast
import base64
import copy
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from substrateinterface import Keypair, KeypairType, SubstrateInterface

SDK_BLOB = "0108f89d79366f13e747ef59c75ce73a543dc404"
COMPAT_BLOB = "06b2e9fc84cc36c647452027b9c5b2cbbfcafb8c"
TEST_KEY = "0123456789abcdef" * 4
SPEC_376_FINALIZED_PERP_CANCEL = (
    "050284bb3d6fdc376a153ead60bc2ab97dd0be7cd55960065f5de9b395a3c3155f8837b88532"
    "03f944ce34754e59ee71b1b514b96f9e420222a084aa4374aac260da37b6acb697514a27657a75"
    "aa1aafe5d278e5ce591801000b0dc99734a101001603a13c7ab8decf4d32cf80c371647c93d0e2"
    "f4183e60982d21a101000002000000"
)
FIXTURES = Path(__file__).resolve().parents[1] / "deepx-rust-sdk/test_data/runtime/testnet"
SNAPSHOT = FIXTURES / "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2"


class OfflineSubstrate(SubstrateInterface):
    def __init__(self):
        self.fixture_metadata = json.loads((SNAPSHOT / "metadata.json").read_text())
        self.fixture_version = json.loads((SNAPSHOT / "runtime_version.json").read_text())
        self.fixture_genesis = json.loads((SNAPSHOT / "genesis_hash.json").read_text())
        identity = json.loads((SNAPSHOT / "manifest.json").read_text())["identity"]
        metadata = bytes.fromhex(self.fixture_metadata["result"].removeprefix("0x"))
        expected_hash = "57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447"
        expected_genesis = "0x87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7"
        if (
            hashlib.sha256(metadata).hexdigest() != expected_hash
            or identity["metadata_sha256"] != expected_hash
            or identity["genesis_hash"] != self.fixture_genesis["result"]
            or self.fixture_genesis["result"] != expected_genesis
            or self.fixture_version["result"]["specVersion"] != 376
            or self.fixture_version["result"]["transactionVersion"] != 1
        ):
            raise ValueError("Runtime fixture identity is not the reviewed snapshot")
        super().__init__(url="http://offline.invalid", auto_discover=False, ss58_format=42)

    def rpc_request(self, method, params, *args, **kwargs):
        if method == "rpc_methods":
            return {"result": {"methods": ["chain_getHead"]}}
        if method == "state_getMetadata":
            return self.fixture_metadata
        if method in ("state_getRuntimeVersion", "chain_getRuntimeVersion"):
            return self.fixture_version
        if method in ("chain_getHead", "chain_getBlockHash"):
            return self.fixture_genesis
        if method == "chain_getHeader":
            return {"result": {"parentHash": "0x" + "00" * 32, "number": "0x0"}}
        raise AssertionError(f"Unexpected offline RPC request: {method}")


def load_pinned_source(path: Path, expected_blob: str) -> bytes:
    if path.suffix == ".json":
        response = json.loads(path.read_text())
        source = base64.b64decode(
            response["content"].replace("\r", "").replace("\n", ""), validate=True
        )
        if response["sha"] != expected_blob:
            raise ValueError("SDK source response has an unexpected GitHub blob")
    else:
        source = path.read_bytes()
    blob = hashlib.sha1(b"blob " + str(len(source)).encode() + b"\0" + source).hexdigest()
    if blob != expected_blob:
        raise ValueError("SDK source is not the reviewed pinned GitHub blob")
    return source


def load_sdk_builder(sdk_source: Path, compat_source: Path):
    source = load_pinned_source(sdk_source, SDK_BLOB)
    compatibility = load_pinned_source(compat_source, COMPAT_BLOB)
    compat_tree = ast.parse(compatibility)
    compat_items = [
        item
        for item in compat_tree.body
        if (isinstance(item, ast.ClassDef) and item.name == "_ImmortalMetadata")
        or (isinstance(item, ast.FunctionDef) and item.name == "create_signed_extrinsic")
    ]
    if {item.name for item in compat_items} != {
        "_ImmortalMetadata",
        "create_signed_extrinsic",
    } or len(compat_items) != 2:
        raise ValueError("Pinned SDK compatibility functions are missing")
    compat_namespace = {"Any": Any, "copy": copy}
    exec(  # noqa: S102 - exact reviewed blob and selected definitions only
        compile(ast.Module(body=compat_items, type_ignores=[]), str(compat_source), "exec"),
        compat_namespace,
    )

    # Execute only the reviewed builder and key constructor with offline transport.
    selected = {"build_signed_pallet_call_extrinsic", "_create_ecdsa_keypair"}
    tree = ast.parse(source)
    functions = [
        item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name in selected
    ]
    if {item.name for item in functions} != selected:
        raise ValueError("Pinned SDK functions are missing")
    namespace = {
        "Any": Any,
        "Optional": Optional,
        "_get_substrate_interface_cls": lambda: OfflineSubstrate,
        "_get_substrate_keypair_libs": lambda: (Keypair, KeypairType),
        "_create_substrate": lambda cls, endpoint: cls(),
        "create_signed_extrinsic": compat_namespace["create_signed_extrinsic"],
    }
    exec(  # noqa: S102 - exact reviewed blob and two selected functions only
        compile(ast.Module(body=functions, type_ignores=[]), str(sdk_source), "exec"),
        namespace,
    )
    return namespace["build_signed_pallet_call_extrinsic"]


def verify_spec376_finalized_signature() -> None:
    """Check the immortal signing context against a finalized on-chain extrinsic."""
    from eth_keys import keys
    from eth_utils import keccak

    # PerpMarket.cancel_order in finalized block 18476010,
    # hash 0x841d9d8cef55aa90a20b79f18d79737c25b47ef8b6012205d1668e83efc2278d
    encoded = bytes.fromhex(SPEC_376_FINALIZED_PERP_CANCEL)
    if len(encoded) - 2 != int.from_bytes(encoded[:2], "little") >> 2:
        raise AssertionError("Finalized extrinsic has an invalid SCALE length")
    if encoded[2] != 0x84:
        raise AssertionError("Finalized extrinsic is not signed version 4")
    account = encoded[3:23]
    signature = keys.Signature(signature_bytes=encoded[23:88])
    extra, call = encoded[88:97], encoded[97:]
    if len(extra) != 9 or extra[0] != 0 or extra[1] != 0x0B or extra[-1] != 0:
        raise AssertionError("Finalized extrinsic has an unexpected immortal extension layout")
    if call[:2] != bytes((22, 3)) or len(call) != 34:
        raise AssertionError("Finalized extrinsic is not the captured perpetual cancel")

    genesis = bytes.fromhex("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")

    def recovered(spec_version: int, era_implicit: bytes) -> bytes:
        payload = (
            call
            + extra
            + spec_version.to_bytes(4, "little")
            + (1).to_bytes(4, "little")
            + genesis
            + era_implicit
        )
        return signature.recover_public_key_from_msg_hash(keccak(payload)).to_canonical_address()

    if (
        recovered(376, genesis) != account
        or recovered(375, genesis) == account
        or recovered(376, bytes(32)) == account
    ):
        raise AssertionError("Finalized signature does not bind spec 376 and immortal genesis")
    print("Finalized spec376 signature context: PerpMarket.cancel_order")


def load_sdk_evm_builder(sdk_source: Path):
    source = load_pinned_source(sdk_source, SDK_BLOB)
    tree = ast.parse(source)
    selected = {
        "build_signed_tx",
        "_parse_account",
        "_normalize_address",
        "_decode_hex_bytes",
    }
    items = [
        item
        for item in tree.body
        if (isinstance(item, ast.FunctionDef) and item.name in selected)
        or (isinstance(item, ast.ClassDef) and item.name == "SignedTxPayload")
    ]
    if {item.name for item in items} != selected | {"SignedTxPayload"}:
        raise ValueError("Pinned SDK EVM signing functions are missing")

    from eth_account import Account
    from eth_utils import keccak, to_canonical_address, to_checksum_address

    def offline_rpc(*_args, **_kwargs):
        raise AssertionError("Offline EVM signing attempted an RPC request")

    namespace = {
        "Any": Any,
        "Optional": Optional,
        "dataclass": dataclass,
        "_get_signing_libs": lambda: (
            Account,
            keccak,
            to_canonical_address,
            to_checksum_address,
        ),
        "_rpc_get_chain_id": offline_rpc,
        "_get_native_account_nonce": offline_rpc,
        "_rpc_estimate_gas": offline_rpc,
    }
    exec(  # noqa: S102 - exact reviewed blob and selected definitions only
        compile(ast.Module(body=items, type_ignores=[]), str(sdk_source), "exec"),
        namespace,
    )
    return namespace["build_signed_tx"]


def load_sdk_evm_wrapper(sdk_source: Path):
    source = load_pinned_source(sdk_source, SDK_BLOB)
    selected = {
        "build_ethereum_transact_extrinsic",
        "_decode_signed_rlp_bytes_to_transaction_v2",
        "_map_legacy_tx",
        "_map_eip1559_tx",
        "_map_eip2930_tx",
        "_map_access_list",
        "_map_action",
        "_legacy_recovery_id",
        "_u256_to_h256_hex",
        "_bytes_to_hex",
        "_bytes_to_h160_hex",
        "_bytes_to_h256_hex",
        "_normalize_h160_hex",
        "_decode_hex_bytes",
        "_compose_ethereum_transact_call",
    }
    items = [
        item
        for item in ast.parse(source).body
        if isinstance(item, ast.FunctionDef) and item.name in selected
    ]
    if {item.name for item in items} != selected:
        raise ValueError("Pinned SDK EVM wrapper functions are missing")

    from eth_utils import to_canonical_address

    namespace = {
        "Any": Any,
        "Optional": Optional,
        "_get_signing_libs": lambda: (None, None, to_canonical_address, None),
        "_get_substrate_interface_cls": lambda: OfflineSubstrate,
        "_create_substrate": lambda cls, endpoint: cls(),
    }
    exec(  # noqa: S102 - exact reviewed blob and selected definitions only
        compile(ast.Module(body=items, type_ignores=[]), str(sdk_source), "exec"),
        namespace,
    )
    return namespace["build_ethereum_transact_extrinsic"]


def verify_evm_vectors(sdk_source: Path):
    builder = load_sdk_evm_builder(sdk_source)
    wrapper_builder = load_sdk_evm_wrapper(sdk_source)
    from eth_abi import encode as abi_encode
    from eth_utils import keccak

    rust_source = (Path(__file__).resolve().parents[1] / "src/signing/evm.rs").read_text()
    signature = "setProfitAndLossPoint(address,uint16,uint128,uint128)"
    pnl_calldata = keccak(text=signature)[:4] + abi_encode(
        ["address", "uint16", "uint128", "uint128"],
        ["0x" + "11" * 20, 7, 123, 456],
    )
    for use_legacy in (False, True):
        payload = builder(
            evm_rpc_url="http://offline.invalid",
            private_key=TEST_KEY,
            precompile_address="0x" + "11" * 20,
            data_hex="0x12345678",
            value=0,
            chain_id=0x12EE,
            gas_limit=500_000,
            use_legacy=use_legacy,
            nonce_ms=1_725_000_000_140,
        )
        for value in (payload.signed_tx[2:], payload.tx_hash[2:], payload.signer[2:]):
            if f'"{value}"' not in rust_source:
                raise AssertionError("SDK/Rust offline EVM vector mismatch")
        mode = "legacy" if use_legacy else "EIP-1559"
        print(f"SDK/Rust complete offline EVM envelope parity: {mode}")
        wrapper = wrapper_builder(
            substrate_ws="ws://offline.invalid",
            signed_tx_hex=payload.signed_tx,
            signer=payload.signer,
        )
        if f'"{wrapper[2:]}"' not in rust_source:
            raise AssertionError("SDK/Rust offline EVM wrapper vector mismatch")
        print(f"SDK/Rust complete unsigned Ethereum.transact parity: {mode}")

        pnl = builder(
            evm_rpc_url="http://offline.invalid",
            private_key=TEST_KEY,
            precompile_address="0x" + "00" * 18 + "044e",
            data_hex="0x" + pnl_calldata.hex(),
            value=0,
            chain_id=0x12EE,
            gas_limit=500_000,
            use_legacy=use_legacy,
            nonce_ms=1_725_000_000_140,
        )
        pnl_wrapper = wrapper_builder(
            substrate_ws="ws://offline.invalid",
            signed_tx_hex=pnl.signed_tx,
            signer=pnl.signer,
        )
        wrapper_hash = hashlib.blake2b(bytes.fromhex(pnl_wrapper[2:]), digest_size=32)
        for value in (pnl.tx_hash[2:], wrapper_hash.hexdigest()):
            if f'"{value}"' not in rust_source:
                raise AssertionError("SDK/Rust Perp precompile vector mismatch")
        print(f"SDK/Rust Perp profit/loss-point precompile hashes: {mode}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sdk_source", type=Path)
    parser.add_argument("compat_source", type=Path)
    args = parser.parse_args()
    builder = load_sdk_builder(args.sdk_source, args.compat_source)
    source = (Path(__file__).resolve().parents[1] / "src/signing/mod.rs").read_text()
    vectors = [
        ("Subaccount", "no_op", {}, 1_725_000_000_124),
        (
            "Subaccount",
            "rename_subaccount",
            {
                "subaccount": "0x" + "11" * 20,
                "new_name": "0x" + b"Desk B".hex(),
            },
            1_725_000_000_142,
        ),
        (
            "Subaccount",
            "set_spot_margin",
            {
                "address": "0x" + "11" * 20,
                "enable_spot_margin": True,
            },
            1_725_000_000_138,
        ),
        (
            "Lending",
            "buy_quota",
            {
                "address": "0x" + "11" * 20,
                "quota": 100,
                "from_subaccount": None,
            },
            1_725_000_000_150,
        ),
        (
            "Lending",
            "buy_quota",
            {
                "address": "0x" + "11" * 20,
                "quota": 100,
                "from_subaccount": "0x" + "22" * 20,
            },
            1_725_000_000_150,
        ),
        (
            "System",
            "remark",
            {"remark": "0x" + b"deepx-offline-signing-check".hex()},
            1_725_000_000_123,
        ),
        (
            "PerpMarket",
            "place_order",
            {
                "params": {
                    "subaccount": "0x" + "11" * 20,
                    "market_id": 7,
                    "is_long": True,
                    "size": 2**128 - 1,
                    "price": 2**128 - 1,
                    "order_type": {"Limit": "GTC"},
                    "take_profit": 2**128 - 1,
                    "stop_loss": None,
                    "reduce_only": False,
                    "post_only": "None",
                }
            },
            1_725_000_000_125,
        ),
        (
            "PerpMarket",
            "place_order",
            {
                "params": {
                    "subaccount": "0x" + "11" * 20,
                    "market_id": 7,
                    "is_long": True,
                    "size": 1_000_000_000_000_000,
                    "price": 2_499_050_000,
                    "order_type": {"Limit": "IOC"},
                    "take_profit": None,
                    "stop_loss": None,
                    "reduce_only": False,
                    "post_only": "None",
                }
            },
            1_725_000_000_125,
        ),
        (
            "PerpMarket",
            "place_order",
            {
                "params": {
                    "subaccount": "0x" + "11" * 20,
                    "market_id": 7,
                    "is_long": True,
                    "size": 1_000_000_000_000_000,
                    "price": 0,
                    "order_type": {"Market": None},
                    "take_profit": None,
                    "stop_loss": None,
                    "reduce_only": False,
                    "post_only": "None",
                }
            },
            1_725_000_000_125,
        ),
        (
            "PerpMarket",
            "place_order",
            {
                "params": {
                    "subaccount": "0x" + "11" * 20,
                    "market_id": 7,
                    "is_long": False,
                    "size": 1_000_000_000_000_000,
                    "price": 2_499_050_000,
                    "order_type": {"Limit": "GTC"},
                    "take_profit": None,
                    "stop_loss": None,
                    "reduce_only": True,
                    "post_only": "MustPostOnly",
                }
            },
            1_725_000_000_125,
        ),
        (
            "PerpMarket",
            "cancel_order",
            {
                "params": {
                    "subaccount": "0x" + "11" * 20,
                    "order_id": 1_725_000_000_001,
                    "market_id": 7,
                    "cancel_reason": "UserCanceled",
                    "fast_cancel": False,
                }
            },
            1_725_000_000_125,
        ),
        (
            "PerpMarket",
            "cancel_order",
            {
                "params": {
                    "subaccount": "0x" + "11" * 20,
                    "order_id": 1_725_000_000_001,
                    "market_id": 7,
                    "cancel_reason": "UserCanceled",
                    "fast_cancel": True,
                }
            },
            1_725_000_000_125,
        ),
        (
            "PerpMarket",
            "close_position",
            {
                "subaccount": "0x" + "11" * 20,
                "market_id": 7,
                "price": 123,
                "slippage": None,
            },
            1_725_000_000_127,
        ),
        (
            "PerpMarket",
            "close_position",
            {
                "subaccount": "0x" + "11" * 20,
                "market_id": 7,
                "price": 123,
                "slippage": 50,
            },
            1_725_000_000_143,
        ),
        (
            "PerpMarket",
            "set_profit_and_loss_point",
            {
                "subaccount": "0x" + "11" * 20,
                "market_id": 7,
                "take_profit_point": 123,
                "stop_loss_point": 456,
            },
            1_725_000_000_130,
        ),
        (
            "SpotMarket",
            "cancel_order",
            {
                "params": {
                    "subaccount": "0x" + "11" * 20,
                    "pair": "0x" + "22" * 32,
                    "order_id": 1_725_000_000_128,
                    "is_buy": False,
                    "cancel_reason": "UserCanceled",
                    "fast_cancel": False,
                }
            },
            1_725_000_000_129,
        ),
        (
            "Subaccount",
            "modify_orders",
            {
                "ops": [
                    {
                        "Cancel": {
                            "Perp": {
                                "subaccount": "0x" + "11" * 20,
                                "order_id": 123,
                                "market_id": 7,
                                "cancel_reason": "UserCanceled",
                                "fast_cancel": False,
                            }
                        }
                    },
                    {
                        "Place": {
                            "Perp": {
                                "subaccount": "0x" + "11" * 20,
                                "market_id": 7,
                                "is_long": True,
                                "size": 1_000_000,
                                "price": 2_500_000_000,
                                "order_type": {"Limit": "GTC"},
                                "take_profit": None,
                                "stop_loss": None,
                                "reduce_only": False,
                                "post_only": "None",
                            }
                        }
                    },
                ]
            },
            456,
        ),
        (
            "Subaccount",
            "remove_delegate_account",
            {"delegate": "0x" + "22" * 20},
            1_725_000_000_132,
        ),
        (
            "Subaccount",
            "set_delegate_account",
            {
                "delegate": "0x" + "22" * 20,
                "name": "0x" + b"Desk A".hex(),
                "valid_until": 1_900_000_000_000,
            },
            1_725_000_000_133,
        ),
        (
            "Subaccount",
            "update_delegate_mode",
            {
                "address": "0x" + "22" * 20,
                "new_mode": "PlaceOrCancelOrder",
            },
            1_725_000_000_138,
        ),
        (
            "Subaccount",
            "update_delegate_mode",
            {"address": "0x" + "22" * 20, "new_mode": "Disable"},
            1_725_000_000_139,
        ),
    ]
    if args.spec_version in (369, 371):
        vectors.extend(
            [
                (
                    "SpotMarket",
                    "place_order",
                    {
                        "params": {
                            "subaccount": "0x" + "11" * 20,
                            "pair": "0x" + "22" * 32,
                            "is_buy": True,
                            "quote_amount": int.from_bytes(bytes([0x33]) * 32, "little"),
                            "base_amount": int.from_bytes(bytes([0x44]) * 32, "little"),
                            "order_type": {"Limit": "IOC"},
                            "post_only": "None",
                            "reduce_only": False,
                        }
                    },
                    1_725_000_000_133,
                ),
                (
                    "SpotMarket",
                    "place_order",
                    {
                        "params": {
                            "subaccount": "0x" + "11" * 20,
                            "pair": "0x" + "22" * 32,
                            "is_buy": True,
                            "quote_amount": int.from_bytes(bytes([0x33]) * 32, "little"),
                            "base_amount": int.from_bytes(bytes([0x44]) * 32, "little"),
                            "order_type": {"Market": None},
                            "post_only": "None",
                            "reduce_only": False,
                        }
                    },
                    1_725_000_000_135,
                ),
            ]
        )
    if args.spec_version == 371:
        vectors.extend(
            [
                (
                    "SpotMarket",
                    "cancel_order",
                    {
                        "params": {
                            "subaccount": "0x" + "11" * 20,
                            "pair": "0x" + "22" * 32,
                            "order_id": 1_725_000_000_128,
                            "is_buy": False,
                            "cancel_reason": "UserCanceled",
                            "fast_cancel": True,
                        }
                    },
                    1_725_000_000_129,
                ),
                (
                    "SpotMarket",
                    "place_order",
                    {
                        "params": {
                            "subaccount": "0x" + "11" * 20,
                            "pair": "0x" + "22" * 32,
                            "is_buy": True,
                            "quote_amount": int.from_bytes(bytes([0x33]) * 32, "little"),
                            "base_amount": int.from_bytes(bytes([0x44]) * 32, "little"),
                            "order_type": {"Limit": "GTC"},
                            "post_only": "None",
                            "reduce_only": True,
                        }
                    },
                    1_725_000_000_126,
                ),
                (
                    "SpotMarket",
                    "place_order",
                    {
                        "params": {
                            "subaccount": "0x" + "11" * 20,
                            "pair": "0x" + "22" * 32,
                            "is_buy": False,
                            "quote_amount": int.from_bytes(bytes([0x33]) * 32, "little"),
                            "base_amount": int.from_bytes(bytes([0x44]) * 32, "little"),
                            "order_type": {"Limit": "IOC"},
                            "post_only": "None",
                            "reduce_only": False,
                        }
                    },
                    1_725_000_000_134,
                ),
                (
                    "SpotMarket",
                    "place_order",
                    {
                        "params": {
                            "subaccount": "0x" + "11" * 20,
                            "pair": "0x" + "22" * 32,
                            "is_buy": False,
                            "quote_amount": int.from_bytes(bytes([0x33]) * 32, "little"),
                            "base_amount": int.from_bytes(bytes([0x44]) * 32, "little"),
                            "order_type": {"Market": 50},
                            "post_only": "None",
                            "reduce_only": False,
                        }
                    },
                    1_725_000_000_136,
                ),
                (
                    "Subaccount",
                    "initialize_subaccount",
                    {"name": "0x" + b"Desk A".hex()},
                    1_725_000_000_137,
                ),
                (
                    "Subaccount",
                    "delete_subaccount",
                    {"subaccount": "0x" + "11" * 20},
                    1_725_000_000_131,
                ),
                (
                    "PerpMarket",
                    "set_global_leverage",
                    {"subaccount": "0x" + "11" * 20, "max_leverage": None},
                    1_725_000_000_140,
                ),
                (
                    "PerpMarket",
                    "set_per_market_leverage",
                    {
                        "subaccount": "0x" + "11" * 20,
                        "market_id": 3,
                        "max_leverage": 20_000,
                    },
                    1_725_000_000_141,
                ),
            ]
        )
    if args.spec_version in (375, 376):
        selected = []
        seen = set()
        for vector in vectors:
            identity = (vector[0], vector[1])
            if (
                identity
                in {
                    ("Subaccount", "no_op"),
                    ("System", "remark"),
                    ("PerpMarket", "place_order"),
                }
                and identity not in seen
            ):
                selected.append(vector)
                seen.add(identity)
        vectors = selected
    for pallet, call, params, nonce in vectors:
        encoded = builder(
            substrate_ws="ws://offline.invalid",
            private_key=TEST_KEY,
            call_module=pallet,
            call_function=call,
            call_params=params,
            nonce_ms=nonce,
        )
        expected_literal = f'"{encoded.removeprefix("0x")}"'
        if expected_literal not in source:
            raise AssertionError(f"SDK/Rust signed extrinsic mismatch: {pallet}.{call}: {encoded}")
        print(f"SDK/Rust complete extrinsic parity: {pallet}.{call}")
    if args.spec_version == 376:
        verify_spec376_finalized_signature()
    verify_evm_vectors(args.sdk_source)
    print(f"SDK blobs: {SDK_BLOB}, {COMPAT_BLOB}; spec{args.spec_version}/tx1; offline only")


if __name__ == "__main__":
    main()
