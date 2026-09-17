"""Verify pinned DeepX SDK signing against captured runtime and Rust regression bytes.

No RPC, submission, or user credentials are used. The source input is the GitHub
contents API response for src/deepx_sdk/_native_py.py at the pinned blob below.
"""

import argparse
import ast
import base64
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from substrateinterface import Keypair, KeypairType, SubstrateInterface

SDK_BLOB = "cc85676dee70db35bbd996b560938597c3715558"
TEST_KEY = "0123456789abcdef" * 4
FIXTURES = Path(__file__).resolve().parents[1] / "test_data/runtime/testnet"
SNAPSHOT = FIXTURES / "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1"


class OfflineSubstrate(SubstrateInterface):
    def __init__(self):
        self.fixture_metadata = json.loads((SNAPSHOT / "metadata.json").read_text())
        self.fixture_version = json.loads(
            (SNAPSHOT / "runtime_version.json").read_text()
        )
        self.fixture_genesis = json.loads((SNAPSHOT / "genesis_hash.json").read_text())
        identity = json.loads((SNAPSHOT / "manifest.json").read_text())["identity"]
        metadata = bytes.fromhex(self.fixture_metadata["result"].removeprefix("0x"))
        expected_hash = {
            366: "e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37",
            369: "98136fdbab99332fa40828119c9d53a71a219f3e23155844cc0230cc663cba3c",
        }.get(self.fixture_version["result"]["specVersion"])
        if (
            hashlib.sha256(metadata).hexdigest() != expected_hash
            or identity["metadata_sha256"] != expected_hash
            or identity["genesis_hash"] != self.fixture_genesis["result"]
            or self.fixture_genesis["result"]
            != "0x86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b"
            or self.fixture_version["result"]["transactionVersion"] != 1
        ):
            raise ValueError("Runtime fixture identity is not the reviewed snapshot")
        super().__init__(
            url="http://offline.invalid", auto_discover=False, ss58_format=42
        )

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


def load_sdk_builder(source_response: Path):
    response = json.loads(source_response.read_text())
    source = base64.b64decode(response["content"])
    blob = hashlib.sha1(
        b"blob " + str(len(source)).encode() + b"\0" + source
    ).hexdigest()
    if response["sha"] != SDK_BLOB or blob != SDK_BLOB:
        raise ValueError("SDK source is not the reviewed pinned GitHub blob")
    # Execute only the exact upstream builder and key constructor, not SDK imports
    # or network helpers. The only substituted boundary is offline RPC transport.
    selected = {"build_signed_pallet_call_extrinsic", "_create_ecdsa_keypair"}
    tree = ast.parse(source)
    functions = [
        item
        for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name in selected
    ]
    if {item.name for item in functions} != selected:
        raise ValueError("Pinned SDK functions are missing")
    namespace = {
        "Any": Any,
        "Optional": Optional,
        "_get_substrate_interface_cls": lambda: OfflineSubstrate,
        "_get_substrate_keypair_libs": lambda: (Keypair, KeypairType),
        "_create_substrate": lambda cls, endpoint: cls(),
    }
    exec(  # noqa: S102 - exact reviewed blob and two selected functions only
        compile(
            ast.Module(body=functions, type_ignores=[]), str(source_response), "exec"
        ),
        namespace,
    )
    return namespace["build_signed_pallet_call_extrinsic"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sdk_contents_response", type=Path)
    parser.add_argument("--spec-version", type=int, choices=(366, 369), default=366)
    args = parser.parse_args()
    global SNAPSHOT
    if args.spec_version == 369:
        SNAPSHOT = (
            FIXTURES
            / "genesis-86604388_metadata-98136fdb_spec-369_tx-1_finalized-95febbff"
        )
    builder = load_sdk_builder(args.sdk_contents_response)
    source = (Path(__file__).resolve().parents[1] / "src/signing/mod.rs").read_text()
    vectors = [
        ("Subaccount", "no_op", {}, 1_725_000_000_124),
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
    ]
    for pallet, call, params, nonce in vectors:
        encoded = builder(
            substrate_ws="ws://offline.invalid",
            private_key=TEST_KEY,
            call_module=pallet,
            call_function=call,
            call_params=params,
            nonce_ms=nonce,
        )
        if encoded.removeprefix("0x") not in source:
            raise AssertionError(
                f"SDK/Rust signed extrinsic mismatch: {pallet}.{call}: {encoded}"
            )
        print(f"SDK/Rust complete extrinsic parity: {pallet}.{call}")
    print(f"SDK blob: {SDK_BLOB}; spec{args.spec_version}/tx1; offline only")


if __name__ == "__main__":
    main()
