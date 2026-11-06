#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License
#  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing permissions and limitations under
#  the License.
# -------------------------------------------------------------------------------------------------

"""Stage tests in a temporary node copy and run its production Spot matcher offline."""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prepare-only", action="store_true", help="Stage without invoking Cargo"
    )
    parser.add_argument(
        "--online",
        action="store_true",
        help="Allow Cargo to fetch missing dependencies",
    )
    parser.add_argument("--toolchain", help="Override the node's pinned Rust toolchain")
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    repository = here.parents[4]
    stage = Path(tempfile.mkdtemp(prefix="deepx-matcher-"))
    node = stage / "deepx-node"
    shutil.copytree(
        repository / "deepx-node",
        node,
        ignore=shutil.ignore_patterns(".git", "target", "log", "base", "node_modules"),
        symlinks=True,
    )
    fixture = node / "pallets/spot-market/src/mev_matcher_tests.rs"
    shutil.copyfile(here / "matcher.rs", fixture)
    tests = node / "pallets/spot-market/src/tests.rs"
    with tests.open("a") as output:
        output.write('\n#[path = "mev_matcher_tests.rs"]\nmod mev_matcher_tests;\n')
    command = [
        "cargo",
        "test",
        "--locked",
        "--manifest-path",
        str(node / "Cargo.toml"),
        "-p",
        "pallet-spot-market",
        "mev_matcher_tests::",
        "--target-dir",
        str(repository / "target"),
        "--",
        "--nocapture",
    ]
    if not args.online:
        command.insert(2, "--offline")
    if args.toolchain:
        command.insert(1, f"+{args.toolchain}")
    print(json.dumps({"stage": str(stage), "command": command}), flush=True)
    if args.prepare_only:
        return 0
    environment = os.environ.copy()
    environment.update(
        CARGO_INCREMENTAL="0",
        CARGO_PROFILE_DEV_DEBUG="0",
        CARGO_PROFILE_TEST_DEBUG="0",
    )
    # Retain the small source copy for inspection, including when compilation fails.
    return subprocess.run(command, cwd=node, env=environment, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
