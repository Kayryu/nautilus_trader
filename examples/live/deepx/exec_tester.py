#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Report the current DeepX execution capability boundary.

DeepX execution is intentionally unavailable until authoritative private account, order, fill,
and position schemas support bootstrap and reconciliation. This script does not connect or submit
orders.
"""

from __future__ import annotations


def main() -> None:
    """
    Exit before constructing a live trading node.
    """
    raise SystemExit(
        "DeepX execution is unavailable: authoritative account bootstrap and reconciliation "
        "schemas have not been integrated"
    )


if __name__ == "__main__":
    main()
