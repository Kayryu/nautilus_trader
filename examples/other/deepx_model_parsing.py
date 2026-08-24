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

from nautilus_trader.adapters.deepx import (
    DeepXOrderBookSnapshot,
    DeepXOrderBookUpdate,
    DeepXPerpetualMarket,
    DeepXTrade,
)

market = DeepXPerpetualMarket.from_json(
    """{
        "baseAsset":"ETH","makerFeeRate":"-0.0001","marketId":3,
        "maxOpenOrders":128,"minNotional":"1","minQty":"0.001",
        "orderTypes":["LIMIT","MARKET"],"quoteAsset":"USDC",
        "status":"TRADING","stepSize":"0.001","symbol":"ETH-USDC",
        "takerFeeRate":"0.0002","tickSize":"0.01"
    }""",
)

book = DeepXOrderBookSnapshot.from_json(
    """{
        "asks":[["2453.76","3.282"]],"bids":[["2453.75","0.842"]],
        "engineTime":1787561622066,"lastUpdateId":62430652,
        "serverTime":1787561622211
    }""",
)

snapshot = DeepXOrderBookUpdate.from_json(
    """{"channel":"perp@orderbook","data":{
        "asks":[["2457.02","0.859"]],"bids":[["2456.81","0.291"]],
        "engineTime":1787562160783,"lastUpdateId":62493922,
        "prevLastUpdateId":null,"serverTime":1787562160833,
        "symbol":"ETH-USDC","updateType":"snapshot"}}""",
)
delta = DeepXOrderBookUpdate.from_json(
    """{"channel":"perp@orderbook","data":{
        "asks":[["2457.02","0"]],"bids":[],
        "engineTime":1787562162051,"lastUpdateId":62494052,
        "prevLastUpdateId":62493922,"serverTime":1787562162052,
        "symbol":"ETH-USDC","updateType":"delta"}}""",
)

trade = DeepXTrade.from_json(
    """{"channel":"perp@trades","data":{
        "buyOrderId":"6652970","id":"159078803000024","makerFee":"-0.02529",
        "marketId":3,"price":"2455.7","qty":"0.103","quoteQty":"252.9371",
        "sellOrderId":"2263022","symbol":"ETH-USDC","takerFee":"0.050581",
        "takerSide":"SELL","time":1787562119833}}""",
)

assert snapshot.follows(None)
assert delta.follows(snapshot.last_update_id)

print(f"Market: {market.symbol} tick_size={market.tick_size}")
print(f"Book: update_id={book.last_update_id} best_bid={book.bids[0]}")
print(f"Delta: update_id={delta.last_update_id} asks={delta.asks}")
print(f"Trade: id={trade.id} side={trade.taker_side} price={trade.price} qty={trade.qty}")