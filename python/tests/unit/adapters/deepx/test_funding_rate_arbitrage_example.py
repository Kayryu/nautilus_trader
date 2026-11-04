# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""Test the DeepX funding example's order sequencing and exposure accounting."""

import json
from decimal import Decimal
from pathlib import Path
from types import MethodType
from types import SimpleNamespace

import pytest
from examples.live.deepx import funding_rate_arbitrage as arb
from examples.live.deepx.funding_rate_arbitrage import MAX_CYCLE_ORDERS
from examples.live.deepx.funding_rate_arbitrage import MAX_NO_FILL_ORDERS
from examples.live.deepx.funding_rate_arbitrage import PERP_ID
from examples.live.deepx.funding_rate_arbitrage import SPOT_ID
from examples.live.deepx.funding_rate_arbitrage import FundingArbitrage
from examples.live.deepx.funding_rate_arbitrage import select_subaccount


def number(value: str) -> SimpleNamespace:
    """Represent a domain price or quantity with an exact decimal value."""
    return SimpleNamespace(as_decimal=lambda: Decimal(value))


def make_strategy() -> tuple[SimpleNamespace, list[tuple[str, Decimal]]]:
    """Build a strategy state with captured order intents."""
    orders: list[tuple[str, Decimal]] = []
    quote = SimpleNamespace(
        ask_price=number("2250.00"),
        bid_price=number("2249.75"),
        ask_size=number("1"),
        bid_size=number("1"),
    )
    strategy = SimpleNamespace(
        _config=SimpleNamespace(
            quantity=Decimal("0.001"),
            min_funding_rate=Decimal(0),
            max_entry_basis_bps=Decimal(5),
            max_combined_notional=Decimal(5),
            max_age_ns=10_000_000_000,
            max_hold_ns=60_000_000_000,
            max_slippage_bps=Decimal(0),
            holding_cost_bps=Decimal(0),
            exit_basis_risk_bps=Decimal(0),
            trade_enabled=True,
        ),
        _funding=SimpleNamespace(rate=Decimal("0.01"), ts_event=100_000_000_000),
        _mark=SimpleNamespace(value=number("2250"), ts_event=100_000_000_000),
        _quantity=object(),
        _spot_qty=Decimal(0),
        _perp_short_qty=Decimal(0),
        _pending=None,
        _pending_filled=Decimal(0),
        _no_fill_orders=0,
        _orders_submitted=0,
        _entry_started=False,
        _hedge_attempted=False,
        _exiting=False,
        _finished=False,
        _hold_start_ns=None,
        _last_attempt_ns=None,
        _state=arb.CycleState.FLAT,
        _persist=lambda: None,
        _depth_available=lambda *_: Decimal(1),
        clock=SimpleNamespace(
            timestamp_ns=lambda: 100_000_000_000,
            cancel_timer=lambda _: None,
            set_time_alert_ns=lambda *_, **__: None,
        ),
        log=SimpleNamespace(info=lambda _: None, warning=lambda _: None, error=lambda _: None),
        cache=SimpleNamespace(
            instrument=lambda _: SimpleNamespace(
                min_quantity=number("0.001"),
                min_notional=number("1"),
                size_increment=number("0.001"),
                multiplier=number("1"),
                taker_fee=Decimal(0),
                price_increment=number("0.01"),
                price_precision=2,
                size_precision=3,
                base_currency=object(),
            )
        ),
        _fresh=lambda instrument_id: quote if instrument_id in (SPOT_ID, PERP_ID) else None,
        _submit=lambda role, amount, _: orders.append((role, amount)),
        _stop_cycle=lambda: None,
        stop=lambda: None,
    )
    strategy._advance = MethodType(FundingArbitrage._advance, strategy)
    strategy._terminal = MethodType(FundingArbitrage._terminal, strategy)
    return strategy, orders


def test_spot_fill_is_hedged_at_its_actual_quantity() -> None:
    """A partial spot buy caps the subsequent short."""
    strategy, orders = make_strategy()
    strategy._config.quantity = Decimal("0.002")
    strategy._config.max_combined_notional = Decimal(10)
    strategy._advance()
    assert orders == [("spot_entry", Decimal("0.002"))]

    orders.clear()
    strategy._spot_qty = Decimal("0.001")
    strategy._advance()
    assert orders == [("perp_entry", Decimal("0.001"))]


def test_subminimum_spot_fill_waits_for_terminal_then_attempts_unwind() -> None:
    """A spot fill smaller than the perpetual minimum must not open a short."""
    strategy, orders = make_strategy()
    strategy._entry_started = True
    strategy._pending = ("spot_entry", "order-1")
    strategy.cache.order = lambda _: SimpleNamespace(is_closed=False)
    fill = SimpleNamespace(client_order_id="order-1", last_qty=number("0.0007"))

    FundingArbitrage.on_order_filled(strategy, fill)
    assert strategy._spot_qty == Decimal("0.0007")
    assert strategy._pending == ("spot_entry", "order-1")
    assert orders == []

    FundingArbitrage.on_order_canceled(strategy, SimpleNamespace(client_order_id="order-1"))
    assert strategy._pending is None
    assert orders == [("spot_exit", Decimal("0.0007"))]


def test_unwind_below_spot_minimum_stops_with_residual_exposure() -> None:
    """A venue-untradeable spot remainder is surfaced instead of rounded up."""
    strategy, _ = make_strategy()
    strategy._spot_qty = Decimal("0.0007")
    strategy.cache.instrument = lambda _: SimpleNamespace(
        size_precision=3,
        size_increment=number("0.001"),
        min_quantity=number("0.001"),
        min_notional=None,
        price_increment=number("0.01"),
        price_precision=2,
        base_currency=object(),
    )
    strategy.portfolio = SimpleNamespace(
        account=lambda _: SimpleNamespace(balance_free=lambda _: number("0.0007"))
    )

    FundingArbitrage._submit(strategy, "spot_exit", Decimal("0.0007"), strategy._fresh(SPOT_ID))
    assert strategy._finished
    assert strategy._spot_qty == Decimal("0.0007")
    assert strategy._orders_submitted == 0


def test_repeated_empty_exit_orders_stop_automatic_retries() -> None:
    """A persistent failed unwind requires manual recovery rather than unlimited orders."""
    strategy, orders = make_strategy()
    strategy._entry_started = True
    strategy._exiting = True
    strategy._spot_qty = Decimal("0.001")
    strategy._perp_short_qty = Decimal("0.001")

    for attempt in range(MAX_NO_FILL_ORDERS):
        strategy._pending = ("perp_exit", f"order-{attempt}")
        FundingArbitrage.on_order_canceled(
            strategy, SimpleNamespace(client_order_id=f"order-{attempt}")
        )

    assert strategy._finished
    assert strategy._no_fill_orders == MAX_NO_FILL_ORDERS
    assert strategy._spot_qty == strategy._perp_short_qty == Decimal("0.001")
    assert orders == [("perp_exit", Decimal("0.001"))] * (MAX_NO_FILL_ORDERS - 1)


def test_total_order_limit_stops_partial_fill_retry_loop() -> None:
    """Partial fills cannot cause an unbounded number of new orders."""
    strategy, _ = make_strategy()
    strategy._orders_submitted = MAX_CYCLE_ORDERS
    strategy._spot_qty = Decimal("0.0004")
    strategy._exiting = True
    strategy._submit = MethodType(FundingArbitrage._submit, strategy)

    strategy._advance()
    assert strategy._finished
    assert strategy._orders_submitted == MAX_CYCLE_ORDERS


def test_cycle_stop_requests_live_node_shutdown() -> None:
    """Ending a cycle releases the blocking node run for post-run verification."""
    calls: list[str] = []
    strategy = SimpleNamespace(
        stop=lambda: calls.append("strategy"),
        _node_handle=SimpleNamespace(stop=lambda: calls.append("node")),
    )

    FundingArbitrage._stop_cycle(strategy)
    assert calls == ["strategy", "node"]


def test_partial_perp_hedge_unwinds_only_excess_spot() -> None:
    """A partial short leads to sale of the unmatched spot only."""
    strategy, orders = make_strategy()
    strategy._entry_started = True
    strategy._hedge_attempted = True
    strategy._spot_qty = Decimal("0.001")
    strategy._perp_short_qty = Decimal("0.0006")

    strategy._advance()
    assert orders == [("spot_exit", Decimal("0.0004"))]


def test_exit_closes_perp_before_selling_matching_spot() -> None:
    """A partial short close releases the same quantity of spot for sale."""
    strategy, orders = make_strategy()
    strategy._entry_started = True
    strategy._exiting = True
    strategy._spot_qty = Decimal("0.001")
    strategy._perp_short_qty = Decimal("0.001")

    strategy._advance()
    assert orders == [("perp_exit", Decimal("0.001"))]

    orders.clear()
    strategy._perp_short_qty = Decimal("0.0006")
    strategy._advance()
    assert orders == [("spot_exit", Decimal("0.0004"))]


def test_nonpositive_funding_starts_exit_after_hedge() -> None:
    """A zero funding sample starts the unwind of an established hedge."""
    strategy, orders = make_strategy()
    strategy._entry_started = True
    strategy._spot_qty = Decimal("0.001")
    strategy._perp_short_qty = Decimal("0.001")
    strategy._funding.rate = Decimal(0)

    strategy._advance()
    assert strategy._exiting
    assert orders == [("perp_exit", Decimal("0.001"))]


def test_stale_funding_sample_does_not_start_entry() -> None:
    """A positive historical sample cannot trigger a new position."""
    strategy, orders = make_strategy()
    strategy._funding.ts_event = 80_000_000_000

    strategy._advance()
    assert orders == []


def test_entry_respects_perpetual_minimum_notional() -> None:
    """Spot is never bought when the matching short would be below venue minimum."""
    strategy, orders = make_strategy()
    strategy.cache.instrument = lambda _: SimpleNamespace(
        size_increment=number("0.001"),
        min_quantity=number("0.001"),
        min_notional=number("3"),
        multiplier=number("1"),
    )

    strategy._advance()
    assert orders == []


@pytest.mark.parametrize(("borrowed", "eth"), [("1", "0"), ("0", "0.001")])
def test_account_preflight_rejects_unrelated_exposure(borrowed: str, eth: str) -> None:
    """Existing borrowing or ETH holdings prevent starting a new funding cycle."""
    account = SimpleNamespace(
        profile=SimpleNamespace(address="0xabc", status="Active"),
        margin_ratio=SimpleNamespace(exact_maintenance_margin_required="0", margin_required="0"),
        balances=SimpleNamespace(
            assets=[
                SimpleNamespace(symbol="USDC", balance="10", balance_borrowed=borrowed),
                SimpleNamespace(symbol="ETH", balance=eth, balance_borrowed="0"),
            ]
        ),
    )
    with pytest.raises(ValueError, match=r"borrowing|owns ETH"):
        select_subaccount("0xabc", [account], Decimal(5))


def test_post_run_audit_counts_only_settled_short_funding(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A flat account and settled short fees are reported separately."""
    subaccount = "0xabc"
    account = SimpleNamespace(
        profile=SimpleNamespace(address=subaccount),
        balances=SimpleNamespace(assets=[SimpleNamespace(symbol="ETH", balance="0")]),
        margin_ratio=SimpleNamespace(exact_maintenance_margin_required="0", margin_required="0"),
    )
    snapshot = SimpleNamespace(
        directory=SimpleNamespace(addresses=[subaccount]), subaccounts=[account]
    )
    empty_orders = SimpleNamespace(items=[], has_next=False)
    fees = SimpleNamespace(
        items=[
            SimpleNamespace(fee="0.00017", is_settled=True, is_long=False),
            SimpleNamespace(fee="-0.00017", is_settled=True, is_long=True),
            SimpleNamespace(fee="0.00003", is_settled=False, is_long=False),
        ]
    )
    monkeypatch.setattr(arb, "get_deepx_wallet_account_snapshot", lambda _: snapshot)
    monkeypatch.setattr(arb, "get_deepx_spot_open_orders", lambda *_, **__: empty_orders)
    monkeypatch.setattr(arb, "get_deepx_perp_open_orders", lambda *_, **__: empty_orders)
    monkeypatch.setattr(arb, "get_deepx_perp_positions", lambda *_, **__: empty_orders)
    monkeypatch.setattr(arb, "get_deepx_perp_funding_fee_pages", lambda *_, **__: [fees])

    assert arb.verify_trade_result(subaccount, 1) == Decimal("0.00017")
    assert "settled short funding records=1" in capsys.readouterr().out


def test_post_run_audit_rejects_residual_spot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The audit cannot report success while ETH remains in the account."""
    account = SimpleNamespace(
        profile=SimpleNamespace(address="0xabc"),
        balances=SimpleNamespace(assets=[SimpleNamespace(symbol="ETH", balance="0.001")]),
        margin_ratio=SimpleNamespace(exact_maintenance_margin_required="0", margin_required="0"),
    )
    snapshot = SimpleNamespace(
        directory=SimpleNamespace(addresses=["0xabc"]), subaccounts=[account]
    )
    empty_orders = SimpleNamespace(items=[], has_next=False)
    monkeypatch.setattr(arb, "get_deepx_wallet_account_snapshot", lambda _: snapshot)
    monkeypatch.setattr(arb, "get_deepx_spot_open_orders", lambda *_, **__: empty_orders)
    monkeypatch.setattr(arb, "get_deepx_perp_open_orders", lambda *_, **__: empty_orders)
    monkeypatch.setattr(arb, "get_deepx_perp_positions", lambda *_, **__: empty_orders)

    with pytest.raises(RuntimeError, match="not verified flat"):
        arb.verify_trade_result("0xabc", 1)


def test_post_run_audit_rejects_open_perpetual_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A zero margin observation alone cannot certify the perpetual is closed."""
    account = SimpleNamespace(
        profile=SimpleNamespace(address="0xabc"),
        balances=SimpleNamespace(assets=[SimpleNamespace(symbol="ETH", balance="0")]),
        margin_ratio=SimpleNamespace(exact_maintenance_margin_required="0", margin_required="0"),
    )
    snapshot = SimpleNamespace(
        directory=SimpleNamespace(addresses=["0xabc"]), subaccounts=[account]
    )
    empty_orders = SimpleNamespace(items=[], has_next=False)
    open_position = SimpleNamespace(items=[SimpleNamespace(status="Open")], has_next=False)
    monkeypatch.setattr(arb, "get_deepx_wallet_account_snapshot", lambda _: snapshot)
    monkeypatch.setattr(arb, "get_deepx_spot_open_orders", lambda *_, **__: empty_orders)
    monkeypatch.setattr(arb, "get_deepx_perp_open_orders", lambda *_, **__: empty_orders)
    monkeypatch.setattr(arb, "get_deepx_perp_positions", lambda *_, **__: open_position)

    with pytest.raises(RuntimeError, match="not verified flat"):
        arb.verify_trade_result("0xabc", 1)


def test_entry_edge_covers_round_trip_fees_slippage_and_holding_cost() -> None:
    """A small positive funding sample cannot clear bounded round-trip costs."""
    base = Decimal("0.001")
    assert (
        arb.entry_edge(
            base,
            Decimal("0.001"),
            Decimal(2000),
            Decimal(2000),
            Decimal(2000),
            Decimal("0.0004"),
            Decimal("0.0002"),
            Decimal(2),
            Decimal(1),
            Decimal(5),
        )
        < 0
    )
    assert (
        arb.entry_edge(
            base,
            Decimal("0.01"),
            Decimal(2000),
            Decimal(2000),
            Decimal(2000),
            Decimal("0.0004"),
            Decimal("0.0002"),
            Decimal(2),
            Decimal(1),
            Decimal(5),
        )
        > 0
    )


def test_partial_spot_fill_uses_contract_multiplier() -> None:
    """The short order quantity is contracts rather than spot base units."""
    strategy, orders = make_strategy()
    strategy._entry_started = True
    strategy._spot_qty = Decimal("0.002")
    instrument = strategy.cache.instrument(PERP_ID)
    instrument.multiplier = number("2")
    instrument.size_increment = number("0.001")
    strategy.cache.instrument = lambda _: instrument

    strategy._advance()
    assert orders == [("perp_entry", Decimal("0.001"))]


@pytest.mark.parametrize(
    ("journal_spot", "venue_spot", "expected"),
    [("0.001", "0.001", True), ("0.001", "0.002", False)],
)
def test_restart_reconciles_journal_with_venue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    journal_spot: str,
    venue_spot: str,
    expected: bool,
) -> None:
    """Recovery closes a matched prior pair and rejects inconsistent venue exposure."""
    strategy, _ = make_strategy()
    journal = tmp_path / "cycle.json"
    journal.write_text(
        json.dumps(
            {
                "version": 1,
                "subaccount": "0xabc",
                "state": "holding",
                "spot_base": journal_spot,
                "perp_contracts": "0.001",
            }
        )
    )
    strategy._config.journal_path = journal
    strategy._config.subaccount = "0xabc"
    strategy._recover = MethodType(FundingArbitrage._recover, strategy)
    strategy.cache.orders_open = lambda **_: []
    monkeypatch.setattr(
        arb,
        "get_deepx_wallet_account_snapshot",
        lambda _: SimpleNamespace(
            subaccounts=[
                SimpleNamespace(
                    profile=SimpleNamespace(address="0xabc", status="Active"),
                    balances=SimpleNamespace(
                        assets=[
                            SimpleNamespace(
                                symbol="ETH",
                                balance=venue_spot,
                                balance_borrowed="0",
                            )
                        ]
                    ),
                )
            ]
        ),
    )
    empty = SimpleNamespace(items=[], has_next=False)
    monkeypatch.setattr(arb, "get_deepx_spot_open_orders", lambda *_, **__: empty)
    monkeypatch.setattr(arb, "get_deepx_perp_open_orders", lambda *_, **__: empty)
    monkeypatch.setattr(
        arb,
        "get_deepx_perp_positions",
        lambda *_, **__: SimpleNamespace(
            items=[SimpleNamespace(status="Open", is_long=False, base_asset_amount="0.001")],
            has_next=False,
        ),
    )

    assert strategy._recover() is expected
    if expected:
        assert strategy._exiting
        assert strategy._spot_qty == strategy._perp_short_qty == Decimal("0.001")
        assert strategy._state == arb.CycleState.EXITING
    else:
        assert strategy._state == arb.CycleState.HALTED


def test_spot_ioc_and_perpetual_close_order_flags() -> None:
    """Spot uses a bounded limit and only the perpetual close requests reduce-only."""
    strategy, _ = make_strategy()
    submitted: list[dict] = []
    strategy._config.max_slippage_bps = Decimal(10)
    strategy.order_factory = SimpleNamespace(
        limit=lambda **kwargs: SimpleNamespace(
            client_order_id="order-1",
            args=kwargs,
        )
    )
    strategy.submit_order = lambda order: submitted.append(order.args)
    strategy.clock.set_time_alert_ns = lambda *_, **__: None
    strategy._submit = MethodType(FundingArbitrage._submit, strategy)
    quote = strategy._fresh(SPOT_ID)

    strategy._submit("spot_entry", Decimal("0.001"), quote)
    assert submitted[0]["time_in_force"] == arb.TimeInForce.IOC
    assert submitted[0]["price"].as_decimal() <= Decimal("2252.25")
    assert "reduce_only" not in submitted[0]

    strategy._pending = None
    strategy._submit("perp_exit", Decimal("0.001"), quote)
    assert submitted[1]["order_side"] == arb.OrderSide.BUY
    assert submitted[1]["reduce_only"] is True


def test_spot_exit_is_capped_by_reconciled_free_inventory() -> None:
    """A spot sale never exceeds the account's available base balance."""
    strategy, _ = make_strategy()
    submitted: list[dict] = []
    strategy.order_factory = SimpleNamespace(
        limit=lambda **kwargs: SimpleNamespace(client_order_id="order-1", args=kwargs)
    )
    strategy.submit_order = lambda order: submitted.append(order.args)
    strategy.clock.set_time_alert_ns = lambda *_, **__: None
    strategy.portfolio = SimpleNamespace(
        account=lambda _: SimpleNamespace(balance_free=lambda _: number("0.001"))
    )
    strategy._submit = MethodType(FundingArbitrage._submit, strategy)
    strategy._submit("spot_exit", Decimal("0.002"), strategy._fresh(SPOT_ID))

    assert submitted[0]["quantity"].as_decimal() == Decimal("0.001")
    assert "reduce_only" not in submitted[0]


def test_order_timeout_cancels_then_halts_without_confirmation() -> None:
    """Unconfirmed cancellation stops further order submissions."""
    strategy, _ = make_strategy()
    calls: list[str] = []
    order = SimpleNamespace(client_order_id="order-1", is_closed=False)
    strategy._pending = ("spot_entry", order.client_order_id)
    strategy.cache.order = lambda _: order
    strategy.cancel_order = lambda _: calls.append("cancel")
    strategy.clock.set_time_alert_ns = lambda *_, **__: calls.append("timer")
    strategy._order_timeout = MethodType(FundingArbitrage._order_timeout, strategy)
    strategy._cancel_timeout = MethodType(FundingArbitrage._cancel_timeout, strategy)
    strategy._stop_cycle = lambda: calls.append("stop")

    strategy._order_timeout(order.client_order_id)
    assert calls == ["cancel", "timer"]
    strategy._cancel_timeout(order.client_order_id)
    assert calls == ["cancel", "timer", "stop"]
    assert strategy._state == arb.CycleState.HALTED


def test_hold_timeout_starts_exit_without_a_new_market_event() -> None:
    """The maximum holding time independently advances the exit state."""
    strategy, orders = make_strategy()
    strategy._entry_started = True
    strategy._spot_qty = Decimal("0.001")
    strategy._perp_short_qty = Decimal("0.001")
    strategy._hold_start_ns = 1
    strategy._hold_timeout = MethodType(FundingArbitrage._hold_timeout, strategy)

    strategy._hold_timeout()
    assert strategy._state == arb.CycleState.EXITING
    assert orders == [("perp_exit", Decimal("0.001"))]
