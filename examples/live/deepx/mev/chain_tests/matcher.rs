// Included as a child of the chain pallet's existing tests module by run_matcher.py.
// This exercises production matching and settlement, without RPC event fixtures.
use super::*;

fn place(owner: AccountId20, buy: bool, base: u128, price: u128, ioc: bool) {
    assert_ok!(SpotMarketMock::place_order(
        signed_oid(owner),
        SpotPlaceParams {
            stp_mode: None,
            subaccount: H160(owner.0),
            pair: EXPECT_PAIR,
            is_buy: buy,
            quote_amount: quote_for_limit_base_amount(base, price),
            base_amount: base.into(),
            order_type: if ioc {
                OrderType::limit_ioc()
            } else {
                OrderType::limit_gtc()
            },
            post_only: PostOnlyParam::None,
            reduce_only: false,
        },
    ));
}

fn match_orders() {
    assert_ok!(SpotMarketMock::match_spot_pair_until_idle(
        EXPECT_PAIR,
        None,
        None
    ));
}

fn setup() {
    setup_spot_market_alice_bob();
    let account = new_subaccount(CHARLIE);
    SubaccountInfo::<Test>::insert(account.address, account);
    // new_subaccount seeds positions; account for the additional deposits in pools.
    bump_pool_deposit(&SYMBOL_B.to_vec().try_into().unwrap(), 10u128.pow(18));
    bump_pool_deposit(&SYMBOL_U.to_vec().try_into().unwrap(), 10_000_000);
    for owner in [ALICE, BOB, CHARLIE] {
        SubaccountInfo::<Test>::mutate(H160(owner.0), |info| {
            info.as_mut().unwrap().spot_margin_trading_enabled = false;
        });
    }
    SpotMarkets::<Test>::mutate(EXPECT_PAIR, |market| {
        let market = market.as_mut().unwrap();
        market.taker_fee_rate = 10_000; // 1%, charged in the received asset.
        market.maker_fee_rate = 0;
    });
    set_spot_oracle_index(TEST_LIMIT_PRICE);
    place(BOB, false, TOY_LIMIT_BASE, TEST_LIMIT_PRICE, false);
    place(BOB, false, TOY_LIMIT_BASE, 51_000_000, false);
    match_orders(); // Asks are already resting before target/entry placement.
    SystemPallet::<Test>::set_block_number(2);
    SystemPallet::<Test>::reset_events();
}

fn trades() -> Vec<(H160, H160, U256, U256)> {
    SystemPallet::<Test>::events()
        .into_iter()
        .filter_map(|record| match record.event {
            RuntimeEvent::SpotMarket(exchange_event::OrderMatched {
                buyer,
                seller,
                trade_amount_b,
                trade_amount_u,
                ..
            }) => Some((buyer, seller, trade_amount_b, trade_amount_u)),
            _ => None,
        })
        .collect()
}

#[test]
fn same_batch_ioc_entry_precedes_gtc_target_and_exits_net_inventory() {
    new_test_ext().execute_with(|| {
        setup();
        let base_before = user_spot_balance(H160(ALICE.0), 1);
        let quote_before = user_spot_balance(H160(ALICE.0), 2);
        // Place target first: matching phase, rather than call order, must favor IOC.
        place(CHARLIE, true, 3 * TOY_LIMIT_BASE, 52_000_000, false);
        place(ALICE, true, TOY_LIMIT_BASE, TEST_LIMIT_PRICE, true);
        match_orders();

        let fills = trades();
        assert_eq!(fills.len(), 2);
        assert_eq!(
            fills[0],
            (
                H160(ALICE.0),
                H160(BOB.0),
                TOY_LIMIT_BASE.into(),
                500_000u128.into()
            )
        );
        assert_eq!(
            fills[1],
            (
                H160(CHARLIE.0),
                H160(BOB.0),
                TOY_LIMIT_BASE.into(),
                510_000u128.into()
            )
        );
        let net_base = TOY_LIMIT_BASE * 99 / 100;
        assert_eq!(user_spot_balance(H160(ALICE.0), 1) - base_before, net_base);
        assert_eq!(quote_before - user_spot_balance(H160(ALICE.0), 2), 500_000);
        assert_eq!(buy_ioc_len(EXPECT_PAIR), 0);

        SystemPallet::<Test>::set_block_number(3);
        place(ALICE, false, net_base, 52_000_000, true);
        match_orders();
        let fills = trades();
        assert_eq!(fills.len(), 3);
        assert_eq!(
            fills[2],
            (
                H160(CHARLIE.0),
                H160(ALICE.0),
                net_base.into(),
                514_800u128.into()
            )
        );
        assert_eq!(user_spot_balance(H160(ALICE.0), 1), base_before);
        assert_eq!(user_spot_balance(H160(ALICE.0), 2) - quote_before, 9_652);
        assert_eq!(sell_ioc_len(EXPECT_PAIR), 0);
        assert!(SpotMarketMock::order_info(&H160(ALICE.0), &1u64).is_none());
        assert!(SpotMarketMock::order_info(&H160(ALICE.0), &2u64).is_none());
    });
}

#[test]
fn target_matched_in_earlier_batch_leaves_entry_unfilled() {
    new_test_ext().execute_with(|| {
        setup();
        place(CHARLIE, true, 3 * TOY_LIMIT_BASE, 52_000_000, false);
        match_orders();
        assert_eq!(trades().len(), 2);
        SystemPallet::<Test>::reset_events();
        let base_before = user_spot_balance(H160(ALICE.0), 1);
        let quote_before = user_spot_balance(H160(ALICE.0), 2);
        place(ALICE, true, TOY_LIMIT_BASE, TEST_LIMIT_PRICE, true);
        match_orders();
        assert!(trades().is_empty());
        assert_eq!(user_spot_balance(H160(ALICE.0), 1), base_before);
        assert_eq!(user_spot_balance(H160(ALICE.0), 2), quote_before);
        assert_eq!(buy_ioc_len(EXPECT_PAIR), 0);
        assert!(SpotMarketMock::order_info(&H160(ALICE.0), &1u64).is_none());
    });
}
