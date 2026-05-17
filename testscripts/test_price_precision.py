#!/usr/bin/env python3
"""
Probe Coinbase's price precision rules using the preview-order endpoint.

Coinbase's docs say to use `quote_increment` for price tick size, but in
practice we've still seen INVALID_PRICE_PRECISION for prices that match
quote_increment (e.g. 78077.7075 rejected). One hypothesis: Coinbase caps
the *total* digit count, not just the decimals after the point.

This script:
  1. Fetches the product spec (prints quote_increment, base_increment, etc.).
  2. Generates a battery of test prices that vary BOTH the decimal count and
     the total significant-digit count.
  3. Submits each as a BUY limit for 1 contract via /orders/preview, which
     validates without placing.
  4. Prints a PASS/FAIL table with the rejection reason so you can see
     exactly where the precision boundary is.

Example:
  python3 testscripts/test_price_precision.py --product BIP-20DEC30-CDE
  python3 testscripts/test_price_precision.py --product BIP-20DEC30-CDE --base 78000
"""

import argparse
import json
import os
import sys

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', 'server'))

from coinbase_http import CoinbaseHTTP  # noqa: E402

BASE = 'https://api.coinbase.com'


def auth_headers(cb, method, path):
    token = cb._make_jwt(method, path)
    return {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }


def fetch_product(cb, product_id):
    path = f'/api/v3/brokerage/products/{product_id}'
    resp = requests.get(BASE + path, headers=auth_headers(cb, 'GET', path))
    if not resp.ok:
        raise RuntimeError(f'get product {product_id} failed: HTTP {resp.status_code} {resp.text[:300]}')
    return resp.json()


def fetch_best_price(cb, product_id):
    """Use a preview market order to discover the live best bid/ask."""
    path = '/api/v3/brokerage/orders/preview'
    body = {
        'product_id': product_id,
        'side': 'BUY',
        'order_configuration': {'market_market_ioc': {'base_size': '1'}},
        'leverage': '3.0',
        'margin_type': 'CROSS',
    }
    resp = requests.post(BASE + path, headers=auth_headers(cb, 'POST', path), json=body)
    try:
        j = resp.json()
    except Exception:
        return None
    # Coinbase returns best_bid / best_ask as strings.
    bid = j.get('best_bid')
    ask = j.get('best_ask')
    return {'bid': bid, 'ask': ask, 'raw': j}


def preview_limit(cb, product_id, side, base_size, limit_price_str):
    path = '/api/v3/brokerage/orders/preview'
    body = {
        'product_id': product_id,
        'side': side,
        'order_configuration': {
            'limit_limit_gtc': {
                'base_size': str(base_size),
                'limit_price': limit_price_str,
            }
        },
        'leverage': '3.0',
        'margin_type': 'CROSS',
    }
    resp = requests.post(BASE + path, headers=auth_headers(cb, 'POST', path), json=body)
    try:
        j = resp.json()
    except Exception:
        j = {'raw_text': resp.text}
    # Coinbase preview returns errors in `errs` or `preview_failure_reason`.
    errs = j.get('errs') or []
    failure_reason = j.get('preview_failure_reason') or ''
    rejected = bool(errs) or bool(failure_reason)
    return {
        'http': resp.status_code,
        'rejected': rejected,
        'failure_reason': failure_reason,
        'errs': errs,
        'body': j,
    }


def classify_price(s):
    """Return (total_digits, decimals_after_point) for a numeric string."""
    if '.' in s:
        int_part, dec_part = s.split('.', 1)
        return (len(int_part) + len(dec_part), len(dec_part))
    return (len(s), 0)


def build_test_prices(anchor):
    """
    Generate candidate prices around `anchor` (the BUY limit will be well
    below market so it never fills, but Coinbase validates precision regardless).

    Anchor is e.g. ~50% of market. We vary the number of decimals AND make
    sure to test prices where total digit count crosses common thresholds
    (5, 6, 7, 8, 9 total digits).
    """
    # Round anchor to a whole number for clean reasoning.
    a = int(anchor)
    cands = []

    # --- Varying decimal places at roughly the same magnitude ---
    # Anchor like 39000 — produces 39000, 39000.5, 39000.55, 39000.555, etc.
    for d in range(0, 8):
        if d == 0:
            cands.append(f'{a}')
        else:
            frac = '5' * d
            cands.append(f'{a}.{frac}')

    # --- Sweep total digit count from 1 to 9, all whole numbers ---
    # Tests if Coinbase has a hard cap on total digits.
    for total in range(1, 10):
        # Build like 5, 55, 555, 5555... within reason.
        cands.append('5' * total)

    # --- Mixed: 5 total digits split across the decimal point ---
    # 12345, 1234.5, 123.45, 12.345, 1.2345, 0.12345
    cands.extend(['12345', '1234.5', '123.45', '12.345', '1.2345', '0.12345'])

    # --- Mixed: 6 total digits ---
    cands.extend(['123456', '12345.6', '1234.56', '123.456', '12.3456', '1.23456'])

    # --- Mixed: 7 total digits ---
    cands.extend(['1234567', '123456.7', '12345.67', '1234.567', '123.4567'])

    # --- Mixed: 8 total digits — likely the failure boundary ---
    cands.extend(['12345678', '1234567.8', '123456.78', '12345.678', '1234.5678'])

    # --- The exact rejected price from production ---
    cands.append('78077.7075')

    # Deduplicate while preserving order.
    seen = set()
    uniq = []
    for c in cands:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    ap.add_argument('--product', default='BIP-20DEC30-CDE',
                    help='Product to test. CDE/FCM products take base_size in CONTRACTS.')
    ap.add_argument('--size',    default='1',
                    help='base_size (contracts for FCM/CDE, base asset for INTX).')
    ap.add_argument('--side',    default='BUY', choices=['BUY', 'SELL'])
    ap.add_argument('--base',    type=float, default=None,
                    help='Anchor price for sweeping decimals. Defaults to ~50%% of best bid.')
    ap.add_argument('--only',    default=None,
                    help='Test only this single price string (skips sweeps).')
    args = ap.parse_args()

    cb = CoinbaseHTTP()

    # ----- Step 1: print product spec -----
    print(f'\n================ Product: {args.product} ================')
    prod_resp = fetch_product(cb, args.product)
    prod = prod_resp.get('product') or prod_resp
    for k in ('product_id', 'product_type', 'quote_increment', 'base_increment',
              'base_min_size', 'base_max_size', 'price_increment',
              'price', 'status', 'trading_disabled'):
        if k in prod:
            print(f'  {k}: {prod[k]}')
    fd = prod.get('future_product_details') or {}
    if fd:
        print(f'  contract_size: {fd.get("contract_size")}')
        print(f'  contract_root_unit: {fd.get("contract_root_unit")}')

    quote_increment = prod.get('quote_increment')
    print(f'\nDocumented quote_increment = {quote_increment}')

    # ----- Step 2: discover live price -----
    print('\n================ Live market ================')
    market = fetch_best_price(cb, args.product)
    if market:
        print(f'  best_bid = {market.get("bid")}')
        print(f'  best_ask = {market.get("ask")}')
    else:
        print('  (could not fetch best bid/ask)')

    # ----- Step 3: pick anchor for BUY limits well below market -----
    if args.base is not None:
        anchor = args.base
    else:
        try:
            anchor = float(market['bid']) * 0.5
        except Exception:
            anchor = 50000.0
    print(f'\nAnchor for decimal sweeps: {anchor:g} (BUY limits this low will not fill)')

    # ----- Step 4: build & test prices -----
    prices = [args.only] if args.only else build_test_prices(anchor)
    print(f'\n================ Testing {len(prices)} prices ================')
    print(f"{'PRICE':<18} {'TOTAL':>5} {'DEC':>4}  {'RESULT':<6}  REASON")
    print('-' * 100)

    passed = []
    failed_precision = []
    failed_other = []

    for p in prices:
        total, dec = classify_price(p)
        res = preview_limit(cb, args.product, args.side, args.size, p)
        if res['rejected']:
            # Pull a short tag for the reason. `errs` items can be strings or dicts.
            reasons = []
            for e in res['errs']:
                if isinstance(e, dict):
                    code = e.get('error_code') or e.get('error') or ''
                    msg  = e.get('error_message') or e.get('message') or ''
                    reasons.append(f'{code}:{msg}'.strip(':'))
                else:
                    reasons.append(str(e))
            if res['failure_reason']:
                reasons.append(res['failure_reason'])
            reason = '; '.join(reasons) or 'unknown'
            label = 'FAIL'
            if 'PRICE_PRECISION' in reason or 'precision' in reason.lower():
                failed_precision.append(p)
            else:
                failed_other.append((p, reason))
        else:
            reason = 'OK'
            label = 'PASS'
            passed.append(p)

        print(f'{p:<18} {total:>5} {dec:>4}  {label:<6}  {reason[:80]}')

    # ----- Step 5: summary -----
    print('\n================ Summary ================')
    print(f'PASS                  ({len(passed)}): {passed}')
    print(f'FAIL (precision-rel)  ({len(failed_precision)}): {failed_precision}')
    print(f'FAIL (other reasons)  ({len(failed_other)}):')
    for p, r in failed_other:
        print(f'   {p}: {r[:120]}')

    # Infer the boundary from passes that are >= the quote_increment magnitude.
    if passed:
        max_total = max(classify_price(p)[0] for p in passed)
        max_dec   = max(classify_price(p)[1] for p in passed)
        print(f'\nPassing prices used at most {max_total} total digits and {max_dec} decimals.')
        if failed_precision:
            min_fail_total = min(classify_price(p)[0] for p in failed_precision)
            min_fail_dec   = min(classify_price(p)[1] for p in failed_precision)
            print(f'Failing (precision) prices used at least {min_fail_total} total digits or {min_fail_dec} decimals.')


if __name__ == '__main__':
    main()
