#!/usr/bin/env python3
"""
Enumerate Coinbase Advanced Trade products visible to your API key.

Uses the same JWT auth as the bot, so you see exactly what your key can
trade. If BTC-PERP-INTX shows up here you can trade it; if it doesn't,
Coinbase considers it invalid for this key.

Examples:
  # All products (paginated)
  python3 testscripts/test_products.py

  # Only perpetual futures
  python3 testscripts/test_products.py --type FUTURE --expiry PERPETUAL

  # Search by substring
  python3 testscripts/test_products.py --search PERP

  # Full details for one product
  python3 testscripts/test_products.py --details BTC-PERP-INTX
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


def fmt_row(p):
    fd = p.get('future_product_details') or {}
    perp = fd.get('perpetual_details') or {}
    flags = []
    if p.get('trading_disabled'): flags.append('TRADING_DISABLED')
    if p.get('is_disabled'):      flags.append('DISABLED')
    if p.get('cancel_only'):      flags.append('CANCEL_ONLY')
    if p.get('post_only'):        flags.append('POST_ONLY')
    if p.get('view_only'):        flags.append('VIEW_ONLY')
    return (
        f"  {p.get('product_id', ''):<24}"
        f" type={p.get('product_type', ''):<8}"
        f" status={p.get('status', ''):<6}"
        f" base={p.get('base_currency_id', '') or p.get('base_display_symbol', ''):<8}"
        f" quote={p.get('quote_currency_id', '') or p.get('quote_display_symbol', ''):<6}"
        f" contract_size={fd.get('contract_size', '')}"
        f" max_lev={perp.get('max_leverage', '')}"
        f" {' '.join(flags)}"
    )


def list_all(cb, product_type, expiry, search):
    cursor = None
    total = 0
    matched = 0
    while True:
        path = '/api/v3/brokerage/products'
        params = []
        if product_type:
            params.append(f'product_type={product_type}')
        if expiry:
            params.append(f'contract_expiry_type={expiry}')
        if cursor:
            params.append(f'cursor={cursor}')
        if params:
            path += '?' + '&'.join(params)
        url = BASE + path

        resp = requests.get(url, headers=auth_headers(cb, 'GET', path.split('?')[0]))
        if not resp.ok:
            print(f'HTTP {resp.status_code}: {resp.text[:500]}')
            return
        data = resp.json()

        products = data.get('products', []) or []
        total += len(products)
        for p in products:
            pid = p.get('product_id', '')
            if search and search.upper() not in pid.upper():
                continue
            matched += 1
            print(fmt_row(p))

        pagination = data.get('pagination') or {}
        cursor = pagination.get('next_cursor') or data.get('cursor')
        if not cursor or not products:
            break

    print(f'\nSeen {total} products, {matched} matched filter.')


def show_details(cb, product_id):
    path = f'/api/v3/brokerage/products/{product_id}'
    url = BASE + path
    resp = requests.get(url, headers=auth_headers(cb, 'GET', path))
    print(f'HTTP {resp.status_code}')
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)


def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    ap.add_argument('--type',    default=None,
                    help='product_type filter: SPOT, FUTURE, etc.')
    ap.add_argument('--expiry',  default=None,
                    help='contract_expiry_type filter: PERPETUAL, EXPIRING, etc.')
    ap.add_argument('--search',  default=None,
                    help='only show product_ids containing this substring (case-insensitive).')
    ap.add_argument('--details', default=None,
                    help='fetch full details for a single product_id and print raw JSON.')
    args = ap.parse_args()

    cb = CoinbaseHTTP()

    if args.details:
        show_details(cb, args.details)
        return

    list_all(cb, args.type, args.expiry, args.search)


if __name__ == '__main__':
    main()
