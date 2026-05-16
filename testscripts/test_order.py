#!/usr/bin/env python3
"""
Standalone Coinbase perp order tester.

Sends a create_order request with the same auth and body shape as the live
trading bot. Prints the full request, HTTP status, response headers, and
parsed body — so you can iterate on what Coinbase actually accepts when the
docs are incomplete.

Examples:
  # Send a real limit buy of 1 BTC contract at $50k (well below market, won't fill)
  python3 testscripts/test_order.py --limit 50000

  # Just print what would be sent, don't actually call Coinbase
  python3 testscripts/test_order.py --limit 50000 --dry-run

  # Hit the /orders/preview endpoint — validates without placing
  python3 testscripts/test_order.py --limit 50000 --preview

  # Try without leverage / margin_type to see if the missing fields are the cause
  python3 testscripts/test_order.py --limit 50000 --no-leverage --no-margin

  # Market buy of 1 contract (REAL — will fill)
  python3 testscripts/test_order.py --size 0.01

  # Same idea but for SELL
  python3 testscripts/test_order.py --side SELL --limit 200000

Credentials are read from the same DB (server/data/db.sqlite, keys `cbkey` /
`cbsecret`) that the bot uses.
"""

import argparse
import json
import os
import sys
import uuid

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..', 'server'))

from coinbase_http import CoinbaseHTTP  # noqa: E402

BASE = 'https://api.coinbase.com'


def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    ap.add_argument('--product',  default='BTC-PERP-INTX')
    ap.add_argument('--side',     default='BUY', choices=['BUY', 'SELL'])
    ap.add_argument('--size',     default='0.01',
                    help='base_size in base asset units. 0.01 = 1 BTC contract.')
    ap.add_argument('--limit',    type=float, default=None,
                    help='limit price; omit for a market order.')
    ap.add_argument('--leverage', default='3.0')
    ap.add_argument('--margin',   default='CROSS', choices=['CROSS', 'ISOLATED'])
    ap.add_argument('--preview',  action='store_true',
                    help='hit /orders/preview instead of /orders (validates without placing).')
    ap.add_argument('--no-leverage', action='store_true',
                    help='omit the leverage field from the body.')
    ap.add_argument('--no-margin',   action='store_true',
                    help='omit the margin_type field from the body.')
    ap.add_argument('--dry-run',  action='store_true',
                    help='print the request body and headers, do not call Coinbase.')
    args = ap.parse_args()

    # Build the order_configuration block — same shape live.py uses.
    if args.limit is not None:
        config = {
            'limit_limit_gtc': {
                'base_size': str(args.size),
                'limit_price': f'{args.limit:.2f}',
            }
        }
    else:
        config = {'market_market_ioc': {'base_size': str(args.size)}}

    body = {
        'product_id': args.product,
        'side': args.side,
        'order_configuration': config,
    }
    # /orders requires client_order_id; /orders/preview rejects it.
    if not args.preview:
        body['client_order_id'] = str(uuid.uuid4())
    if not args.no_leverage:
        body['leverage'] = args.leverage
    if not args.no_margin:
        body['margin_type'] = args.margin

    path = '/api/v3/brokerage/orders' + ('/preview' if args.preview else '')
    url = BASE + path

    # Reuse the bot's JWT signing so auth is byte-identical to what live.py sends.
    cb = CoinbaseHTTP()
    token = cb._make_jwt('POST', path)
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    print('================ Request ================')
    print(f'POST {url}')
    print('Headers:')
    for k, v in headers.items():
        if k == 'Authorization':
            v = v[:30] + '…(truncated)'
        print(f'  {k}: {v}')
    print('Body:')
    print(json.dumps(body, indent=2))

    if args.dry_run:
        print('\n(--dry-run: not sending)')
        return

    resp = requests.post(url, headers=headers, json=body)

    print(f'\n================ Response: HTTP {resp.status_code} ================')
    print('Headers:')
    for k, v in resp.headers.items():
        print(f'  {k}: {v}')
    print('Body:')
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)


if __name__ == '__main__':
    main()
