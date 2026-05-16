import time
import secrets
import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
import json
import jwt
from urllib.parse import urlparse
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import util

_util = util.util()

_BASE = 'https://api.coinbase.com'
_JWT_TTL = 120

# Coinbase API returns wrong contract sizes and max leverages post US/INTX merger.
# These are the correct values confirmed from the exchange.
# Format: product_id -> (contract_size_in_base_asset, max_leverage)
_PRODUCT_SPECS = {
    'BTC-PERP-INTX':  (0.01,   3.3),
    'ETH-PERP-INTX':  (0.1,    3.0),
    'SOL-PERP-INTX':  (5.0,    1.8),
    'XRP-PERP-INTX':  (500.0,  1.8),
    'DOGE-PERP-INTX': (5000.0, 1.1),
    'ADA-PERP-INTX':  (1000.0, 2.4),
    'PAXG-PERP-INTX': (1.0,    12.1),
    'ZEC-PERP-INTX':  (1.0,    2.0),
    'XLM-PERP-INTX':  (5000.0, 2.6),
    'LINK-PERP-INTX': (50.0,   2.3),
    'SUI-PERP-INTX':  (500.0,  1.8),
    'AAVE-PERP-INTX': (5.0,    1.5),
}

KNOWN_CONTRACT_SIZES = {k: v[0] for k, v in _PRODUCT_SPECS.items()}
KNOWN_MAX_LEVERAGES  = {k: v[1] for k, v in _PRODUCT_SPECS.items()}


class CoinbaseHTTP:
    """
    Thin HTTP client for Coinbase Advanced Trade API endpoints.

    Auth: Coinbase requires each JWT to include a 'uri' claim that encodes the
    specific method+path being called, so a new JWT is generated per request.
    The crypto operation is fast (<1ms) and this matches how the official SDK works.
    """

    _instance = None
    _key_name: str = None
    _private_key = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # ------------------------------------------------------------------ auth

    def _load_key(self):
        """Load and cache the parsed private key from DB credentials."""
        key_name = _util.getkeyval('cbkey')
        key_secret = _util.getkeyval('cbsecret')
        if not key_name or not key_secret:
            raise Exception("Coinbase credentials missing — set cbkey and cbsecret in Settings")
        if self._key_name == key_name and self._private_key is not None:
            return key_name, self._private_key
        # Normalize escaped newlines from copy-paste
        key_secret = key_secret.replace('\\n', '\n').strip()
        private_key = load_pem_private_key(key_secret.encode(), password=None)
        CoinbaseHTTP._key_name = key_name
        CoinbaseHTTP._private_key = private_key
        return key_name, private_key

    def _make_jwt(self, method: str, path: str) -> str:
        """Generate a request-scoped JWT with the required 'uri' claim."""
        key_name, private_key = self._load_key()
        now = int(time.time())
        uri = f"{method.upper()} api.coinbase.com{path}"
        return jwt.encode(
            {'sub': key_name, 'iss': 'cdp', 'nbf': now, 'exp': now + _JWT_TTL, 'uri': uri},
            private_key,
            algorithm='ES256',
            headers={'kid': key_name, 'nonce': secrets.token_hex(16)},
        )

    # ------------------------------------------------------------------ base request

    def request(self, url: str, method: str = 'GET', body: dict = None) -> str:
        """
        Make an authenticated request to the Coinbase API.

        Args:
            url:    Full URL including query string if needed.
            method: HTTP verb ('GET', 'POST', 'DELETE', ...).
            body:   Optional dict — sent as JSON request body.

        Returns:
            Response body as a string (caller parses as needed).
        """
        path = urlparse(url).path
        token = self._make_jwt(method, path)
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        }

        for attempt in range(2):
            try:
                resp = requests.request(method, url, headers=headers, json=body)
                break
            except RequestsConnectionError as e:
                if attempt == 0:
                    print(f"[CoinbaseHTTP] Connection reset on {method} {url}, retrying...")
                    time.sleep(1)
                    token = self._make_jwt(method, path)
                    headers['Authorization'] = f'Bearer {token}'
                else:
                    raise

        if resp.status_code == 401:
            # Credentials may have changed — clear cached key and retry once
            CoinbaseHTTP._private_key = None
            token = self._make_jwt(method, path)
            headers['Authorization'] = f'Bearer {token}'
            resp = requests.request(method, url, headers=headers, json=body)

        if not resp.ok:
            print(f"[CoinbaseHTTP] {method} {url} → {resp.status_code}: {resp.text[:500]}")

        return resp.text

    def _get(self, path: str, params: dict = None) -> dict:
        """GET helper — appends query params and returns parsed JSON."""
        url = _BASE + path
        if params:
            qs = '&'.join(f'{k}={v}' for k, v in params.items() if v is not None)
            if qs:
                url += '?' + qs
        text = self.request(url, method='GET')
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            raise Exception(f"Non-JSON response from GET {url}: {text[:300]!r}")

    def _post(self, path: str, body: dict) -> dict:
        """POST helper — returns parsed JSON."""
        text = self.request(_BASE + path, method='POST', body=body)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            raise Exception(f"Non-JSON response from POST {path}: {text[:300]!r}")

    # ------------------------------------------------------------------ key permissions

    def get_key_permissions(self) -> dict:
        """
        GET /api/v3/brokerage/key_permissions
        Returns: { can_view, can_trade, can_transfer, portfolio_uuid, portfolio_type }
        Useful for diagnosing PERMISSION_DENIED errors — tells you exactly which
        scopes the current key has and which portfolio it belongs to.
        """
        return self._get('/api/v3/brokerage/key_permissions')

    # ------------------------------------------------------------------ accounts

    def list_accounts(self, limit: int = None, cursor: str = None) -> dict:
        """
        GET /api/v3/brokerage/accounts
        Returns: { has_next, accounts: [...], cursor, size }
        """
        return self._get('/api/v3/brokerage/accounts', {
            'limit': limit,
            'cursor': cursor,
        })

    def get_account(self, account_uuid: str) -> dict:
        """
        GET /api/v3/brokerage/accounts/{account_uuid}
        Returns: { account: { uuid, name, currency, available_balance, ... } }
        """
        return self._get(f'/api/v3/brokerage/accounts/{account_uuid}')

    # ------------------------------------------------------------------ margin / balance

    def get_current_margin_window(self, margin_profile_type: str = 'MARGIN_PROFILE_TYPE_UNSPECIFIED') -> dict:
        """
        GET /api/v3/brokerage/cfm/intraday/current_margin_window
        Returns: { margin_window, is_intraday_margin_killswitch_enabled, ... }
        """
        return self._get('/api/v3/brokerage/cfm/intraday/current_margin_window', {
            'margin_profile_type': margin_profile_type,
        })

    def get_balance_summary(self) -> dict:
        """
        GET /api/v3/brokerage/cfm/balance_summary
        Returns the full US Derivatives balance summary including available_margin,
        futures_buying_power, unrealized_pnl, initial_margin, etc.
        """
        return self._get('/api/v3/brokerage/cfm/balance_summary')

    # ------------------------------------------------------------------ positions

    def get_position(self, product_id: str) -> dict:
        """
        GET /api/v3/brokerage/cfm/positions/{product_id}
        Returns: { position: { product_id, side, number_of_contracts, avg_entry_price, ... } }
        """
        return self._get(f'/api/v3/brokerage/cfm/positions/{product_id}')

    def list_positions(self) -> dict:
        """
        GET /api/v3/brokerage/cfm/positions
        Returns: { positions: [ { product_id, side, number_of_contracts, ... } ] }
        """
        return self._get('/api/v3/brokerage/cfm/positions')

    # ------------------------------------------------------------------ orders

    def create_order(self, client_order_id: str, product_id: str, side: str,
                     order_configuration: dict, leverage: str = None,
                     margin_type: str = None, retail_portfolio_id: str = None) -> dict:
        """
        POST /api/v3/brokerage/orders
        side: 'BUY' or 'SELL'
        order_configuration: one of the Coinbase order config shapes, e.g.:
            { 'market_market_ioc': { 'base_size': '0.001' } }
            { 'limit_limit_gtc':   { 'base_size': '0.001', 'limit_price': '50000' } }
            { 'stop_limit_stop_limit_gtc': { 'base_size': '0.001', 'limit_price': '...', 'stop_price': '...', 'stop_direction': '...' } }
        Returns: { success, success_response: { order_id, ... }, error_response }
        """
        body = {
            'client_order_id': client_order_id,
            'product_id': product_id,
            'side': side,
            'order_configuration': order_configuration,
        }
        if leverage is not None:
            body['leverage'] = leverage
        if margin_type is not None:
            body['margin_type'] = margin_type
        if retail_portfolio_id is not None:
            body['retail_portfolio_id'] = retail_portfolio_id
        return self._post('/api/v3/brokerage/orders', body)

    def preview_order(self, product_id: str, side: str, order_configuration: dict,
                      leverage: str = None, margin_type: str = None,
                      retail_portfolio_id: str = None) -> dict:
        """
        POST /api/v3/brokerage/orders/preview
        Returns: { order_total, commission_total, best_bid, best_ask, leverage, ... }
        """
        body = {
            'product_id': product_id,
            'side': side,
            'order_configuration': order_configuration,
        }
        if leverage is not None:
            body['leverage'] = leverage
        if margin_type is not None:
            body['margin_type'] = margin_type
        if retail_portfolio_id is not None:
            body['retail_portfolio_id'] = retail_portfolio_id
        return self._post('/api/v3/brokerage/orders/preview', body)

    def cancel_orders(self, order_ids: list) -> dict:
        """
        POST /api/v3/brokerage/orders/batch_cancel
        order_ids: list of Coinbase order ID strings
        Returns: { results: [ { success, failure_reason, order_id } ] }
        """
        return self._post('/api/v3/brokerage/orders/batch_cancel', {'order_ids': order_ids})

    def close_position(self, client_order_id: str, product_id: str, size: float = None) -> dict:
        """
        POST /api/v3/brokerage/orders/close_position
        size: number of contracts to close; omit to close the full position.
        Returns: { success, success_response: { order_id, ... }, error_response }
        """
        body = {
            'client_order_id': client_order_id,
            'product_id': product_id,
        }
        if size is not None:
            body['size'] = size
        return self._post('/api/v3/brokerage/orders/close_position', body)

    def edit_order(self, order_id: str, price: str = None, size: str = None,
                   stop_price: str = None) -> dict:
        """
        POST /api/v3/brokerage/orders/edit
        Returns: { success, errors: [ { edit_failure_reason, preview_failure_reason } ] }
        """
        body = {'order_id': order_id}
        if price is not None:
            body['price'] = price
        if size is not None:
            body['size'] = size
        if stop_price is not None:
            body['stop_price'] = stop_price
        return self._post('/api/v3/brokerage/orders/edit', body)

    def edit_order_preview(self, order_id: str, price: str = None, size: str = None,
                           stop_price: str = None) -> dict:
        """
        POST /api/v3/brokerage/orders/edit_preview
        Returns: { slippage, order_total, commission_total, best_bid, best_ask, ... }
        """
        body = {'order_id': order_id}
        if price is not None:
            body['price'] = price
        if size is not None:
            body['size'] = size
        if stop_price is not None:
            body['stop_price'] = stop_price
        return self._post('/api/v3/brokerage/orders/edit_preview', body)

    def get_order(self, order_id: str) -> dict:
        """
        GET /api/v3/brokerage/orders/historical/{order_id}
        Returns: { order: { order_id, product_id, side, status, filled_size, ... } }
        """
        return self._get(f'/api/v3/brokerage/orders/historical/{order_id}')

    def list_orders(self, product_id: str = None, order_status: list = None,
                    product_type: str = None, limit: int = None,
                    cursor: str = None, sort_by: str = None) -> dict:
        """
        GET /api/v3/brokerage/orders/historical/batch
        Returns: { orders: [...], has_next, cursor }
        order_status example: ['OPEN'], ['FILLED'], ['CANCELLED']
        """
        params = {
            'product_type': product_type or 'UNKNOWN_PRODUCT_TYPE',
            'order_placement_source': 'RETAIL_ADVANCED',
            'limit': limit,
            'cursor': cursor,
            'sort_by': sort_by,
        }
        if product_id:
            params['product_id'] = product_id
        if order_status:
            # Coinbase accepts repeated query params; build manually
            url = _BASE + '/api/v3/brokerage/orders/historical/batch'
            qs_parts = [f'{k}={v}' for k, v in params.items() if v is not None]
            for s in order_status:
                qs_parts.append(f'order_status={s}')
            url += '?' + '&'.join(qs_parts)
            return json.loads(self.request(url, method='GET'))
        return self._get('/api/v3/brokerage/orders/historical/batch', params)

    # ------------------------------------------------------------------ candles

    def get_candles(self, product_id: str, start: str, end: str, granularity: str) -> dict:
        """
        GET /api/v3/brokerage/products/{product_id}/candles
        Returns: { candles: [ { start, low, high, open, close, volume } ] }
        """
        return self._get(f'/api/v3/brokerage/products/{product_id}/candles', {
            'start': start,
            'end': end,
            'granularity': granularity,
        })

    def list_fills(self, product_id: str = None, order_id: str = None,
                   start_sequence_timestamp: str = None, end_sequence_timestamp: str = None,
                   limit: int = None, cursor: str = None, sort_by: str = None) -> dict:
        """
        GET /api/v3/brokerage/orders/historical/fills
        Returns: { fills: [ { entry_id, trade_id, order_id, price, size, commission, ... } ], cursor }
        """
        return self._get('/api/v3/brokerage/orders/historical/fills', {
            'product_id': product_id,
            'order_id': order_id,
            'start_sequence_timestamp': start_sequence_timestamp,
            'end_sequence_timestamp': end_sequence_timestamp,
            'limit': limit,
            'cursor': cursor,
            'sort_by': sort_by or 'UNKNOWN_SORT_BY',
        })

    # ------------------------------------------------------------------ products

    def list_products(self, product_type: str = None, contract_expiry_type: str = None,
                      limit: int = None, cursor: str = None) -> dict:
        """
        GET /api/v3/brokerage/products
        Returns: { products: [ { product_id, base_min_size, base_max_size, base_increment,
                                  future_product_details: { contract_size, perpetual_details: { max_leverage } } } ],
                   num_products, pagination }
        Key fields for perps: future_product_details.contract_size, future_product_details.perpetual_details.max_leverage
        """
        return self._get('/api/v3/brokerage/products', {
            'product_type': product_type,
            'contract_expiry_type': contract_expiry_type,
            'limit': limit,
            'cursor': cursor,
        })

    def get_product(self, product_id: str) -> dict:
        """
        GET /api/v3/brokerage/products/{product_id}
        Returns the full product dict including:
          - base_min_size, base_max_size, base_increment
          - future_product_details.contract_size  (e.g. "0.001" for BTC-PERP-INTX)
          - future_product_details.perpetual_details.max_leverage
          - future_product_details.perpetual_details.funding_rate
        """
        return self._get(f'/api/v3/brokerage/products/{product_id}')
