import time
import math
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import requests
import logging
from typing import List, Dict, Optional, Tuple
from cryptography.fernet import Fernet
import ccxt
import ccxt.async_support as ccxt_async
import dash
from dash import dcc, html, Output, Input
import plotly.graph_objs as go
from concurrent.futures import ThreadPoolExecutor
import cachetools
import psutil
import asyncio
from threading import Thread

# ======== THIẾT LẬP BAN ĐẦU ========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grid_trading_bot.log'),
        logging.StreamHandler()
    ]
)

api_cache = cachetools.TTLCache(maxsize=100, ttl=300)

# ======== BẢO MẬT API KEYS ========
def get_encrypted_credentials(exchange: str) -> Tuple[str, str]:
    cipher_key = os.environ.get(f"{exchange.upper()}_CIPHER_KEY") or os.getenv(f"{exchange.upper()}_CIPHER_KEY")
    if not cipher_key:
        cipher_key = Fernet.generate_key().decode()
        logging.warning(f"Không tìm thấy cipher key cho {exchange}. Tạo mới: {cipher_key}. Vui lòng lưu vào biến môi trường hệ thống.")
    
    cipher_suite = Fernet(cipher_key.encode())
    encrypted_api_key = os.getenv(f"{exchange.upper()}_ENCRYPTED_API_KEY")
    encrypted_api_secret = os.getenv(f"{exchange.upper()}_ENCRYPTED_API_SECRET")
    
    if not encrypted_api_key or not encrypted_api_secret:
        api_key = os.getenv(f"{exchange.upper()}_API_KEY", "your_api_key")
        api_secret = os.getenv(f"{exchange.upper()}_API_SECRET", "your_api_secret")
        encrypted_api_key = cipher_suite.encrypt(api_key.encode()).decode()
        encrypted_api_secret = cipher_suite.encrypt(api_secret.encode()).decode()
        with open(".env", "a") as f:
            f.write(f"\n{exchange.upper()}_ENCRYPTED_API_KEY={encrypted_api_key}\n{exchange.upper()}_ENCRYPTED_API_SECRET={encrypted_api_secret}")
    
    return (
        cipher_suite.decrypt(encrypted_api_key.encode()).decode(),
        cipher_suite.decrypt(encrypted_api_secret.encode()).decode()
    )

# ======== CẤU HÌNH NÂNG CAO ========
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges = os.getenv("ENABLED_EXCHANGES", "binance").split(",")
        self.exchange_credentials = {ex: get_encrypted_credentials(ex) for ex in self.enabled_exchanges}
        self.symbol = os.getenv("TRADING_SYMBOL", "BTCUSDT")
        self.initial_investment = float(os.getenv("INITIAL_INVESTMENT", 100))
        self.min_quantity = 0.0002
        self.max_position = 0.01
        self.base_stop_loss_percent = 2.0
        self.base_take_profit_percent = 2.0
        self.price_drop_threshold = 1.0
        self.adaptive_grid_enabled = True
        self.min_grid_levels = 5
        self.max_grid_levels = 10
        self.base_grid_step_percent = 0.01
        self.grid_rebalance_interval = 300
        self.trailing_stop_enabled = True
        self.trailing_up_activation = 85000
        self.trailing_down_activation = 82000
        self.trailing_tp_enabled = True
        self.trailing_buy_stop_enabled = True
        self.trailing_buy_activation_percent = 1.5
        self.trailing_buy_distance_percent = 1.0
        self.pump_protection_threshold = 0.03
        self.circuit_breaker_threshold = 0.07
        self.circuit_breaker_duration = 360
        self.abnormal_activity_threshold = 3.0
        self.maker_fee = 0.0002
        self.taker_fee = 0.0004
        self.telegram_enabled = bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.api_retry_count = 3
        self.api_retry_delay = 5
        self.max_open_orders = 50
        self.status_update_interval = 600
        self.volatility_window = 100
        self.ma_short_period = 20
        self.ma_long_period = 50
        self.atr_period = 14
        self.rsi_period = 14
        self.bb_period = 20
        self.bb_std_dev = 1.8
        self.min_acceptable_volume = 100
        self.fee_threshold = 0.1
        self.websocket_enabled = True
        self.websocket_reconnect_interval = 60

config = AdvancedConfig()

# ======== TRẠNG THÁI BOT ========
class BotState:
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.tracker = EnhancedProfitTracker()
        self.protection = EnhancedProtectionSystem()
        self.current_price = 0.0
        self.grid_manager = EnhancedSmartGridManager()
        self.order_manager = EnhancedOrderManager()

# ======== LỚP TRỪU TƯỢNG CHO SÀN GIAO DỊCH ========
class ExchangeInterface:
    def __init__(self, exchange_name: str, api_key: str, api_secret: str):
        self.exchange_name = exchange_name
        self.symbol = config.symbol
        self.price_precision = 2
        self.quantity_precision = 6
        self.websocket_connected = False
        self.latest_price = 0.0
        self.latest_volume = 0.0
        self.latest_high = 0.0
        self.latest_low = 0.0

    def get_price(self) -> float:
        raise NotImplementedError

    def get_volume(self) -> float:
        raise NotImplementedError

    def get_high_low(self) -> Tuple[float, float]:
        raise NotImplementedError

    def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    def get_open_orders(self) -> List[Dict]:
        raise NotImplementedError

    def get_all_orders(self) -> List[Dict]:
        raise NotImplementedError

    def get_balance(self) -> Tuple[float, float]:
        raise NotImplementedError

    def get_order_book(self) -> Dict:
        raise NotImplementedError

    def get_symbol_filters(self) -> Dict:
        raise NotImplementedError

    def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        raise NotImplementedError

    async def start_websocket(self, state: BotState):
        raise NotImplementedError

class BinanceExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("binance", api_key, api_secret)
        self.client = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'testnet': True
        })
        self.ws_client = None

    def get_price(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['last'])

    def get_volume(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['quoteVolume'])

    def get_high_low(self) -> Tuple[float, float]:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['high']), float(ticker['low'])

    def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
        return self.client.create_order(
            symbol=self.symbol,
            side=side.lower(),
            type=order_type,
            amount=round(quantity, self.quantity_precision),
            price=round(price, self.price_precision) if price else None
        )

    def cancel_order(self, order_id: str) -> None:
        self.client.cancel_order(order_id, self.symbol)

    def get_open_orders(self) -> List[Dict]:
        orders = self.client.fetch_open_orders(self.symbol)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]

    def get_all_orders(self) -> List[Dict]:
        orders = self.client.fetch_orders(self.symbol, limit=50)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]

    def get_balance(self) -> Tuple[float, float]:
        balance = self.client.fetch_balance()
        usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
        base_asset = self.symbol.split('USDT')[0]
        btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
        return usdt_balance, btc_balance

    def get_order_book(self) -> Dict:
        return self.client.fetch_order_book(self.symbol, limit=50)

    def get_symbol_filters(self) -> Dict:
        try:
            markets = self.client.load_markets()
            market = markets[self.symbol]
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Lỗi lấy thông tin LOT_SIZE trên Binance: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
        return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)

    async def start_websocket(self, state: BotState):
        if not config.websocket_enabled:
            return
        await self.client.load_markets()
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                state.current_price = float(ticker['last'])
                self.latest_price = state.current_price
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Giá cập nhật - {state.current_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} lỗi: {str(e)}, thử lại sau {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

class MEXCExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("mexc", api_key, api_secret)
        self.client = ccxt_async.mexc({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        self.symbol = self.symbol.replace("USDT", "/USDT")

    def get_price(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['last'])

    def get_volume(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['quoteVolume'])

    def get_high_low(self) -> Tuple[float, float]:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['high']), float(ticker['low'])

    def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
        return self.client.create_order(
            symbol=self.symbol,
            side=side.lower(),
            type=order_type,
            amount=round(quantity, self.quantity_precision),
            price=round(price, self.price_precision) if price else None
        )

    def cancel_order(self, order_id: str) -> None:
        self.client.cancel_order(order_id, self.symbol)

    def get_open_orders(self) -> List[Dict]:
        orders = self.client.fetch_open_orders(self.symbol)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]

    def get_all_orders(self) -> List[Dict]:
        orders = self.client.fetch_orders(self.symbol, limit=50)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]

    def get_balance(self) -> Tuple[float, float]:
        balance = self.client.fetch_balance()
        usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
        base_asset = self.symbol.split('/')[0]
        btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
        return usdt_balance, btc_balance

    def get_order_book(self) -> Dict:
        return self.client.fetch_order_book(self.symbol, limit=50)

    def get_symbol_filters(self) -> Dict:
        try:
            markets = self.client.load_markets()
            market = markets[self.symbol]
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Lỗi lấy thông tin LOT_SIZE trên MEXC: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
        return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)

    async def start_websocket(self, state: BotState):
        if not config.websocket_enabled:
            return
        await self.client.load_markets()
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                state.current_price = float(ticker['last'])
                self.latest_price = state.current_price
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Giá cập nhật - {state.current_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} lỗi: {str(e)}, thử lại sau {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

class OKXExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("okx", api_key, api_secret)
        self.client = ccxt_async.okx({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        self.symbol = self.symbol.replace("USDT", "/USDT")

    def get_price(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['last'])

    def get_volume(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['quoteVolume'])

    def get_high_low(self) -> Tuple[float, float]:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['high']), float(ticker['low'])

    def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
        return self.client.create_order(
            symbol=self.symbol,
            side=side.lower(),
            type=order_type,
            amount=round(quantity, self.quantity_precision),
            price=round(price, self.price_precision) if price else None
        )

    def cancel_order(self, order_id: str) -> None:
        self.client.cancel_order(order_id, self.symbol)

    def get_open_orders(self) -> List[Dict]:
        orders = self.client.fetch_open_orders(self.symbol)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]

    def get_all_orders(self) -> List[Dict]:
        orders = self.client.fetch_orders(self.symbol, limit=50)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]

    def get_balance(self) -> Tuple[float, float]:
        balance = self.client.fetch_balance()
        usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
        base_asset = self.symbol.split('/')[0]
        btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
        return usdt_balance, btc_balance

    def get_order_book(self) -> Dict:
        return self.client.fetch_order_book(self.symbol, limit=50)

    def get_symbol_filters(self) -> Dict:
        try:
            markets = self.client.load_markets()
            market = markets[self.symbol]
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Lỗi lấy thông tin LOT_SIZE trên OKX: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
        return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)

    async def start_websocket(self, state: BotState):
        if not config.websocket_enabled:
            return
        await self.client.load_markets()
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                state.current_price = float(ticker['last'])
                self.latest_price = state.current_price
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Giá cập nhật - {state.current_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} lỗi: {str(e)}, thử lại sau {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

# ======== QUẢN LÝ NHIỀU SÀN GIAO DỊCH ========
class ExchangeManager:
    def __init__(self):
        self.exchanges = {}
        for exchange_name in config.enabled_exchanges:
            api_key, api_secret = config.exchange_credentials[exchange_name]
            if exchange_name == "binance":
                self.exchanges[exchange_name] = BinanceExchange(api_key, api_secret)
            elif exchange_name == "mexc":
                self.exchanges[exchange_name] = MEXCExchange(api_key, api_secret)
            elif exchange_name == "okx":
                self.exchanges[exchange_name] = OKXExchange(api_key, api_secret)
            else:
                logging.error(f"Sàn {exchange_name} không được hỗ trợ!")
                raise ValueError(f"Unsupported exchange: {exchange_name}")
            logging.info(f"Đã khởi tạo sàn {exchange_name}")

    def check_price_disparity(self) -> None:
        prices = {name: ex.latest_price if ex.websocket_connected else safe_api_call(ex.get_price) for name, ex in self.exchanges.items()}
        max_diff = max(prices.values()) - min(prices.values())
        if max_diff / min(prices.values()) > 0.01:
            logging.warning(f"Chênh lệch giá lớn: {max_diff:.2f} giữa {prices}")
            send_telegram_alert(f"⚠️ Chênh lệch giá lớn: {max_diff:.2f} giữa {prices}")

# ======== XỬ LÝ LỖI MẠNH HƠN ========
def safe_api_call(func, *args, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            cache_key = f"{func.__name__}_{str(args)}_{str(kwargs)}"
            if cache_key in api_cache:
                return api_cache[cache_key]
            result = func(*args, **kwargs)
            api_cache[cache_key] = result
            return result
        except (ccxt.DDoSProtection, ccxt.RateLimitExceeded) as e:
            logging.warning(f"Sàn giới hạn API: {str(e)}. Chờ {config.api_retry_delay} giây")
            time.sleep(config.api_retry_delay * (2 ** attempt))
        except ccxt.ExchangeNotAvailable as e:
            logging.error(f"Sàn không khả dụng: {str(e)}. Tạm dừng 5 phút")
            send_telegram_alert(f"⚠️ Sàn không khả dụng: {str(e)}")
            time.sleep(300)
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            if attempt == max_retries - 1:
                logging.error(f"API call thất bại sau {max_retries} lần: {str(e)}")
                raise
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Lỗi không xác định: {str(e)}")
            raise

# ======== TÍNH TOÁN CHỈ BÁO KỸ THUẬT ========
def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
    if len(closes) < period + 1:
        return 0.0
    tr_list = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
    return float(np.mean(tr_list[-period:]))

def calculate_rsi(prices: List[float], period: int) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        gains.append(change if change > 0 else 0)
        losses.append(abs(change) if change < 0 else 0)
    avg_gain = np.mean(gains[-period:]) if gains else 0
    avg_loss = np.mean(losses[-period:]) if losses else 0
    rs = avg_gain / avg_loss if avg_loss != 0 else 0
    return 100 - (100 / (1 + rs))

def calculate_bollinger_bands(prices: List[float], period: int, std_dev: float) -> Tuple[float, float, float]:
    if len(prices) < period:
        return prices[-1], prices[-1], prices[-1]
    sma = float(np.mean(prices[-period:]))
    std = float(np.std(prices[-period:]))
    return sma, sma + std_dev * std, sma - std_dev * std

def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    if len(returns) < 2:
        return 0.0
    mean_return = float(np.mean(returns))
    std_return = float(np.std(returns))
    return (mean_return - risk_free_rate) / std_return * np.sqrt(252) if std_return > 0 else 0.0

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        dd = (peak - value) / peak
        max_dd = max(max_dd, dd)
    return max_dd * 100

# ======== PHẦN BỔ SUNG TÍNH TOÁN LƯỚI TỰ ĐỘNG ========
class GridCalculator:
    def __init__(self):
        self.price_precision = 2
        self.quantity_precision = 6
        
    def calculate_volatility(self, price_history: List[float]) -> float:
        if len(price_history) < config.volatility_window:
            return 0.0
        returns = np.diff(price_history) / price_history[:-1]
        return float(np.std(returns) * np.sqrt(252) * 100)

    def calculate_grid_parameters(self, current_price: float, investment: float, price_history: List[float], highs: List[float], lows: List[float], exchange: ExchangeInterface) -> Dict:
        """Tính toán tham số lưới thích ứng dựa trên RSI, ATR và volatility."""
        try:
            # Tính các chỉ báo kỹ thuật
            volatility = self.calculate_volatility(price_history)
            volatility_factor = max(min(volatility / 20, 2.0), 0.5)
            atr = calculate_atr(highs, lows, price_history, config.atr_period)
            rsi = calculate_rsi(price_history, config.rsi_period)

            # Điều chỉnh số mức lưới dựa trên RSI
            if rsi > 70:  # Quá mua
                grid_levels = int(config.min_grid_levels + (config.max_grid_levels - config.min_grid_levels) * 0.2)  # Giảm số mức
                rsi_adjustment = 1.5  # Tăng khoảng cách
            elif rsi < 30:  # Quá bán
                grid_levels = int(config.min_grid_levels + (config.max_grid_levels - config.min_grid_levels) * 0.8)  # Tăng số mức
                rsi_adjustment = 0.5  # Giảm khoảng cách
            else:  # Bình thường
                grid_levels = int(config.min_grid_levels + (config.max_grid_levels - config.min_grid_levels) / volatility_factor)
                rsi_adjustment = 1.0

            grid_levels = max(config.min_grid_levels, min(config.max_grid_levels, grid_levels))

            # Điều chỉnh khoảng cách lưới dựa trên ATR và RSI
            atr_spacing = (atr / current_price * 100) * 1.5  # Hệ số 1.5 để tăng nhạy với biến động
            grid_spacing = max(atr_spacing, config.base_grid_step_percent * volatility_factor) * rsi_adjustment

            # Tính toán số lượng mỗi lệnh
            order_amount = max(investment / grid_levels, 10)
            quantity_per_order = order_amount / current_price
            filters = exchange.get_symbol_filters()
            adjusted_quantity = max(filters['minQty'], math.ceil(quantity_per_order / filters['stepSize']) * filters['stepSize'])

            logging.info(f"RSI: {rsi:.2f}, ATR: {atr:.2f}, Grid Levels: {grid_levels}, Spacing: {grid_spacing:.4f}%")

            return {
                'levels': grid_levels,
                'spacing': grid_spacing,
                'quantity': round(adjusted_quantity, self.quantity_precision),
                'min_price': current_price * (1 - (grid_levels/2) * grid_spacing / 100),
                'max_price': current_price * (1 + (grid_levels/2) * grid_spacing / 100)
            }
        except Exception as e:
            logging.error(f"Lỗi tính toán lưới: {str(e)}")
            return {'levels': 5, 'spacing': 0.005, 'quantity': 0.0002, 'min_price': current_price * 0.95, 'max_price': current_price * 1.05}

    def generate_grid_prices(self, current_price: float, levels: int, spacing: float, exchange: ExchangeInterface) -> List[float]:
        grid_prices = [round(current_price * (1 + (i - levels // 2) * spacing / 100), self.price_precision) for i in range(levels + 1)]
        order_book = safe_api_call(exchange.get_order_book)
        price_volume = {price: sum(float(bid[1]) for bid in order_book['bids'] if abs(float(bid[0]) - price) < 0.01 * price) +
                              sum(float(ask[1]) for ask in order_book['asks'] if abs(float(ask[0]) - price) < 0.01 * price)
                       for price in grid_prices}
        return sorted([price for price, vol in price_volume.items() if vol > config.min_acceptable_volume])

# ======== THEO DÕI LỢI NHUẬN ========
class EnhancedProfitTracker:
    def __init__(self):
        self.initial_balance = config.initial_investment
        self.current_balance = config.initial_investment
        self.trades = []
        self.start_time = datetime.now()
        self.fee_tracker = 0.0
        self.win_count = 0
        self.loss_count = 0
        self.best_entry_price = None
        self.average_buy_price = 0.0
        self.buy_trades = []
        self.total_btc_bought = 0.0
        self.realized_pnl = 0.0
        self.profit_history = []
        self.returns_history = []
        
    def add_trade(self, side: str, price: float, quantity: float, is_maker: bool = True) -> None:
        fee = quantity * price * (config.maker_fee if is_maker else config.taker_fee)
        self.fee_tracker += fee
        trade = {'time': datetime.now(), 'updateTime': int(time.time() * 1000), 'side': side, 'price': price, 'quantity': quantity, 'fee': fee, 'value': quantity * price}
        self.trades.append(trade)
        
        if side == 'BUY':
            self.current_balance -= trade['value'] + fee
            self.buy_trades.append(trade)
            self.total_btc_bought += quantity
            total_buy_value = sum(t['price'] * t['quantity'] for t in self.buy_trades)
            self.average_buy_price = total_buy_value / sum(t['quantity'] for t in self.buy_trades) if self.buy_trades else 0.0
            self.best_entry_price = min(self.best_entry_price or price, price)
            send_telegram_alert(f"✅ Lệnh BUY @ {price} đã khớp, số lượng: {quantity} {config.symbol}")
        else:
            self.current_balance += trade['value'] - fee
            self.total_btc_bought -= quantity
            pnl = self._calculate_trade_pnl(trade)
            self.realized_pnl += pnl
            if pnl > 0:
                self.win_count += 1
            elif pnl < 0:
                self.loss_count += 1
            send_telegram_alert(f"✅ Lệnh SELL @ {price} đã khớp, số lượng: {quantity} {config.symbol} | {'Lợi nhuận' if pnl >= 0 else 'Lỗ'}: {pnl:.2f} USDT")
            if self.total_btc_bought <= 0:
                self.best_entry_price = None
                self.average_buy_price = 0.0
                self.buy_trades = []
                self.total_btc_bought = 0.0
        logging.info(f"Đã ghi lại giao dịch: {trade}")

    def _calculate_trade_pnl(self, sell_trade: Dict) -> float:
        if not self.buy_trades:
            return 0.0
        sell_quantity = sell_trade['quantity']
        sell_price = sell_trade['price']
        sell_fee = sell_trade['fee']
        remaining_quantity = sell_quantity
        trade_pnl = 0.0
        self.buy_trades.sort(key=lambda x: x['updateTime'])
        for buy_trade in self.buy_trades[:]:
            if remaining_quantity <= 0:
                break
            matched_quantity = min(remaining_quantity, buy_trade['quantity'])
            buy_cost = matched_quantity * buy_trade['price'] + buy_trade['fee'] * (matched_quantity / buy_trade['quantity'])
            sell_revenue = matched_quantity * sell_price - sell_fee * (matched_quantity / sell_quantity)
            trade_pnl += sell_revenue - buy_cost
            remaining_quantity -= matched_quantity
            buy_trade['quantity'] -= matched_quantity
            if buy_trade['quantity'] <= 0:
                self.buy_trades.remove(buy_trade)
        return round(trade_pnl, 2)

    def calculate_pnl(self, current_price: float, exchange: Optional[ExchangeInterface]) -> Tuple[float, float, float, float]:
        total_btc = self.total_btc_bought
        usdt_value = total_btc * current_price if total_btc > 0 else 0.0
        realized_pnl = self.current_balance - self.initial_balance
        realized_pnl_percent = (realized_pnl / self.initial_balance) * 100 if self.initial_balance > 0 else 0.0
        unrealized_pnl = (current_price - self.best_entry_price) * total_btc if total_btc > 0 and self.best_entry_price else 0.0
        unrealized_pnl_percent = (unrealized_pnl / self.initial_balance) * 100 if self.initial_balance > 0 else 0.0
        self.profit_history.append(realized_pnl + unrealized_pnl)
        if len(self.profit_history) > 1:
            self.returns_history.append((self.profit_history[-1] - self.profit_history[-2]) / self.initial_balance)
        return realized_pnl, realized_pnl_percent, unrealized_pnl, unrealized_pnl_percent

# ======== HỆ THỐNG BẢO VỆ ========
class EnhancedProtectionSystem:
    def __init__(self):
        self.last_price = 0
        self.highest_price = 0
        self.orders_paused = False
        self.circuit_breaker_activated = False
        self.trailing_stop_activated = [False] * 4
        self.trailing_stop_price = [None] * 4
        self.price_history = []
        self.high_history = []
        self.low_history = []
        self.volume_history = []

    def check_market_conditions(self, current_price: float, current_volume: float, high: float, low: float) -> Optional[str]:
        self.price_history.append(current_price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(current_volume)
        if len(self.price_history) > config.volatility_window:
            self.price_history.pop(0)
            self.high_history.pop(0)
            self.low_history.pop(0)
            self.volume_history.pop(0)
        
        if self.last_price and abs(current_price - self.last_price) / self.last_price > config.circuit_breaker_threshold:
            self.circuit_breaker_activated = True
            self.trigger_protection(f"Circuit breaker: thay đổi {abs(current_price - self.last_price) / self.last_price:.2%}", "🚨")
            return 'circuit_breaker'
        
        self.last_price = current_price
        self.highest_price = max(self.highest_price, current_price)
        return None

    def update_trailing_stop(self, current_price: float) -> Optional[float]:
        if config.trailing_stop_enabled:
            if current_price > self.highest_price:
                self.highest_price = current_price
            trailing_stop_price = self.highest_price * (1 - config.base_stop_loss_percent / 100)
            if current_price <= trailing_stop_price:
                self.trigger_protection(f"Trailing stop kích hoạt @ {trailing_stop_price:.2f}", "🛑")
                return trailing_stop_price
        return None

    def check_pump_protection(self, current_price: float) -> bool:
        if len(self.price_history) >= 5:
            short_term_change = abs(current_price - self.price_history[-5]) / self.price_history[-5]
            if short_term_change > config.pump_protection_threshold:
                self.trigger_protection(f"Pump/Dump phát hiện: {short_term_change:.2%}", "⚠️")
                return True
        return False

    def check_exit_strategy(self, tracker: EnhancedProfitTracker, current_price: float) -> bool:
        realized_pnl, _, unrealized_pnl, _ = tracker.calculate_pnl(current_price, None)
        total_pnl = realized_pnl + unrealized_pnl
        if total_pnl >= config.initial_investment * 0.1:
            self.trigger_protection("Đạt mục tiêu lợi nhuận 10%", "💰")
            return True
        elif total_pnl <= -config.initial_investment * 0.05:
            self.trigger_protection("Cắt lỗ tại -5%", "🛑")
            return True
        return False

    def trigger_protection(self, message: str, emoji: str) -> None:
        send_telegram_alert(f"{emoji} {message}")
        logging.warning(f"{emoji} {message}")

# ======== QUẢN LÝ LƯỚI ========
class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels = []
        self.last_adjustment_time = datetime.now()
        self.grid_calculator = GridCalculator()
        
    def calculate_adaptive_grid(self, current_price: float, price_history: List[float], highs: List[float], lows: List[float], exchange: ExchangeInterface) -> List[float]:
        grid_params = self.grid_calculator.calculate_grid_parameters(current_price, config.initial_investment, price_history, highs, lows, exchange)
        self.grid_levels = self.grid_calculator.generate_grid_prices(current_price, grid_params['levels'], grid_params['spacing'], exchange)
        logging.info(f"Lưới: {grid_params['levels']} mức, khoảng cách {grid_params['spacing']:.2f}%, khối lượng {grid_params['quantity']}")
        return self.grid_levels
    
    def rebalance_grid(self, current_price: float, price_history: List[float], highs: List[float], lows: List[float], exchange: ExchangeInterface) -> None:
        self.calculate_adaptive_grid(current_price, price_history, highs, lows, exchange)
        self.last_adjustment_time = datetime.now()

    def place_grid_orders(self, current_price: float, exchange: ExchangeInterface) -> None:
        open_orders = safe_api_call(exchange.get_open_orders)
        grid_params = self.grid_calculator.calculate_grid_parameters(
            current_price, config.initial_investment, 
            self.grid_levels, self.grid_levels, self.grid_levels, exchange
        )
        quantity = grid_params['quantity']
        
        for price in self.grid_levels:
            if len(open_orders) >= config.max_open_orders:
                break
            side = 'BUY' if price < current_price else 'SELL'
            if any(o['price'] == str(price) and o['side'] == side for o in open_orders):
                continue
            try:
                order = safe_api_call(exchange.place_order, side=side, order_type='LIMIT', quantity=quantity, price=price)
                logging.info(f"Đặt lệnh {side} @ {price}, qty: {quantity}, Order ID: {order['orderId']}")
            except Exception as e:
                logging.error(f"Lỗi đặt lệnh @ {price}: {str(e)}")

# ======== QUẢN LÝ LỆNH ========
class EnhancedOrderManager:
    def __init__(self):
        self.open_orders = []
        self.is_paused = False
        
    def cancel_all_orders(self, exchange: ExchangeInterface) -> None:
        for order in safe_api_call(exchange.get_open_orders):
            safe_api_call(exchange.cancel_order, order['orderId'])
        self.open_orders = []
        logging.info("Đã hủy tất cả lệnh")

    def sync_open_orders(self, exchange: ExchangeInterface) -> None:
        self.open_orders = safe_api_call(exchange.get_open_orders)

# ======== TIỆN ÍCH HỖ TRỢ ========
def send_telegram_alert(message: str) -> None:
    if not config.telegram_enabled:
        return
    try:
        url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
        requests.post(url, json={'chat_id': config.telegram_chat_id, 'text': message}, timeout=5).raise_for_status()
        logging.info(f"Đã gửi Telegram: {message}")
    except Exception as e:
        logging.error(f"Gửi Telegram thất bại: {str(e)}")

def check_account_balance(exchange: ExchangeInterface) -> Tuple[float, float]:
    usdt_balance, btc_balance = safe_api_call(exchange.get_balance)
    if usdt_balance < config.initial_investment:
        raise Exception(f"Số dư USDT không đủ: {usdt_balance} < {config.initial_investment}")
    return usdt_balance, btc_balance

# ======== DỮ LIỆU GIẢ LẬP VÀ BACKTESTING ========
def generate_simulated_data(hours: int = 100, initial_price: float = 60000.0, volatility: float = 0.01) -> pd.DataFrame:
    timestamps = [datetime.now() - timedelta(hours=hours - i) for i in range(hours)]
    prices = [initial_price]
    volatility_factor = volatility
    atr_factor = 0.005

    for i in range(1, hours):
        change = np.random.normal(0, volatility_factor * prices[-1])
        new_price = prices[-1] + change
        prices.append(max(new_price, 1000))

    data = {
        'timestamp': [int(t.timestamp() * 1000) for t in timestamps],
        'open': [prices[i] for i in range(hours)],
        'high': [p + abs(np.random.normal(0, p * atr_factor)) for p in prices],
        'low': [p - abs(np.random.normal(0, p * atr_factor)) for p in prices],
        'close': prices,
        'volume': [abs(np.random.normal(1000, 500)) * (1 + abs(p - prices[i-1]) / prices[i-1] if i > 0 else 1) for i, p in enumerate(prices)]
    }
    return pd.DataFrame(data)

def backtest_strategy(exchange: ExchangeInterface, historical_data: pd.DataFrame) -> Dict:
    tracker = EnhancedProfitTracker()
    grid_manager = EnhancedSmartGridManager()
    protection = EnhancedProtectionSystem()
    
    for idx, row in historical_data.iterrows():
        current_price = row['close']
        bid_price = current_price * (1 - np.random.uniform(0, 0.0005))
        ask_price = current_price * (1 + np.random.uniform(0, 0.0005))
        
        grid_manager.calculate_adaptive_grid(current_price, historical_data['close'][:idx].tolist(), 
                                            historical_data['high'][:idx].tolist(), historical_data['low'][:idx].tolist(), exchange)
        for price in grid_manager.grid_levels:
            if price < bid_price and tracker.current_balance > 10:
                tracker.add_trade("BUY", ask_price, config.min_quantity, is_maker=True)
            elif price > ask_price and tracker.total_btc_bought > 0:
                tracker.add_trade("SELL", bid_price, config.min_quantity, is_maker=True)
        
        if protection.check_exit_strategy(tracker, current_price):
            break
    
    realized_pnl, _, unrealized_pnl, _ = tracker.calculate_pnl(historical_data['close'].iloc[-1], exchange)
    sharpe = calculate_sharpe_ratio(tracker.returns_history)
    max_dd = calculate_max_drawdown(tracker.profit_history)
    return {
        "PnL": realized_pnl + unrealized_pnl,
        "Trades": len(tracker.trades),
        "Sharpe Ratio": sharpe,
        "Max Drawdown": max_dd,
        "Total Fees": tracker.fee_tracker
    }

def run_simulated_backtest(exchange: ExchangeInterface):
    simulated_data = generate_simulated_data(hours=100, initial_price=60000.0)
    backtest_result = backtest_strategy(exchange, simulated_data)
    
    logging.info(f"Backtest giả lập trên {exchange.exchange_name}:")
    result_msg = (
        f"PnL: {backtest_result['PnL']:.2f} USDT\n"
        f"Số giao dịch: {backtest_result['Trades']}\n"
        f"Sharpe Ratio: {backtest_result['Sharpe Ratio']:.2f}\n"
        f"Max Drawdown: {backtest_result['Max Drawdown']:.2f}%\n"
        f"Total Fees: {backtest_result['Total Fees']:.2f} USDT"
    )
    logging.info(result_msg)
    send_telegram_alert(f"📈 Backtest giả lập trên {exchange.exchange_name}:\n{result_msg}")
    return backtest_result

# ======== BOT CHÍNH ========
def update_market_data(state: BotState, exchange: ExchangeInterface) -> Tuple[float, float, float, float]:
    if exchange.websocket_connected:
        current_price = exchange.latest_price
        current_volume = safe_api_call(exchange.get_volume)
        high, low = safe_api_call(exchange.get_high_low)
    else:
        current_price = safe_api_call(exchange.get_price)
        current_volume = safe_api_call(exchange.get_volume)
        high, low = safe_api_call(exchange.get_high_low)
        logging.warning(f"WebSocket {exchange.exchange_name} không khả dụng, dùng polling làm fallback")
    
    state.current_price = current_price
    return current_price, current_volume, high, low

def handle_orders(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    state.order_manager.sync_open_orders(exchange)
    executed_orders = safe_api_call(exchange.get_all_orders)
    for order in executed_orders:
        if order['status'] == 'FILLED' and not any(t['updateTime'] == order['updateTime'] for t in state.tracker.trades):
            state.tracker.add_trade(order['side'], float(order['price']), float(order['executedQty']))
    
    if len(state.order_manager.open_orders) < config.max_open_orders:
        state.grid_manager.place_grid_orders(current_price, exchange)

def check_protections(state: BotState, exchange: ExchangeInterface, current_price: float, current_volume: float, high: float, low: float) -> bool:
    market_status = state.protection.check_market_conditions(current_price, current_volume, high, low)
    if market_status == 'circuit_breaker':
        state.order_manager.cancel_all_orders(exchange)
        logging.warning(f"🛑 Bot trên {state.exchange_name} dừng do Circuit Breaker")
        send_telegram_alert(f"🛑 Bot trên {state.exchange_name} dừng do Circuit Breaker")
        return True
    
    if state.protection.check_pump_protection(current_price):
        state.order_manager.cancel_all_orders(exchange)
        state.order_manager.is_paused = True
        logging.warning(f"⏸ Bot trên {state.exchange_name} tạm dừng do Pump/Dump")
        send_telegram_alert(f"⏸ Bot trên {state.exchange_name} tạm dừng do Pump/Dump")
        return True
    
    trailing_stop_price = state.protection.update_trailing_stop(current_price)
    if trailing_stop_price:
        state.order_manager.cancel_all_orders(exchange)
        if state.tracker.total_btc_bought > 0:
            safe_api_call(exchange.place_order, side='SELL', order_type='MARKET', quantity=state.tracker.total_btc_bought)
        logging.warning(f"🛑 Bot trên {state.exchange_name} dừng do Trailing Stop")
        send_telegram_alert(f"🛑 Bot trên {state.exchange_name} dừng do Trailing Stop")
        return True
    
    if state.protection.check_exit_strategy(state.tracker, current_price):
        state.order_manager.cancel_all_orders(exchange)
        if state.tracker.total_btc_bought > 0:
            safe_api_call(exchange.place_order, side='SELL', order_type='MARKET', quantity=state.tracker.total_btc_bought)
        logging.info(f"🏁 Bot trên {state.exchange_name} thoát lệnh")
        send_telegram_alert(f"🏁 Bot trên {state.exchange_name} thoát lệnh")
        return True
    
    return False

def update_status(state: BotState, exchange: ExchangeInterface, last_status_update: float) -> float:
    if time.time() - last_status_update >= config.status_update_interval:
        realized_pnl, realized_pnl_percent, unrealized_pnl, unrealized_pnl_percent = state.tracker.calculate_pnl(state.current_price, exchange)
        usdt_balance, btc_balance = safe_api_call(exchange.get_balance)
        sharpe = calculate_sharpe_ratio(state.tracker.returns_history)
        max_dd = calculate_max_drawdown(state.tracker.profit_history)
        status_msg = (
            f"📊 Trạng thái {state.exchange_name}:\n"
            f"- Giá: {state.current_price:.2f} USDT\n"
            f"- Số dư: {usdt_balance:.2f} USDT | {btc_balance:.6f} BTC\n"
            f"- PnL thực tế: {realized_pnl:.2f} USD ({realized_pnl_percent:.2f}%)\n"
            f"- PnL tiềm năng: {unrealized_pnl:.2f} USD ({unrealized_pnl_percent:.2f}%)\n"
            f"- Sharpe Ratio: {sharpe:.2f}\n"
            f"- Max Drawdown: {max_dd:.2f}%"
        )
        logging.info(status_msg)
        send_telegram_alert(status_msg)
        return time.time()
    return last_status_update

def rebalance_grid_if_needed(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    if (datetime.now() - state.grid_manager.last_adjustment_time).total_seconds() >= config.grid_rebalance_interval:
        state.order_manager.cancel_all_orders(exchange)
        state.grid_manager.rebalance_grid(current_price, state.protection.price_history, state.protection.high_history, state.protection.low_history, exchange)
        state.grid_manager.place_grid_orders(current_price, exchange)
        logging.info(f"🔄 Đã tái cân bằng lưới trên {state.exchange_name}")
        send_telegram_alert(f"🔄 Đã tái cân bằng lưới trên {state.exchange_name}")

def handle_exchange_downtime(state: BotState, downtime_start: Optional[datetime]) -> Tuple[bool, Optional[datetime]]:
    if downtime_start and (datetime.now() - downtime_start).total_seconds() > 1800:
        logging.info(f"🔄 Bot trên {state.exchange_name} thoát Safe Mode")
        send_telegram_alert(f"🔄 Bot trên {state.exchange_name} thoát Safe Mode")
        return False, None
    if not downtime_start:
        downtime_start = datetime.now()
        state.order_manager.is_paused = True
        logging.warning(f"🛡️ Bot trên {state.exchange_name} chuyển sang Safe Mode do sàn không khả dụng")
        send_telegram_alert(f"🛡️ Bot trên {state.exchange_name} chuyển sang Safe Mode do sàn không khả dụng")
    time.sleep(60)
    return True, downtime_start

def run_bot_for_exchange(exchange: ExchangeInterface, states: Dict[str, BotState]) -> None:
    state = BotState(exchange.exchange_name)
    states[exchange.exchange_name] = state
    
    logging.info(f"🤖 Bot khởi động trên {exchange.exchange_name.upper()}")
    state.order_manager.cancel_all_orders(exchange)
    usdt_balance, btc_balance = check_account_balance(exchange)
    state.current_price = safe_api_call(exchange.get_price)
    state.grid_manager.calculate_adaptive_grid(state.current_price, state.protection.price_history, state.protection.high_history, state.protection.low_history, exchange)
    state.grid_manager.place_grid_orders(state.current_price, exchange)
    
    if config.websocket_enabled:
        async def run_websocket():
            await exchange.start_websocket(state)
        
        loop = asyncio.new_event_loop()
        websocket_thread = Thread(target=lambda: loop.run_until_complete(run_websocket()), daemon=True)
        websocket_thread.start()
        logging.info(f"Thread WebSocket khởi động cho {exchange.exchange_name}")
    
    last_status_update = time.time()
    safe_mode = False
    downtime_start = None
    
    try:
        while True:
            if state.order_manager.is_paused and not safe_mode:
                time.sleep(60)
                state.order_manager.is_paused = False
                continue
            
            try:
                current_price, current_volume, high, low = update_market_data(state, exchange)
                safe_mode, downtime_start = False, None
            except ccxt.ExchangeNotAvailable:
                safe_mode, downtime_start = handle_exchange_downtime(state, downtime_start)
                continue
            
            if safe_mode:
                continue
            
            if check_protections(state, exchange, current_price, current_volume, high, low):
                break
            
            handle_orders(state, exchange, current_price)
            rebalance_grid_if_needed(state, exchange, current_price)
            last_status_update = update_status(state, exchange, last_status_update)
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info(f"⏹ Bot trên {exchange.exchange_name} dừng bởi người dùng")
        send_telegram_alert(f"⏹ Bot trên {exchange.exchange_name} dừng bởi người dùng")
        state.order_manager.cancel_all_orders(exchange)
    except Exception as e:
        logging.error(f"🚨 Lỗi nghiêm trọng trên {exchange.exchange_name}: {str(e)}")
        send_telegram_alert(f"🚨 Lỗi nghiêm trọng trên {exchange.exchange_name}: {str(e)}")
        state.order_manager.cancel_all_orders(exchange)
    finally:
        if config.websocket_enabled:
            asyncio.run_coroutine_threadsafe(exchange.client.close(), loop).result()
        logging.info(f"Bot trên {exchange.exchange_name} đã kết thúc")

# ======== GIAO DIỆN WEB DASHBOARD ========
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Grid Trading Bot Dashboard"),
    dcc.Dropdown(id='exchange-selector', options=[{'label': ex, 'value': ex} for ex in config.enabled_exchanges], value=config.enabled_exchanges[0]),
    dcc.Graph(id='pnl-chart'),
    dcc.Graph(id='price-chart'),
    dcc.Interval(id='refresh', interval=60*1000)
])

@app.callback(
    [Output('pnl-chart', 'figure'), Output('price-chart', 'figure')],
    [Input('refresh', 'n_intervals'), Input('exchange-selector', 'value')]
)
def update_dashboard(n, selected_exchange):
    state = bot_states.get(selected_exchange)
    if not state:
        return go.Figure(), go.Figure()
    
    pnl_fig = go.Figure(
        data=[go.Scatter(y=state.tracker.profit_history, mode='lines', name='PnL')],
        layout=go.Layout(title=f'PnL History - {selected_exchange}')
    )
    price_fig = go.Figure(
        data=[go.Scatter(y=state.protection.price_history, mode='lines', name='Price')],
        layout=go.Layout(title=f'Price History - {selected_exchange}')
    )
    return pnl_fig, price_fig

# ======== CHẠY BOT ĐA SÀN GIAO DỊCH ========
bot_states: Dict[str, BotState] = {}

def main():
    exchange_manager = ExchangeManager()
    
    for exchange_name, exchange in exchange_manager.exchanges.items():
        run_simulated_backtest(exchange)
    
    for exchange_name, exchange in exchange_manager.exchanges.items():
        historical_data = safe_api_call(exchange.fetch_historical_data, timeframe='1h', limit=100)
        backtest_result = backtest_strategy(exchange, historical_data)
        logging.info(f"Backtest thực trên {exchange_name}: {backtest_result}")
        send_telegram_alert(f"📈 Backtest thực trên {exchange_name}: PnL={backtest_result['PnL']:.2f}, Trades={backtest_result['Trades']}, Sharpe={backtest_result['Sharpe Ratio']:.2f}, MaxDD={backtest_result['Max Drawdown']:.2f}%, Fees={backtest_result['Total Fees']:.2f}")
    
    cpu_usage = psutil.cpu_percent(interval=1)
    max_workers = min(len(exchange_manager.exchanges), max(1, int(psutil.cpu_count() * (1 - cpu_usage / 100))))
    logging.info(f"Khởi động bot với {max_workers} workers dựa trên CPU usage: {cpu_usage}%")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda ex: run_bot_for_exchange(ex, bot_states), exchange_manager.exchanges.values())
    
    app.run_server(debug=False, host='0.0.0.0', port=8050)

if __name__ == "__main__":
    main()
