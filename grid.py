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

# ======== INITIAL SETUP ========
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

# ======== SECURITY (Removed encryption to avoid InvalidToken) ========
def get_encrypted_credentials(exchange: str) -> Tuple[str, str]:
    api_key = os.getenv(f"{exchange.upper()}_API_KEY", "your_api_key")
    api_secret = os.getenv(f"{exchange.upper()}_API_SECRET", "your_api_secret")
    return (api_key, api_secret)

# ======== ADVANCED CONFIG (Fixed min_quantity) ========
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges = os.getenv("ENABLED_EXCHANGES", "binance").split(",")
        self.exchange_credentials = {ex: get_encrypted_credentials(ex) for ex in self.enabled_exchanges}
        self.symbol = os.getenv("TRADING_SYMBOL", "BTCUSDT")
        self.initial_investment = float(os.getenv("INITIAL_INVESTMENT", 100))
        self.min_quantity = 0.0005  # Fixed to match BTCUSDT LOT_SIZE on Testnet
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

# ======== TELEGRAM ALERTS ========
def send_telegram_alert(message: str) -> None:
    if not config.telegram_enabled:
        return
    try:
        url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": config.telegram_chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logging.info(f"Telegram alert sent: {message}")
    except Exception as e:
        logging.error(f"Failed to send Telegram alert: {str(e)}")

# ======== ENHANCED CLASSES ========
class EnhancedProfitTracker:
    def __init__(self):
        self.total_profit = 0.0
        self.trade_count = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.equity_curve = []
        self.total_fees = 0.0

    def record_trade(self, profit: float, fee: float) -> None:
        self.total_profit += profit
        self.trade_count += 1
        self.total_fees += fee
        if profit > 0:
            self.winning_trades += 1
        elif profit < 0:
            self.losing_trades += 1
        self.equity_curve.append(self.total_profit)

    def get_stats(self) -> Dict:
        win_rate = (self.winning_trades / self.trade_count * 100) if self.trade_count > 0 else 0.0
        max_drawdown = calculate_max_drawdown(self.equity_curve)
        returns = [self.equity_curve[i] - self.equity_curve[i-1] for i in range(1, len(self.equity_curve))] if len(self.equity_curve) > 1 else []
        sharpe_ratio = calculate_sharpe_ratio(returns)
        return {
            "Total Profit": self.total_profit,
            "Trade Count": self.trade_count,
            "Win Rate": win_rate,
            "Max Drawdown": max_drawdown,
            "Sharpe Ratio": sharpe_ratio,
            "Total Fees": self.total_fees
        }

class EnhancedProtectionSystem:
    def __init__(self):
        self.price_history = []
        self.high_history = []
        self.low_history = []
        self.volume_history = []
        self.circuit_breaker_triggered = False
        self.circuit_breaker_start = None
        self.trailing_stop_price = 0.0
        self.trailing_buy_price = 0.0

    def update(self, price: float, high: float, low: float, volume: float) -> None:
        self.price_history.append(price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(volume)
        if len(self.price_history) > config.volatility_window:
            self.price_history.pop(0)
            self.high_history.pop(0)
            self.low_history.pop(0)
            self.volume_history.pop(0)

    def check_pump_protection(self, price: float) -> bool:
        if len(self.price_history) < 2:
            return False
        price_change = (price - self.price_history[-2]) / self.price_history[-2]
        return price_change > config.pump_protection_threshold

    def check_circuit_breaker(self, price: float) -> bool:
        if len(self.price_history) < 2:
            return False
        price_change = abs(price - self.price_history[-2]) / self.price_history[-2]
        if price_change > config.circuit_breaker_threshold:
            self.circuit_breaker_triggered = True
            self.circuit_breaker_start = time.time()
            logging.warning(f"Circuit breaker triggered: Price change {price_change:.2%}")
            send_telegram_alert(f"⚠ Circuit breaker triggered: Price change {price_change:.2%}")
            return True
        return False

    def check_circuit_breaker_status(self) -> bool:
        if self.circuit_breaker_triggered:
            elapsed = time.time() - self.circuit_breaker_start
            if elapsed > config.circuit_breaker_duration:
                self.circuit_breaker_triggered = False
                self.circuit_breaker_start = None
                logging.info("Circuit breaker reset")
                send_telegram_alert("✅ Circuit breaker reset")
                return False
            return True
        return False

    def check_abnormal_activity(self, volume: float) -> bool:
        if len(self.volume_history) < 2:
            return False
        avg_volume = np.mean(self.volume_history[:-1])
        return volume > avg_volume * config.abnormal_activity_threshold

    def update_trailing_stop(self, price: float) -> Tuple[bool, float]:
        if not config.trailing_stop_enabled:
            return False, self.trailing_stop_price
        if price > self.trailing_stop_price and price > config.trailing_up_activation:
            self.trailing_stop_price = price * (1 - config.base_stop_loss_percent / 100)
        elif price < self.trailing_stop_price and price < config.trailing_down_activation:
            self.trailing_stop_price = price * (1 + config.base_stop_loss_percent / 100)
        should_stop = price <= self.trailing_stop_price
        return should_stop, self.trailing_stop_price

    def update_trailing_buy(self, price: float) -> Tuple[bool, float]:
        if not config.trailing_buy_stop_enabled:
            return False, self.trailing_buy_price
        if price < self.trailing_buy_price or self.trailing_buy_price == 0:
            self.trailing_buy_price = price * (1 + config.trailing_buy_activation_percent / 100)
        should_buy = price >= self.trailing_buy_price * (1 + config.trailing_buy_distance_percent / 100)
        return should_buy, self.trailing_buy_price

class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels = []
        self.last_rebalance = time.time()
        self.order_ids = []

    def calculate_adaptive_grid(self, current_price: float, price_history: List[float], high_history: List[float], low_history: List[float], exchange: 'ExchangeInterface') -> None:
        if not config.adaptive_grid_enabled or len(price_history) < config.volatility_window:
            self.grid_levels = self._create_static_grid(current_price)
            return

        atr = calculate_atr(high_history, low_history, price_history, config.atr_period)
        volatility = atr / current_price if current_price != 0 else 0.0
        num_levels = max(config.min_grid_levels, min(config.max_grid_levels, int(volatility / config.base_grid_step_percent)))
        step_percent = volatility / num_levels if num_levels > 0 else config.base_grid_step_percent

        self.grid_levels = []
        for i in range(num_levels):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            self.grid_levels.append((buy_price, sell_price))

        logging.info(f"Adaptive grid calculated: {num_levels} levels, step {step_percent:.2%}")

    def _create_static_grid(self, current_price: float) -> List[Tuple[float, float]]:
        grid = []
        for i in range(config.min_grid_levels):
            buy_price = current_price * (1 - config.base_grid_step_percent * (i + 1))
            sell_price = current_price * (1 + config.base_grid_step_percent * (i + 1))
            grid.append((buy_price, sell_price))
        return grid

    async def place_grid_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        await self.cancel_all_orders(exchange)
        filters = await exchange.get_symbol_filters()
        quantity = max(filters['minQty'], config.initial_investment / current_price / len(self.grid_levels))
        quantity = min(quantity, filters['maxQty'])
        quantity = round(quantity - (quantity % filters['stepSize']), exchange.quantity_precision)

        for buy_price, sell_price in self.grid_levels:
            if quantity < filters['minQty']:
                logging.warning(f"Quantity {quantity} below minimum {filters['minQty']}, skipping order")
                continue
            try:
                if buy_price < current_price:
                    buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price)
                    self.order_ids.append(buy_order['id'])
                    logging.info(f"Placed BUY order at {buy_price:.2f}, qty: {quantity}")
                if sell_price > current_price:
                    sell_order = await safe_api_call(exchange.place_order, side='SELL', order_type='LIMIT', quantity=quantity, price=sell_price)
                    self.order_ids.append(sell_order['id'])
                    logging.info(f"Placed SELL order at {sell_price:.2f}, qty: {quantity}")
            except Exception as e:
                logging.error(f"Failed to place grid order: {str(e)}")

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        for order_id in self.order_ids:
            try:
                await safe_api_call(exchange.cancel_order, order_id)
            except Exception as e:
                logging.warning(f"Failed to cancel order {order_id}: {str(e)}")
        self.order_ids.clear()

class EnhancedOrderManager:
    def __init__(self):
        self.open_orders = []
        self.is_paused = False

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        try:
            open_orders = await safe_api_call(exchange.get_open_orders)
            for order in open_orders:
                await safe_api_call(exchange.cancel_order, order['orderId'])
            self.open_orders.clear()
            logging.info("All open orders cancelled")
        except Exception as e:
            logging.error(f"Failed to cancel all orders: {str(e)}")

    async def check_and_handle_orders(self, current_price: float, exchange: 'ExchangeInterface', profit_tracker: 'EnhancedProfitTracker') -> None:
        try:
            orders = await safe_api_call(exchange.get_all_orders)
            for order in orders:
                if order['status'] == 'FILLED':
                    side = order['side']
                    price = float(order['price'])
                    qty = float(order['executedQty'])
                    fee = qty * price * (config.maker_fee if side == 'BUY' else config.taker_fee)
                    profit = (current_price - price) * qty if side == 'BUY' else (price - current_price) * qty
                    profit_tracker.record_trade(profit, fee)
                    logging.info(f"Order filled: {side} at {price:.2f}, profit: {profit:.2f}, fee: {fee:.2f}")
                    send_telegram_alert(f"✅ Order filled: {side} at {price:.2f}, profit: {profit:.2f}, fee: {fee:.2f}")
        except Exception as e:
            logging.error(f"Error checking orders: {str(e)}")

# ======== BOT STATE ========
class BotState:
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.tracker = EnhancedProfitTracker()
        self.protection = EnhancedProtectionSystem()
        self.current_price = 0.0
        self.grid_manager = EnhancedSmartGridManager()
        self.order_manager = EnhancedOrderManager()

# ======== EXCHANGE INTERFACE ABSTRACTION ========
class ExchangeInterface:
    def __init__(self, exchange_name: str, api_key: str, api_secret: str):
        self-exchange_name = exchange_name
        self.symbol = config.symbol
        self.price_precision = 2
        self.quantity_precision = 6
        self.websocket_connected = False
        self.latest_price = 0.0
        self.latest_volume = 0.0
        self.latest_high = 0.0
        self.latest_low = 0.0

    async def get_price(self) -> float:
        raise NotImplementedError

    async def get_volume(self) -> float:
        raise NotImplementedError

    async def get_high_low(self) -> Tuple[float, float]:
        raise NotImplementedError

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        raise NotImplementedError

    async def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    async def get_open_orders(self) -> List[Dict]:
        raise NotImplementedError

    async def get_all_orders(self) -> List[Dict]:
        raise NotImplementedError

    async def get_balance(self) -> Tuple[float, float]:
        raise NotImplementedError

    async def get_order_book(self) -> Dict:
        raise NotImplementedError

    async def get_symbol_filters(self) -> Dict:
        raise NotImplementedError

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        raise NotImplementedError

    async def start_websocket(self, state: BotState):
        raise NotImplementedError

class BinanceExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("binance", api_key, api_secret)
        self.client = ccxt_async.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'testnet': True  # Ensure Testnet is enabled
        })
        self.ws_client = None

    async def get_price(self) -> float:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['last'])
        except Exception as e:
            logging.error(f"Error fetching price: {str(e)}")
            raise

    async def get_volume(self) -> float:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['quoteVolume'])
        except Exception as e:
            logging.error(f"Error fetching volume: {str(e)}")
            raise

    async def get_high_low(self) -> Tuple[float, float]:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['high']), float(ticker['low'])
        except Exception as e:
            logging.error(f"Error fetching high/low: {str(e)}")
            raise

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        try:
            order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
            return await self.client.create_order(
                symbol=self.symbol,
                side=side.lower(),
                type=order_type,
                amount=round(quantity, self.quantity_precision),
                price=round(price, self.price_precision) if price else None
            )
        except Exception as e:
            logging.error(f"Error placing order: {str(e)}")
            raise

    async def cancel_order(self, order_id: str) -> None:
        try:
            await self.client.cancel_order(order_id, self.symbol)
        except Exception as e:
            logging.error(f"Error cancelling order: {str(e)}")
            raise

    async def get_open_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_open_orders(self.symbol)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching open orders: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching all orders: {str(e)}")
            raise

    async def get_balance(self) -> Tuple[float, float]:
        try:
            balance = await self.client.fetch_balance()
            usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
            base_asset = self.symbol.split('USDT')[0]
            btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
            return usdt_balance, btc_balance
        except Exception as e:
            logging.error(f"Error fetching balance: {str(e)}")
            raise

    async def get_order_book(self) -> Dict:
        try:
            return await self.client.fetch_order_book(self.symbol, limit=50)
        except Exception as e:
            logging.error(f"Error fetching order book: {str(e)}")
            raise

    async def get_symbol_filters(self) -> Dict:
        try:
            markets = await self.client.load_markets()
            if self.symbol not in markets:
                logging.error(f"Symbol {self.symbol} not found in markets")
                return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}
                
            market = markets[self.symbol]
            logging.info(f"Successfully fetched LOT_SIZE for {self.symbol}: {market['limits']}")
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Error getting LOT_SIZE info from Binance: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            ohlcv = await self.client.fetch_ohAngela DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"No historical data returned for {self.symbol} on {self.exchange_name}")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            logging.info(f"Fetched {len(df)} rows of historical data for {self.symbol}")
            return df
        except Exception as e:
            logging.error(f"Error fetching historical data: {str(e)}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    async def start_websocket(self, state: BotState):
        if not config.websocket_enabled:
            return
            
        try:
            await self.client.load_markets()
        except Exception as e:
            logging.error(f"Error loading markets in WebSocket: {str(e)}")
            self.websocket_connected = False
            return
            
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                state.current_price = float(ticker['last'])
                self.latest_price = state.current_price
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Price update - {state.current_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} error: {str(e)}, retrying in {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

class MEXCExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("mexc", api_key, api_secret)
        self.client = ccxt_async.mexc({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        self.symbol = self.symbol.replace("USDT", "/USDT")

    async def get_price(self) -> float:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['last'])
        except Exception as e:
            logging.error(f"Error fetching price: {str(e)}")
            raise

    async def get_volume(self) -> float:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['quoteVolume'])
        except Exception as e:
            logging.error(f"Error fetching volume: {str(e)}")
            raise

    async def get_high_low(self) -> Tuple[float, float]:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['high']), float(ticker['low'])
        except Exception as e:
            logging.error(f"Error fetching high/low: {str(e)}")
            raise

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        try:
            order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
            return await self.client.create_order(
                symbol=self.symbol,
                side=side.lower(),
                type=order_type,
                amount=round(quantity, self.quantity_precision),
                price=round(price, self.price_precision) if price else None
            )
        except Exception as e:
            logging.error(f"Error placing order: {str(e)}")
            raise

    async def cancel_order(self, order_id: str) -> None:
        try:
            await self.client.cancel_order(order_id, self.symbol)
        except Exception as e:
            logging.error(f"Error cancelling order: {str(e)}")
            raise

    async def get_open_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_open_orders(self.symbol)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching open orders: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching all orders: {str(e)}")
            raise

    async def get_balance(self) -> Tuple[float, float]:
        try:
            balance = await self.client.fetch_balance()
            usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
            base_asset = self.symbol.split('/')[0]
            btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
            return usdt_balance, btc_balance
        except Exception as e:
            logging.error(f"Error fetching balance: {str(e)}")
            raise

    async def get_order_book(self) -> Dict:
        try:
            return await self.client.fetch_order_book(self.symbol, limit=50)
        except Exception as e:
            logging.error(f"Error fetching order book: {str(e)}")
            raise

    async def get_symbol_filters(self) -> Dict:
        try:
            markets = await self.client.load_markets()
            market = markets[self.symbol]
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Error getting LOT_SIZE info from MEXC: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            ohlcv = await self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"No historical data returned for {self.symbol} on {self.exchange_name}")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            logging.info(f"Fetched {len(df)} rows of historical data for {self.symbol}")
            return df
        except Exception as e:
            logging.error(f"Error fetching historical data: {str(e)}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    async def start_websocket(self, state: BotState):
        if not config.websocket_enabled:
            return
            
        try:
            await self.client.load_markets()
        except Exception as e:
            logging.error(f"Error loading markets: {str(e)}")
            return
            
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                state.current_price = float(ticker['last'])
                self.latest_price = state.current_price
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Price update - {state.current_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} error: {str(e)}, retrying in {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

# ======== MULTI-EXCHANGE MANAGEMENT ========
class ExchangeManager:
    def __init__(self):
        self.exchanges = {}
        for exchange_name in config.enabled_exchanges:
            api_key, api_secret = config.exchange_credentials[exchange_name]
            if exchange_name == "binance":
                self.exchanges[exchange_name] = BinanceExchange(api_key, api_secret)
            elif exchange_name == "mexc":
                self.exchanges[exchange_name] = MEXCExchange(api_key, api_secret)
            else:
                logging.error(f"Exchange {exchange_name} not supported!")
                raise ValueError(f"Unsupported exchange: {exchange_name}")
            logging.info(f"Initialized exchange {exchange_name}")

    async def check_price_disparity(self) -> None:
        prices = {}
        for name, ex in self.exchanges.items():
            if ex.websocket_connected:
                prices[name] = ex.latest_price
            else:
                prices[name] = await safe_api_call(ex.get_price)
        max_diff = max(prices.values()) - min(prices.values())
        if max_diff / min(prices.values()) > 0.01:
            logging.warning(f"Large price disparity: {max_diff:.2f} between {prices}")
            send_telegram_alert(f"⚠️ Large price disparity: {max_diff:.2f} between {prices}")

# ======== IMPROVED ERROR HANDLING ========
async def safe_api_call(func, *args, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            cache_key = f"{func.__name__}_{str(args)}_{str(kwargs)}"
            if cache_key in api_cache:
                return api_cache[cache_key]
            result = await func(*args, **kwargs)
            api_cache[cache_key] = result
            return result
        except (ccxt.DDoSProtection, ccxt.RateLimitExceeded) as e:
            logging.warning(f"API rate limited: {str(e)}. Waiting {config.api_retry_delay} seconds")
            await asyncio.sleep(config.api_retry_delay * (2 ** attempt))
        except ccxt.ExchangeNotAvailable as e:
            logging.error(f"Exchange unavailable: {str(e)}. Pausing for 5 minutes")
            send_telegram_alert(f"⚠️ Exchange unavailable: {str(e)}")
            await asyncio.sleep(300)
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            if attempt == max_retries - 1:
                logging.error(f"API call failed after {max_retries} attempts: {str(e)}")
                raise
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Unknown error: {str(e)}")
            raise

# ======== TECHNICAL INDICATOR CALCULATIONS ========
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
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if peak == 0:  # Fixed ZeroDivisionError
            continue
        peak = max(peak, value)
        dd = (peak - value) / peak if peak != 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd * 100

# ======== SIMULATED BACKTEST ========
async def run_simulated_backtest(exchange: ExchangeInterface) -> Dict:
    logging.info(f"Running simulated backtest on {exchange.exchange_name}")
    historical_data = await safe_api_call(exchange.fetch_historical_data, timeframe='1h', limit=100)
    if historical_data.empty:
        logging.warning(f"No historical data for backtest on {exchange.exchange_name}")
        return {"Total Profit": 0.0, "Trade Count": 0, "Sharpe Ratio": 0.0, "Max Drawdown": 0.0, "Total Fees": 0.0}

    profit_tracker = EnhancedProfitTracker()
    grid_manager = EnhancedSmartGridManager()
    protection = EnhancedProtectionSystem()

    for i in range(len(historical_data)):
        price = historical_data['close'].iloc[i]
        high = historical_data['high'].iloc[i]
        low = historical_data['low'].iloc[i]
        volume = historical_data['volume'].iloc[i]

        protection.update(price, high, low, volume)
        grid_manager.calculate_adaptive_grid(price, protection.price_history, protection.high_history, protection.low_history, exchange)

        # Simulate grid orders
        for buy_price, sell_price in grid_manager.grid_levels:
            if price <= buy_price:
                profit = (sell_price - buy_price) * config.min_quantity
                fee = config.min_quantity * (buy_price + sell_price) * config.maker_fee
                profit_tracker.record_trade(profit, fee)
            elif price >= sell_price:
                profit = (sell_price - buy_price) * config.min_quantity
                fee = config.min_quantity * (buy_price + sell_price) * config.maker_fee
                profit_tracker.record_trade(profit, fee)

    stats = profit_tracker.get_stats()
    logging.info(f"Simulated backtest on {exchange.exchange_name}: {stats}")
    return stats

# ======== REAL BACKTEST ========
async def backtest_strategy(exchange: ExchangeInterface, historical_data: pd.DataFrame) -> Dict:
    logging.info(f"Running real backtest on {exchange.exchange_name}")
    if historical_data.empty:
        logging.warning(f"No historical data for real backtest on {exchange.exchange_name}")
        return {"Total Profit": 0.0, "Trade Count": 0, "Sharpe Ratio": 0.0, "Max Drawdown": 0.0, "Total Fees": 0.0}

    profit_tracker = EnhancedProfitTracker()
    grid_manager = EnhancedSmartGridManager()
    protection = EnhancedProtectionSystem()

    for i in range(len(historical_data)):
        price = historical_data['close'].iloc[i]
        high = historical_data['high'].iloc[i]
        low = historical_data['low'].iloc[i]
        volume = historical_data['volume'].iloc[i]

        protection.update(price, high, low, volume)
        grid_manager.calculate_adaptive_grid(price, protection.price_history, protection.high_history, protection.low_history, exchange)

        # Simulate grid orders with real historical data
        for buy_price, sell_price in grid_manager.grid_levels:
            if price <= buy_price:
                profit = (sell_price - buy_price) * config.min_quantity
                fee = config.min_quantity * (buy_price + sell_price) * config.maker_fee
                profit_tracker.record_trade(profit, fee)
            elif price >= sell_price:
                profit = (sell_price - buy_price) * config.min_quantity
                fee = config.min_quantity * (buy_price + sell_price) * config.maker_fee
                profit_tracker.record_trade(profit, fee)

    stats = profit_tracker.get_stats()
    return stats

# ======== BOT EXECUTION HELPERS ========
async def check_account_balance(exchange: ExchangeInterface) -> Tuple[float, float]:
    usdt_balance, btc_balance = await safe_api_call(exchange.get_balance)
    logging.info(f"Account balance on {exchange.exchange_name}: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC")
    if usdt_balance < config.initial_investment:
        logging.warning(f"Insufficient USDT balance: {usdt_balance:.2f} < {config.initial_investment}")
        send_telegram_alert(f"⚠️ Insufficient USDT balance on {exchange.exchange_name}: {usdt_balance:.2f} < {config.initial_investment}")
    return usdt_balance, btc_balance

async def update_market_data(state: BotState, exchange: ExchangeInterface) -> Tuple[float, float, float, float]:
    if not exchange.websocket_connected:
        state.current_price = await safe_api_call(exchange.get_price)
    current_volume = await safe_api_call(exchange.get_volume)
    high, low = await safe_api_call(exchange.get_high_low)
    state.protection.update(state.current_price, high, low, current_volume)
    return state.current_price, current_volume, high, low

async def handle_exchange_downtime(state: BotState, downtime_start: Optional[float]) -> Tuple[bool, Optional[float]]:
    if downtime_start is None:
        downtime_start = time.time()
        logging.warning(f"Exchange {state.exchange_name} unavailable, entering safe mode")
        send_telegram_alert(f"⚠️ Exchange {state.exchange_name} unavailable, entering safe mode")
    elapsed = time.time() - downtime_start
    if elapsed > 300:  # 5 minutes
        logging.error(f"Exchange {state.exchange_name} unavailable for too long, stopping bot")
        send_telegram_alert(f"❌ Exchange {state.exchange_name} unavailable for too long, stopping bot")
        return True, downtime_start
    return True, downtime_start

async def check_protections(state: BotState, exchange: ExchangeInterface, current_price: float, current_volume: float, high: float, low: float) -> bool:
    if state.protection.check_pump_protection(current_price):
        logging.warning(f"Pump protection triggered on {state.exchange_name}")
        send_telegram_alert(f"⚠️ Pump protection triggered on {state.exchange_name}")
        state.order_manager.is_paused = True
        return False

    if state.protection.check_circuit_breaker(current_price) or state.protection.check_circuit_breaker_status():
        await state.order_manager.cancel_all_orders(exchange)
        return True

    if state.protection.check_abnormal_activity(current_volume):
        logging.warning(f"Abnormal activity detected on {state.exchange_name}: Volume {current_volume}")
        send_telegram_alert(f"⚠️ Abnormal activity detected on {state.exchange_name}: Volume {current_volume}")
        state.order_manager.is_paused = True
        return False

    should_stop, trailing_stop_price = state.protection.update_trailing_stop(current_price)
    if should_stop:
        logging.info(f"Trailing stop triggered on {state.exchange_name} at {trailing_stop_price:.2f}")
        send_telegram_alert(f"✅ Trailing stop triggered on {state.exchange_name} at {trailing_stop_price:.2f}")
        await state.order_manager.cancel_all_orders(exchange)
        return True

    should_buy, trailing_buy_price = state.protection.update_trailing_buy(current_price)
    if should_buy:
        logging.info(f"Trailing buy triggered on {state.exchange_name} at {trailing_buy_price:.2f}")
        send_telegram_alert(f"✅ Trailing buy triggered on {state.exchange_name} at {trailing_buy_price:.2f}")
        await state.grid_manager.place_grid_orders(current_price, exchange)

    return False

async def handle_orders(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    await state.order_manager.check_and_handle_orders(current_price, exchange, state.tracker)

async def rebalance_grid_if_needed(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    if time.time() - state.grid_manager.last_rebalance > config.grid_rebalance_interval:
        state.grid_manager.calculate_adaptive_grid(
            current_price,
            state.protection.price_history,
            state.protection.high_history,
            state.protection.low_history,
            exchange
        )
        await state.grid_manager.place_grid_orders(current_price, exchange)
        state.grid_manager.last_rebalance = time.time()
        logging.info(f"Grid rebalanced on {state.exchange_name}")

async def update_status(state: BotState, exchange: ExchangeInterface, last_status_update: float) -> float:
    if time.time() - last_status_update > config.status_update_interval:
        stats = state.tracker.get_stats()
        usdt_balance, btc_balance = await safe_api_call(exchange.get_balance)
        logging.info(f"Status update on {state.exchange_name}: {stats}, Balance: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC")
        send_telegram_alert(
            f"📊 Status update on {state.exchange_name}:\n"
            f"Total Profit: {stats['Total Profit']:.2f}\n"
            f"Trades: {stats['Trade Count']}\n"
            f"Win Rate: {stats['Win Rate']:.2f}%\n"
            f"Max Drawdown: {stats['Max Drawdown']:.2f}%\n"
            f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}\n"
            f"Total Fees: {stats['Total Fees']:.2f}\n"
            f"Balance: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC"
        )
        last_status_update = time.time()
    return last_status_update

# ======== MAIN BOT EXECUTION ========
async def run_bot_for_exchange(exchange: ExchangeInterface, states: Dict[str, BotState]) -> None:
    state = BotState(exchange.exchange_name)
    states[exchange.exchange_name] = state
    
    logging.info(f"🤖 Bot starting on {exchange.exchange_name.upper()}")
    await state.order_manager.cancel_all_orders(exchange)
    usdt_balance, btc_balance = await check_account_balance(exchange)
    state.current_price = await safe_api_call(exchange.get_price)
    state.grid_manager.calculate_adaptive_grid(state.current_price, state.protection.price_history, state.protection.high_history, state.protection.low_history, exchange)
    await state.grid_manager.place_grid_orders(state.current_price, exchange)
    
    if config.websocket_enabled:
        async def run_websocket():
            await exchange.start_websocket(state)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        websocket_thread = Thread(target=lambda: loop.run_until_complete(run_websocket()), daemon=True)
        websocket_thread.start()
        logging.info(f"WebSocket thread started for {exchange.exchange_name}")
    
    last_status_update = time.time()
    safe_mode = False
    downtime_start = None
    
    try:
        while True:
            if state.order_manager.is_paused and not safe_mode:
                await asyncio.sleep(60)
                state.order_manager.is_paused = False
                continue
            
            try:
                current_price, current_volume, high, low = await update_market_data(state, exchange)
                safe_mode, downtime_start = False, None
            except ccxt.ExchangeNotAvailable:
                safe_mode, downtime_start = await handle_exchange_downtime(state, downtime_start)
                continue
            
            if safe_mode:
                continue
            
            if await check_protections(state, exchange, current_price, current_volume, high, low):
                break
            
            await handle_orders(state, exchange, current_price)
            await rebalance_grid_if_needed(state, exchange, current_price)
            last_status_update = await update_status(state, exchange, last_status_update)
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logging.info(f"🤖 Bot on {exchange.exchange_name} stopped by user")
        send_telegram_alert(f"🤖 Bot on {exchange.exchange_name} stopped by user")
        await state.order_manager.cancel_all_orders(exchange)
    except Exception as e:
        logging.error(f"❌ Critical error on {exchange.exchange_name}: {str(e)}")
        send_telegram_alert(f"❌ Critical error on {exchange.exchange_name}: {str(e)}")
        await state.order_manager.cancel_all_orders(exchange)
    finally:
        if config.websocket_enabled:
            await exchange.client.close()
        logging.info(f"Bot on {exchange.exchange_name} has ended")

# ======== DASH APP SETUP ========
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Grid Trading Bot Dashboard"),
    dcc.Graph(id='price-chart'),
    dcc.Graph(id='profit-chart'),
    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0)
])

@app.callback(
    [Output('price-chart', 'figure'), Output('profit-chart', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_charts(n):
    global bot_states
    price_data = []
    profit_data = []
    
    for exchange_name, state in bot_states.items():
        price_data.append(go.Scatter(
            x=[datetime.now()],
            y=[state.current_price],
            mode='lines+markers',
            name=f"{exchange_name} Price"
        ))
        profit_data.append(go.Scatter(
            x=[datetime.now()],
            y=[state.tracker.total_profit],
            mode='lines+markers',
            name=f"{exchange_name} Profit"
        ))
    
    price_fig = {
        'data': price_data,
        'layout': go.Layout(title='Price History', xaxis={'title': 'Time'}, yaxis={'title': 'Price'})
    }
    profit_fig = {
        'data': profit_data,
        'layout': go.Layout(title='Profit History', xaxis={'title': 'Time'}, yaxis={'title': 'Profit'})
    }
    
    return price_fig, profit_fig

# ======== MAIN EXECUTION ========
bot_states: Dict[str, BotState] = {}

async def main():
    exchange_manager = ExchangeManager()
    
    for exchange_name, exchange in exchange_manager.exchanges.items():
        await run_simulated_backtest(exchange)
    
    for exchange_name, exchange in exchange_manager.exchanges.items():
        historical_data = await safe_api_call(exchange.fetch_historical_data, timeframe='1h', limit=100)
        backtest_result = await backtest_strategy(exchange, historical_data)
        logging.info(f"Real backtest on {exchange_name}: {backtest_result}")
        send_telegram_alert(
            f"📈 Real backtest on {exchange_name}: "
            f"PnL={backtest_result['Total Profit']:.2f}, "
            f"Trades={backtest_result['Trade Count']}, "
            f"Sharpe={backtest_result['Sharpe Ratio']:.2f}, "
            f"MaxDD={backtest_result['Max Drawdown']:.2f}%, "
            f"Fees={backtest_result['Total Fees']:.2f}"
        )
    
    cpu_usage = psutil.cpu_percent(interval=1)
    max_workers = min(len(exchange_manager.exchanges), max(1, int(psutil.cpu_count() * (1 - cpu_usage / 100))))
    logging.info(f"Starting bot with {max_workers} workers based on CPU usage: {cpu_usage}%")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for exchange in exchange_manager.exchanges.values():
            executor.submit(lambda ex=exchange: asyncio.run(run_bot_for_exchange(ex, bot_states)))
    
    app.run(debug=False, host='0.0.0.0', port=8050)

if __name__ == "__main__":
    asyncio.run(main())