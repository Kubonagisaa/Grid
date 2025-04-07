import time
import math
from datetime import datetime, timedelta
import os
from typing import List, Tuple, Dict, Optional, Any
from collections import deque
from abc import ABC, abstractmethod
from dataclasses import dataclass
import asyncio
from threading import Thread
import logging
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import ccxt.async_support as ccxt_async
import dash
from dash import dcc, html, Output, Input
import plotly.graph_objs as go
from concurrent.futures import ThreadPoolExecutor
import cachetools
import psutil

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

# ======== SECURITY (Encrypt API keys) ========
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", Fernet.generate_key())
cipher = Fernet(ENCRYPTION_KEY)

def encrypt_credentials(api_key: str, api_secret: str) -> Tuple[str, str]:
    """
    Encrypts API key and secret using Fernet encryption.

    Args:
        api_key (str): API key to encrypt.
        api_secret (str): API secret to encrypt.

    Returns:
        Tuple[str, str]: Encrypted API key and secret.
    """
    encrypted_key = cipher.encrypt(api_key.encode()).decode()
    encrypted_secret = cipher.encrypt(api_secret.encode()).decode()
    return encrypted_key, encrypted_secret

def decrypt_credentials(encrypted_key: str, encrypted_secret: str) -> Tuple[str, str]:
    """
    Decrypts API key and secret using Fernet encryption.

    Args:
        encrypted_key (str): Encrypted API key.
        encrypted_secret (str): Encrypted API secret.

    Returns:
        Tuple[str, str]: Decrypted API key and secret.
    """
    api_key = cipher.decrypt(encrypted_key.encode()).decode()
    api_secret = cipher.decrypt(encrypted_secret.encode()).decode()
    return api_key, api_secret

def get_encrypted_credentials(exchange: str) -> Tuple[str, str]:
    """
    Retrieves and decrypts API credentials for the specified exchange.

    Args:
        exchange (str): Name of the exchange (e.g., 'binance').

    Returns:
        Tuple[str, str]: Decrypted API key and secret.
    """
    encrypted_key = os.getenv(f"{exchange.upper()}_API_KEY", "your_encrypted_api_key")
    encrypted_secret = os.getenv(f"{exchange.upper()}_API_SECRET", "your_encrypted_api_secret")
    return decrypt_credentials(encrypted_key, encrypted_secret)

# ======== CONFIGURATION MANAGEMENT (Load from .env) ========
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges: List[str] = os.getenv("ENABLED_EXCHANGES", "binance").split(",")
        self.exchange_credentials: Dict[str, Tuple[str, str]] = {
            ex: get_encrypted_credentials(ex) for ex in self.enabled_exchanges
        }
        self.symbol: str = os.getenv("TRADING_SYMBOL", "BTCUSDT")
        self.initial_investment: float = float(os.getenv("INITIAL_INVESTMENT", 100))
        self.min_quantity: float = float(os.getenv("MIN_QUANTITY", 0.0005))
        self.max_position: float = float(os.getenv("MAX_POSITION", 0.01))
        self.base_stop_loss_percent: float = float(os.getenv("BASE_STOP_LOSS_PERCENT", 2.0))
        self.base_take_profit_percent: float = float(os.getenv("BASE_TAKE_PROFIT_PERCENT", 2.0))
        self.price_drop_threshold: float = float(os.getenv("PRICE_DROP_THRESHOLD", 1.0))
        self.adaptive_grid_enabled: bool = os.getenv("ADAPTIVE_GRID_ENABLED", "True").lower() == "true"
        self.min_grid_levels: int = int(os.getenv("MIN_GRID_LEVELS", 5))
        self.max_grid_levels: int = int(os.getenv("MAX_GRID_LEVELS", 10))
        self.base_grid_step_percent: float = float(os.getenv("BASE_GRID_STEP_PERCENT", 0.01))
        self.grid_rebalance_interval: int = int(os.getenv("GRID_REBALANCE_INTERVAL", 300))
        self.trailing_stop_enabled: bool = os.getenv("TRAILING_STOP_ENABLED", "True").lower() == "true"
        self.trailing_up_activation: float = float(os.getenv("TRAILING_UP_ACTIVATION", 85000))
        self.trailing_down_activation: float = float(os.getenv("TRAILING_DOWN_ACTIVATION", 82000))
        self.trailing_tp_enabled: bool = os.getenv("TRAILING_TP_ENABLED", "True").lower() == "true"
        self.trailing_buy_stop_enabled: bool = os.getenv("TRAILING_BUY_STOP_ENABLED", "True").lower() == "true"
        self.trailing_buy_activation_percent: float = float(os.getenv("TRAILING_BUY_ACTIVATION_PERCENT", 1.5))
        self.trailing_buy_distance_percent: float = float(os.getenv("TRAILING_BUY_DISTANCE_PERCENT", 1.0))
        self.pump_protection_threshold: float = float(os.getenv("PUMP_PROTECTION_THRESHOLD", 0.03))
        self.circuit_breaker_threshold: float = float(os.getenv("CIRCUIT_BREAKER_THRESHOLD", 0.07))
        self.circuit_breaker_duration: int = int(os.getenv("CIRCUIT_BREAKER_DURATION", 360))
        self.abnormal_activity_threshold: float = float(os.getenv("ABNORMAL_ACTIVITY_THRESHOLD", 3.0))
        self.maker_fee: float = float(os.getenv("MAKER_FEE", 0.0002))
        self.taker_fee: float = float(os.getenv("TAKER_FEE", 0.0004))
        self.telegram_enabled: bool = bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))
        self.telegram_bot_token: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id: Optional[str] = os.getenv("TELEGRAM_CHAT_ID")
        self.api_retry_count: int = int(os.getenv("API_RETRY_COUNT", 3))
        self.api_retry_delay: int = int(os.getenv("API_RETRY_DELAY", 5))
        self.max_open_orders: int = int(os.getenv("MAX_OPEN_ORDERS", 50))
        self.status_update_interval: int = int(os.getenv("STATUS_UPDATE_INTERVAL", 600))
        self.volatility_window: int = int(os.getenv("VOLATILITY_WINDOW", 100))
        self.ma_short_period: int = int(os.getenv("MA_SHORT_PERIOD", 20))
        self.ma_long_period: int = int(os.getenv("MA_LONG_PERIOD", 50))
        self.atr_period: int = int(os.getenv("ATR_PERIOD", 14))
        self.rsi_period: int = int(os.getenv("RSI_PERIOD", 14))
        self.bb_period: int = int(os.getenv("BB_PERIOD", 20))
        self.bb_std_dev: float = float(os.getenv("BB_STD_DEV", 1.8))
        self.min_acceptable_volume: float = float(os.getenv("MIN_ACCEPTABLE_VOLUME", 100))
        self.fee_threshold: float = float(os.getenv("FEE_THRESHOLD", 0.1))
        self.websocket_enabled: bool = os.getenv("WEBSOCKET_ENABLED", "True").lower() == "true"
        self.websocket_reconnect_interval: int = int(os.getenv("WEBSOCKET_RECONNECT_INTERVAL", 60))

config = AdvancedConfig()

# ======== TELEGRAM ALERTS ========
def send_telegram_alert(message: str) -> None:
    """
    Sends an alert message to a Telegram chat.

    Args:
        message (str): The message to send.
    """
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

# ======== DATA CLASSES ========
@dataclass
class MarketData:
    price: float
    high: float
    low: float
    volume: float

@dataclass
class Signal:
    side: str  # "BUY" or "SELL"
    price: float
    quantity: float

# ======== ENHANCED CLASSES ========
class EnhancedProfitTracker:
    def __init__(self):
        self.total_profit: float = 0.0
        self.trade_count: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0
        self.equity_curve: List[float] = []
        self.total_fees: float = 0.0

    def record_trade(self, profit: float, fee: float) -> None:
        """
        Records a trade's profit and fee, updating statistics.

        Args:
            profit (float): Profit from the trade.
            fee (float): Fee incurred for the trade.
        """
        self.total_profit += profit
        self.trade_count += 1
        self.total_fees += fee
        if profit > 0:
            self.winning_trades += 1
        elif profit < 0:
            self.losing_trades += 1
        self.equity_curve.append(self.total_profit)

    def get_stats(self) -> Dict[str, float]:
        """
        Retrieves trading statistics.

        Returns:
            Dict[str, float]: Statistics including total profit, trade count, win rate, etc.
        """
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
        self.price_history: deque = deque(maxlen=config.volatility_window)
        self.high_history: deque = deque(maxlen=config.volatility_window)
        self.low_history: deque = deque(maxlen=config.volatility_window)
        self.volume_history: deque = deque(maxlen=config.volatility_window)
        self.circuit_breaker_triggered: bool = False
        self.circuit_breaker_start: Optional[float] = None
        self.trailing_stop_price: float = 0.0
        self.trailing_buy_price: float = 0.0

    def update(self, price: float, high: float, low: float, volume: float) -> None:
        """
        Updates the protection system with new market data.

        Args:
            price (float): Current price.
            high (float): High price.
            low (float): Low price.
            volume (float): Trading volume.
        """
        self.price_history.append(price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(volume)

    def check_pump_protection(self, price: float) -> bool:
        """
        Checks if a pump protection condition is triggered.

        Args:
            price (float): Current price.

        Returns:
            bool: True if pump protection is triggered, False otherwise.
        """
        if len(self.price_history) < 2:
            return False
        price_change = (price - self.price_history[-2]) / self.price_history[-2]
        return price_change > config.pump_protection_threshold

    def check_circuit_breaker(self, price: float) -> bool:
        """
        Checks if a circuit breaker condition is triggered.

        Args:
            price (float): Current price.

        Returns:
            bool: True if circuit breaker is triggered, False otherwise.
        """
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
        """
        Checks the status of the circuit breaker.

        Returns:
            bool: True if circuit breaker is active, False otherwise.
        """
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
        """
        Checks for abnormal trading activity based on volume.

        Args:
            volume (float): Current trading volume.

        Returns:
            bool: True if abnormal activity is detected, False otherwise.
        """
        if len(self.volume_history) < 2:
            return False
        avg_volume = np.mean(list(self.volume_history)[:-1])
        return volume > avg_volume * config.abnormal_activity_threshold

    def update_trailing_stop(self, price: float) -> Tuple[bool, float]:
        """
        Updates the trailing stop price and checks if stop condition is met.

        Args:
            price (float): Current price.

        Returns:
            Tuple[bool, float]: (Whether stop is triggered, updated trailing stop price).
        """
        if not config.trailing_stop_enabled:
            return False, self.trailing_stop_price
        if price > self.trailing_stop_price and price > config.trailing_up_activation:
            self.trailing_stop_price = price * (1 - config.base_stop_loss_percent / 100)
        elif price < self.trailing_stop_price and price < config.trailing_down_activation:
            self.trailing_stop_price = price * (1 + config.base_stop_loss_percent / 100)
        should_stop = price <= self.trailing_stop_price
        return should_stop, self.trailing_stop_price

    def update_trailing_buy(self, price: float) -> Tuple[bool, float]:
        """
        Updates the trailing buy price and checks if buy condition is met.

        Args:
            price (float): Current price.

        Returns:
            Tuple[bool, float]: (Whether buy is triggered, updated trailing buy price).
        """
        if not config.trailing_buy_stop_enabled:
            return False, self.trailing_buy_price
        if price < self.trailing_buy_price or self.trailing_buy_price == 0:
            self.trailing_buy_price = price * (1 + config.trailing_buy_activation_percent / 100)
        should_buy = price >= self.trailing_buy_price * (1 + config.trailing_buy_distance_percent / 100)
        return should_buy, self.trailing_buy_price

class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels: List[Tuple[float, float]] = []
        self.last_rebalance: float = time.time()
        self.order_ids: List[str] = []
        self.price_history: deque = deque(maxlen=config.volatility_window)
        self.high_history: deque = deque(maxlen=config.volatility_window)
        self.low_history: deque = deque(maxlen=config.volatility_window)

    def calculate_adaptive_grid(self, current_price: float, exchange: Optional['ExchangeInterface'] = None) -> None:
        """
        Calculates an adaptive grid based on market volatility.

        Args:
            current_price (float): Current market price.
            exchange (Optional[ExchangeInterface]): Exchange interface, if needed.
        """
        self.price_history.append(current_price)
        # These will be updated via MarketData in the strategy
        if not config.adaptive_grid_enabled or len(self.price_history) < config.volatility_window:
            self.grid_levels = self._create_static_grid(current_price)
            return

        atr = calculate_atr(
            list(self.high_history),
            list(self.low_history),
            list(self.price_history),
            config.atr_period
        )
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
        """
        Creates a static grid with fixed levels.

        Args:
            current_price (float): Current market price.

        Returns:
            List[Tuple[float, float]]: List of (buy_price, sell_price) tuples.
        """
        grid = []
        for i in range(config.min_grid_levels):
            buy_price = current_price * (1 - config.base_grid_step_percent * (i + 1))
            sell_price = current_price * (1 + config.base_grid_step_percent * (i + 1))
            grid.append((buy_price, sell_price))
        return grid

    async def place_grid_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        """
        Places grid orders based on current grid levels.

        Args:
            current_price (float): Current market price.
            exchange (ExchangeInterface): Exchange interface to place orders.
        """
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
        """
        Cancels all open grid orders.

        Args:
            exchange (ExchangeInterface): Exchange interface to cancel orders.
        """
        for order_id in self.order_ids:
            try:
                await safe_api_call(exchange.cancel_order, order_id)
            except Exception as e:
                logging.warning(f"Failed to cancel order {order_id}: {str(e)}")
        self.order_ids.clear()

class EnhancedOrderManager:
    def __init__(self):
        self.open_orders: List[Dict] = []
        self.is_paused: bool = False

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        """
        Cancels all open orders on the exchange.

        Args:
            exchange (ExchangeInterface): Exchange interface to cancel orders.
        """
        try:
            open_orders = await safe_api_call(exchange.get_open_orders)
            for order in open_orders:
                await safe_api_call(exchange.cancel_order, order['orderId'])
            self.open_orders.clear()
            logging.info("All open orders cancelled")
        except Exception as e:
            logging.error(f"Failed to cancel all orders: {str(e)}")

    async def check_and_handle_orders(self, current_price: float, exchange: 'ExchangeInterface', profit_tracker: 'EnhancedProfitTracker') -> None:
        """
        Checks and handles filled orders, recording profits and fees.

        Args:
            current_price (float): Current market price.
            exchange (ExchangeInterface): Exchange interface to check orders.
            profit_tracker (EnhancedProfitTracker): Profit tracker to record trades.
        """
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

# ======== STRATEGY INTERFACE ========
class GridStrategy(ABC):
    @abstractmethod
    async def generate_signals(self, data: MarketData) -> List[Signal]:
        """
        Generates trading signals based on market data.

        Args:
            data (MarketData): Current market data.

        Returns:
            List[Signal]: List of trading signals.
        """
        pass

class AdaptiveGridStrategy(GridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[Signal]:
        """
        Generates trading signals using an adaptive grid strategy.

        Args:
            data (MarketData): Current market data.

        Returns:
            List[Signal]: List of buy/sell signals.
        """
        # Update grid manager's histories
        self.grid_manager.price_history.append(data.price)
        self.grid_manager.high_history.append(data.high)
        self.grid_manager.low_history.append(data.low)

        self.grid_manager.calculate_adaptive_grid(data.price)
        signals = []
        for buy_price, sell_price in self.grid_manager.grid_levels:
            if data.price <= buy_price:
                signals.append(Signal(side="BUY", price=buy_price, quantity=config.min_quantity))
            elif data.price >= sell_price:
                signals.append(Signal(side="SELL", price=sell_price, quantity=config.min_quantity))
        return signals

# ======== BOT STATE ========
class BotState:
    def __init__(self, exchange: 'ExchangeInterface', strategy: GridStrategy):
        self.exchange: 'ExchangeInterface' = exchange
        self.exchange_name: str = exchange.exchange_name
        self.tracker: EnhancedProfitTracker = EnhancedProfitTracker()
        self.protection: EnhancedProtectionSystem = EnhancedProtectionSystem()
        self.current_price: float = 0.0
        self.grid_manager: EnhancedSmartGridManager = EnhancedSmartGridManager()
        self.order_manager: EnhancedOrderManager = EnhancedOrderManager()
        self.strategy: GridStrategy = strategy
        self.event_queue: asyncio.Queue = asyncio.Queue()

    async def process_events(self) -> None:
        """
        Processes events from the event queue, generating and executing signals.
        """
        while True:
            event = await self.event_queue.get()
            if event['type'] == 'price_update':
                market_data = MarketData(
                    price=event['price'],
                    volume=event['volume'],
                    high=event['high'],
                    low=event['low']
                )
                self.current_price = market_data.price
                self.protection.update(market_data.price, market_data.high, market_data.low, market_data.volume)
                if await check_protections(self, self.exchange, market_data.price, market_data.volume, market_data.high, market_data.low):
                    break
                signals = await self.strategy.generate_signals(market_data)
                for signal in signals:
                    try:
                        await self.exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
                    except Exception as e:
                        logging.error(f"Failed to place order: {signal.side} at {signal.price}: {str(e)}")
                await handle_orders(self, self.exchange, market_data.price)
                await rebalance_grid_if_needed(self, self.exchange, market_data.price)

# ======== EXCHANGE INTERFACE ABSTRACTION ========
class ExchangeInterface(ABC):
    def __init__(self, exchange_name: str, api_key: str, api_secret: str):
        self.exchange_name: str = exchange_name
        self.symbol: str = config.symbol
        self.price_precision: int = 2
        self.quantity_precision: int = 6
        self.websocket_connected: bool = False
        self.latest_price: float = 0.0
        self.latest_volume: float = 0.0
        self.latest_high: float = 0.0
        self.latest_low: float = 0.0
        self.client: Any = None  # Will be set by subclasses

    @abstractmethod
    async def get_price(self) -> float:
        pass

    @abstractmethod
    async def get_volume(self) -> float:
        pass

    @abstractmethod
    async def get_high_low(self) -> Tuple[float, float]:
        pass

    @abstractmethod
    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        pass

    @abstractmethod
    async def get_open_orders(self) -> List[Dict]:
        pass

    @abstractmethod
    async def get_all_orders(self) -> List[Dict]:
        pass

    @abstractmethod
    async def get_balance(self) -> Tuple[float, float]:
        pass

    @abstractmethod
    async def get_order_book(self) -> Dict:
        pass

    @abstractmethod
    async def get_symbol_filters(self) -> Dict:
        pass

    @abstractmethod
    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        pass

    @abstractmethod
    async def start_websocket(self, state: BotState) -> None:
        pass

    async def health_check(self) -> Dict[str, Any]:
        """
        Performs a health check on the exchange connection.

        Returns:
            Dict[str, Any]: Health status including WebSocket connection and API latency.
        """
        try:
            start_time = time.time()
            await self.get_price()
            latency = (time.time() - start_time) * 1000  # Latency in milliseconds
            return {
                'exchange': self.exchange_name,
                'websocket': self.websocket_connected,
                'api_latency_ms': latency,
                'status': 'healthy'
            }
        except Exception as e:
            return {
                'exchange': self.exchange_name,
                'websocket': self.websocket_connected,
                'api_latency_ms': None,
                'status': f'unhealthy: {str(e)}'
            }

class BaseExchange(ExchangeInterface):
    async def get_price(self) -> float:
        """
        Fetches the current price of the symbol.

        Returns:
            float: Current price.
        """
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['last'])
        except Exception as e:
            logging.error(f"Error fetching price on {self.exchange_name}: {str(e)}")
            raise

    async def get_volume(self) -> float:
        """
        Fetches the current trading volume of the symbol.

        Returns:
            float: Current volume.
        """
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['quoteVolume'])
        except Exception as e:
            logging.error(f"Error fetching volume on {self.exchange_name}: {str(e)}")
            raise

    async def get_high_low(self) -> Tuple[float, float]:
        """
        Fetches the high and low prices of the symbol.

        Returns:
            Tuple[float, float]: High and low prices.
        """
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            return float(ticker['high']), float(ticker['low'])
        except Exception as e:
            logging.error(f"Error fetching high/low on {self.exchange_name}: {str(e)}")
            raise

class BinanceExchange(BaseExchange):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("binance", api_key, api_secret)
        decrypted_key, decrypted_secret = decrypt_credentials(api_key, api_secret)
        self.client = ccxt_async.binance({
            'apiKey': decrypted_key,
            'secret': decrypted_secret,
            'enableRateLimit': True,
            'testnet': True
        })
        self.ws_client = None

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
            logging.error(f"Error placing order on {self.exchange_name}: {str(e)}")
            raise

    async def cancel_order(self, order_id: str) -> None:
        try:
            await self.client.cancel_order(order_id, self.symbol)
        except Exception as e:
            logging.error(f"Error cancelling order on {self.exchange_name}: {str(e)}")
            raise

    async def get_open_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_open_orders(self.symbol)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching open orders on {self.exchange_name}: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching all orders on {self.exchange_name}: {str(e)}")
            raise

    async def get_balance(self) -> Tuple[float, float]:
        try:
            balance = await self.client.fetch_balance()
            usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
            base_asset = self.symbol.split('USDT')[0]
            btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
            return usdt_balance, btc_balance
        except Exception as e:
            logging.error(f"Error fetching balance on {self.exchange_name}: {str(e)}")
            raise

    async def get_order_book(self) -> Dict:
        try:
            return await self.client.fetch_order_book(self.symbol, limit=50)
        except Exception as e:
            logging.error(f"Error fetching order book on {self.exchange_name}: {str(e)}")
            raise

    async def get_symbol_filters(self) -> Dict:
        try:
            markets = await self.client.load_markets()
            if self.symbol not in markets:
                logging.error(f"Symbol {self.symbol} not found in markets on {self.exchange_name}")
                return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}
            market = markets[self.symbol]
            logging.info(f"Successfully fetched LOT_SIZE for {self.symbol}: {market['limits']}")
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Error getting LOT_SIZE info from {self.exchange_name}: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            ohlcv = await self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"No historical data returned for {self.symbol} on {self.exchange_name}")
                raise ValueError(f"Insufficient historical data for {self.symbol}. Required: {limit} rows, got: 0")
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            if len(df) < config.volatility_window:
                logging.warning(f"Insufficient historical data for {self.symbol}. Required: {config.volatility_window}, got: {len(df)}")
                raise ValueError(f"Insufficient historical data for {self.symbol}. Required: {config.volatility_window}, got: {len(df)}")
            logging.info(f"Fetched {len(df)} rows of historical data for {self.symbol}")
            return df
        except Exception as e:
            logging.error(f"Error fetching historical data on {self.exchange_name}: {str(e)}")
            raise

    async def start_websocket(self, state: BotState) -> None:
        if not config.websocket_enabled:
            return
        try:
            await self.client.load_markets()
        except Exception as e:
            logging.error(f"Error loading markets in WebSocket on {self.exchange_name}: {str(e)}")
            self.websocket_connected = False
            return
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                await state.event_queue.put({
                    'type': 'price_update',
                    'price': float(ticker['last']),
                    'volume': float(ticker['quoteVolume']),
                    'high': float(ticker['high']),
                    'low': float(ticker['low'])
                })
                self.latest_price = float(ticker['last'])
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Price update - {self.latest_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} error: {str(e)}, retrying in {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

class MEXCExchange(BaseExchange):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("mexc", api_key, api_secret)
        decrypted_key, decrypted_secret = decrypt_credentials(api_key, api_secret)
        self.client = ccxt_async.mexc({
            'apiKey': decrypted_key,
            'secret': decrypted_secret,
            'enableRateLimit': True
        })
        self.symbol = self.symbol.replace("USDT", "/USDT")

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
            logging.error(f"Error placing order on {self.exchange_name}: {str(e)}")
            raise

    async def cancel_order(self, order_id: str) -> None:
        try:
            await self.client.cancel_order(order_id, self.symbol)
        except Exception as e:
            logging.error(f"Error cancelling order on {self.exchange_name}: {str(e)}")
            raise

    async def get_open_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_open_orders(self.symbol)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching open orders on {self.exchange_name}: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]
        except Exception as e:
            logging.error(f"Error fetching all orders on {self.exchange_name}: {str(e)}")
            raise

    async def get_balance(self) -> Tuple[float, float]:
        try:
            balance = await self.client.fetch_balance()
            usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
            base_asset = self.symbol.split('/')[0]
            btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
            return usdt_balance, btc_balance
        except Exception as e:
            logging.error(f"Error fetching balance on {self.exchange_name}: {str(e)}")
            raise

    async def get_order_book(self) -> Dict:
        try:
            return await self.client.fetch_order_book(self.symbol, limit=50)
        except Exception as e:
            logging.error(f"Error fetching order book on {self.exchange_name}: {str(e)}")
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
            logging.error(f"Error getting LOT_SIZE info from {self.exchange_name}: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            ohlcv = await self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"No historical data returned for {self.symbol} on {self.exchange_name}")
                raise ValueError(f"Insufficient historical data for {self.symbol}. Required: {limit} rows, got: 0")
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            if len(df) < config.volatility_window:
                logging.warning(f"Insufficient historical data for {self.symbol}. Required: {config.volatility_window}, got: {len(df)}")
                raise ValueError(f"Insufficient historical data for {self.symbol}. Required: {config.volatility_window}, got: {len(df)}")
            logging.info(f"Fetched {len(df)} rows of historical data for {self.symbol}")
            return df
        except Exception as e:
            logging.error(f"Error fetching historical data on {self.exchange_name}: {str(e)}")
            raise

    async def start_websocket(self, state: BotState) -> None:
        if not config.websocket_enabled:
            return
        try:
            await self.client.load_markets()
        except Exception as e:
            logging.error(f"Error loading markets on {self.exchange_name}: {str(e)}")
            return
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                await state.event_queue.put({
                    'type': 'price_update',
                    'price': float(ticker['last']),
                    'volume': float(ticker['quoteVolume']),
                    'high': float(ticker['high']),
                    'low': float(ticker['low'])
                })
                self.latest_price = float(ticker['last'])
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Price update - {self.latest_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} error: {str(e)}, retrying in {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

# ======== MULTI-EXCHANGE MANAGEMENT ========
class ExchangeManager:
    def __init__(self):
        self.exchanges: Dict[str, ExchangeInterface] = {}
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
        """
        Checks for price disparities between exchanges.
        """
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

# ======== ERROR HANDLING ========
async def safe_api_call(func, *args, max_retries: int = 3, **kwargs) -> Any:
    """
    Safely calls an API function with retry logic.

    Args:
        func: The API function to call.
        max_retries (int): Maximum number of retries.
        *args, **kwargs: Arguments to pass to the function.

    Returns:
        Any: Result of the API call.
    """
    for attempt in range(max_retries):
        try:
            cache_key = f"{func.__name__}_{str(args)}_{str(kwargs)}"
            if cache_key in api_cache:
                return api_cache[cache_key]
            result = await func(*args, **kwargs)
            api_cache[cache_key] = result
            return result
        except (ccxt_async.DDoSProtection, ccxt_async.RateLimitExceeded) as e:
            logging.warning(f"API rate limited: {str(e)}. Waiting {config.api_retry_delay} seconds")
            await asyncio.sleep(config.api_retry_delay * (2 ** attempt))
        except ccxt_async.ExchangeNotAvailable as e:
            logging.error(f"Exchange unavailable: {str(e)}. Pausing for 5 minutes")
            send_telegram_alert(f"⚠️ Exchange unavailable: {str(e)}")
            await asyncio.sleep(300)
        except (ccxt_async.NetworkError, ccxt_async.ExchangeError) as e:
            if attempt == max_retries - 1:
                logging.error(f"API call failed after {max_retries} attempts: {str(e)}")
                raise
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Unknown error: {str(e)}")
            raise

# ======== TECHNICAL INDICATOR CALCULATIONS ========
class RSICalculator:
    def __init__(self, period: int):
        self.period: int = period
        self.gains: deque = deque(maxlen=period)
        self.losses: deque = deque(maxlen=period)
        self.avg_gain: float = 0.0
        self.avg_loss: float = 0.0
        self.initialized: bool = False

    def update(self, price: float, prev_price: float) -> float:
        """
        Updates the RSI value with a new price.

        Args:
            price (float): Current price.
            prev_price (float): Previous price.

        Returns:
            float: Updated RSI value.
        """
        change = price - prev_price
        gain = change if change > 0 else 0
        loss = abs(change) if change < 0 else 0
        self.gains.append(gain)
        self.losses.append(loss)

        if len(self.gains) < self.period:
            return 50.0

        if not self.initialized:
            self.avg_gain = sum(self.gains) / self.period
            self.avg_loss = sum(self.losses) / self.period
            self.initialized = True
        else:
            self.avg_gain = (self.avg_gain * (self.period - 1) + gain) / self.period
            self.avg_loss = (self.avg_loss * (self.period - 1) + loss) / self.period

        if self.avg_loss == 0:
            return 100.0
        rs = self.avg_gain / self.avg_loss
        return 100 - (100 / (1 + rs))

def calculate_rsi(prices: List[float], period: int) -> float:
    """
    Calculates the Relative Strength Index (RSI) for a series of prices.

    Args:
        prices (List[float]): List of prices.
        period (int): RSI period.

    Returns:
        float: RSI value.
    """
    rsi_calculator = RSICalculator(period)
    rsi = 50.0
    for i in range(1, len(prices)):
        rsi = rsi_calculator.update(prices[i], prices[i-1])
    return rsi

def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
    """
    Calculates the Average True Range (ATR) for a series of prices.

    Args:
        highs (List[float]): List of high prices.
        lows (List[float]): List of low prices.
        closes (List[float]): List of closing prices.
        period (int): ATR period.

    Returns:
        float: ATR value.
    """
    # Validate input lengths
    if len(highs) != len(lows) or len(lows) != len(closes):
        raise ValueError("Highs, lows, and closes lists must have the same length")
    
    # Ensure there are enough data points to calculate ATR
    if len(closes) < period + 1:  # Need at least period + 1 elements because we access closes[i-1]
        raise ValueError(f"Need at least {period + 1} data points to calculate ATR with period {period}, got {len(closes)}")
    
    # Calculate True Range (TR) for each period
    tr_list = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
    
    # Calculate ATR as the average of the last 'period' TR values
    if not tr_list:
        return 0.0
    atr = sum(tr_list[-period:]) / period
    return atr

def calculate_bollinger_bands(prices: List[float], period: int, std_dev: float) -> Tuple[float, float, float]:
    """
    Calculates Bollinger Bands for a series of prices.

    Args:
        prices (List[float]): List of prices.
        period (int): Period for calculation.
        std_dev (float): Standard deviation multiplier.

    Returns:
        Tuple[float, float, float]: Middle, upper, and lower Bollinger Bands.
    """
    if len(prices) < period:
        return prices[-1], prices[-1], prices[-1]
    sma = float(np.mean(prices[-period:]))
    std = float(np.std(prices[-period:]))
    return sma, sma + std_dev * std, sma - std_dev * std

def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    """
    Calculates the annualized Sharpe ratio for a series of returns.

    Args:
        returns (List[float]): List of daily returns.
        risk_free_rate (float, optional): Risk-free rate, defaults to 0.02 (2%).

    Returns:
        float: Annualized Sharpe ratio. Returns 0.0 if insufficient data or zero standard deviation.
    """
    if len(returns) < 2:
        return 0.0
    mean_return = float(np.mean(returns))
    std_return = float(np.std(returns))
    return (mean_return - risk_free_rate) / std_return * np.sqrt(252) if std_return > 0 else 0.0

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    """
    Calculates the maximum drawdown percentage from an equity curve.

    Args:
        equity_curve (List[float]): List of equity values over time.

    Returns:
        float: Maximum drawdown percentage. Returns 0.0 if insufficient data.
    """
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if peak == 0:
            continue
        peak = max(peak, value)
        dd = (peak - value) / peak if peak != 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd * 100

# ======== SIMULATED BACKTEST ========
async def run_simulated_backtest(exchange: ExchangeInterface) -> Dict[str, float]:
    """
    Runs a simulated backtest on historical data.

    Args:
        exchange (ExchangeInterface): Exchange interface to fetch data.

    Returns:
        Dict[str, float]: Backtest statistics.
    """
    logging.info(f"Running simulated backtest on {exchange.exchange_name}")
    try:
        historical_data = await safe_api_call(exchange.fetch_historical_data, timeframe='1h', limit=100)
    except ValueError as e:
        logging.warning(f"Backtest failed on {exchange.exchange_name}: {str(e)}")
        return {"Total Profit": 0.0, "Trade Count": 0, "Sharpe Ratio": 0.0, "Max Drawdown": 0.0, "Total Fees": 0.0}

    profit_tracker = EnhancedProfitTracker()
    grid_manager = EnhancedSmartGridManager()
    protection = EnhancedProtectionSystem()
    strategy = AdaptiveGridStrategy(grid_manager)

    for i in range(len(historical_data)):
        price = historical_data['close'].iloc[i]
        high = historical_data['high'].iloc[i]
        low = historical_data['low'].iloc[i]
        volume = historical_data['volume'].iloc[i]

        protection.update(price, high, low, volume)
        market_data = MarketData(price=price, high=high, low=low, volume=volume)
        signals = await strategy.generate_signals(market_data)

        for signal in signals:
            if signal.side == "BUY":
                profit = (signal.price * (1 + config.base_take_profit_percent / 100) - signal.price) * signal.quantity
                fee = signal.quantity * signal.price * config.maker_fee
                profit_tracker.record_trade(profit, fee)
            elif signal.side == "SELL":
                profit = (signal.price - signal.price * (1 - config.base_stop_loss_percent / 100)) * signal.quantity
                fee = signal.quantity * signal.price * config.maker_fee
                profit_tracker.record_trade(profit, fee)

    stats = profit_tracker.get_stats()
    logging.info(f"Simulated backtest on {exchange.exchange_name}: {stats}")
    return stats

# ======== REAL BACKTEST ========
async def backtest_strategy(exchange: ExchangeInterface, historical_data: pd.DataFrame) -> Dict[str, float]:
    """
    Runs a real backtest on historical data.

    Args:
        exchange (ExchangeInterface): Exchange interface.
        historical_data (pd.DataFrame): Historical market data.

    Returns:
        Dict[str, float]: Backtest statistics.
    """
    logging.info(f"Running real backtest on {exchange.exchange_name}")
    if historical_data.empty:
        logging.warning(f"No historical data for real backtest on {exchange.exchange_name}")
        return {"Total Profit": 0.0, "Trade Count": 0, "Sharpe Ratio": 0.0, "Max Drawdown": 0.0, "Total Fees": 0.0}

    profit_tracker = EnhancedProfitTracker()
    grid_manager = EnhancedSmartGridManager()
    protection = EnhancedProtectionSystem()
    strategy = AdaptiveGridStrategy(grid_manager)

    for i in range(len(historical_data)):
        price = historical_data['close'].iloc[i]
        high = historical_data['high'].iloc[i]
        low = historical_data['low'].iloc[i]
        volume = historical_data['volume'].iloc[i]

        protection.update(price, high, low, volume)
        market_data = MarketData(price=price, high=high, low=low, volume=volume)
        signals = await strategy.generate_signals(market_data)

        for signal in signals:
            if signal.side == "BUY":
                profit = (signal.price * (1 + config.base_take_profit_percent / 100) - signal.price) * signal.quantity
                fee = signal.quantity * signal.price * config.maker_fee
                profit_tracker.record_trade(profit, fee)
            elif signal.side == "SELL":
                profit = (signal.price - signal.price * (1 - config.base_stop_loss_percent / 100)) * signal.quantity
                fee = signal.quantity * signal.price * config.maker_fee
                profit_tracker.record_trade(profit, fee)

    stats = profit_tracker.get_stats()
    return stats

# ======== BOT EXECUTION HELPERS ========
async def check_account_balance(exchange: ExchangeInterface) -> Tuple[float, float]:
    """
    Checks the account balance on the exchange.

    Args:
        exchange (ExchangeInterface): Exchange interface.

    Returns:
        Tuple[float, float]: USDT and BTC balances.
    """
    usdt_balance, btc_balance = await safe_api_call(exchange.get_balance)
    logging.info(f"Account balance on {exchange.exchange_name}: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC")
    if usdt_balance < config.initial_investment:
        logging.warning(f"Insufficient USDT balance: {usdt_balance:.2f} < {config.initial_investment}")
        send_telegram_alert(f"⚠️ Insufficient USDT balance on {exchange.exchange_name}: {usdt_balance:.2f} < {config.initial_investment}")
    return usdt_balance, btc_balance

async def handle_exchange_downtime(state: BotState, downtime_start: Optional[float]) -> Tuple[bool, Optional[float]]:
    """
    Handles exchange downtime by entering safe mode.

    Args:
        state (BotState): Bot state.
        downtime_start (Optional[float]): Timestamp when downtime started.

    Returns:
        Tuple[bool, Optional[float]]: (Whether in safe mode, updated downtime start).
    """
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
    """
    Checks protection mechanisms and decides whether to pause or stop the bot.

    Args:
        state (BotState): Bot state.
        exchange (ExchangeInterface): Exchange interface.
        current_price (float): Current price.
        current_volume (float): Current volume.
        high (float): High price.
        low (float): Low price.

    Returns:
        bool: True if bot should stop, False otherwise.
    """
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
    """
    Handles order checking and updates.

    Args:
        state (BotState): Bot state.
        exchange (ExchangeInterface): Exchange interface.
        current_price (float): Current price.
    """
    await state.order_manager.check_and_handle_orders(current_price, exchange, state.tracker)

async def rebalance_grid_if_needed(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    """
    Rebalances the grid if the rebalance interval has passed.

    Args:
        state (BotState): Bot state.
        exchange (ExchangeInterface): Exchange interface.
        current_price (float): Current price.
    """
    if time.time() - state.grid_manager.last_rebalance > config.grid_rebalance_interval:
        state.grid_manager.calculate_adaptive_grid(current_price, exchange)
        await state.grid_manager.place_grid_orders(current_price, exchange)
        state.grid_manager.last_rebalance = time.time()
        logging.info(f"Grid rebalanced on {state.exchange_name}")

async def update_status(state: BotState, exchange: ExchangeInterface, last_status_update: float) -> float:
    """
    Updates the bot status and sends alerts periodically.

    Args:
        state (BotState): Bot state.
        exchange (ExchangeInterface): Exchange interface.
        last_status_update (float): Timestamp of the last status update.

    Returns:
        float: Updated timestamp of the last status update.
    """
    if time.time() - last_status_update > config.status_update_interval:
        stats = state.tracker.get_stats()
        usdt_balance, btc_balance = await safe_api_call(exchange.get_balance)
        health = await exchange.health_check()
        logging.info(f"Status update on {state.exchange_name}: {stats}, Balance: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC, Health: {health}")
        send_telegram_alert(
            f"📊 Status update on {state.exchange_name}:\n"
            f"Total Profit: {stats['Total Profit']:.2f}\n"
            f"Trades: {stats['Trade Count']}\n"
            f"Win Rate: {stats['Win Rate']:.2f}%\n"
            f"Max Drawdown: {stats['Max Drawdown']:.2f}%\n"
            f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}\n"
            f"Total Fees: {stats['Total Fees']:.2f}\n"
            f"Balance: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC\n"
            f"Health: {health['status']} (Latency: {health['api_latency_ms']:.2f}ms)"
        )
        last_status_update = time.time()
    return last_status_update

# ======== MAIN BOT EXECUTION ========
async def run_bot_for_exchange(exchange: ExchangeInterface, states: Dict[str, BotState]) -> None:
    """
    Runs the trading bot for a specific exchange.

    Args:
        exchange (ExchangeInterface): Exchange interface.
        states (Dict[str, BotState]): Dictionary of bot states.
    """
    strategy = AdaptiveGridStrategy(EnhancedSmartGridManager())
    state = BotState(exchange, strategy)
    states[exchange.exchange_name] = state

    logging.info(f"🤖 Bot starting on {exchange.exchange_name.upper()}")
    await state.order_manager.cancel_all_orders(exchange)
    usdt_balance, btc_balance = await check_account_balance(exchange)
    state.current_price = await safe_api_call(exchange.get_price)

    health = await exchange.health_check()
    logging.info(f"Initial health check: {health}")
    if health['status'] != 'healthy':
        send_telegram_alert(f"⚠️ Health check failed on {exchange.exchange_name}: {health['status']}")
        return

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
                await state.process_events()
                safe_mode, downtime_start = False, None
            except ccxt_async.ExchangeNotAvailable:
                safe_mode, downtime_start = await handle_exchange_downtime(state, downtime_start)
                continue

            if safe_mode:
                continue

            last_status_update = await update_status(state, exchange, last_status_update)

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
def update_charts(n: int) -> Tuple[Dict, Dict]:
    """
    Updates the dashboard charts with price and profit data.

    Args:
        n (int): Number of intervals.

    Returns:
        Tuple[Dict, Dict]: Price and profit chart figures.
    """
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

# ======== UNIT TESTS ========
import unittest
from unittest.mock import AsyncMock, patch

class TestIndicators(unittest.TestCase):
    def test_calculate_rsi(self):
        prices = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115]
        rsi = calculate_rsi(prices, period=14)
        self.assertAlmostEqual(rsi, 100.0, places=2)  # Giá tăng liên tục, RSI nên gần 100

        prices = [100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85]
        rsi = calculate_rsi(prices, period=14)
        self.assertAlmostEqual(rsi, 0.0, places=2)  # Giá giảm liên tục, RSI nên gần 0

    def test_calculate_rsi_insufficient_data(self):
        prices = [100, 101, 102]
        rsi = calculate_rsi(prices, period=14)
        self.assertEqual(rsi, 50.0)  # Dữ liệu không đủ, trả về giá trị mặc định

    def test_calculate_atr(self):
        # Provide at least period + 1 data points (period=2, so need 3 data points)
        highs = [105, 106, 107]
        lows = [95, 94, 93]
        closes = [100, 101, 102]
        atr = calculate_atr(highs, lows, closes, period=2)
        # Expected ATR calculation:
        # TR1 = max(106-94, |106-100|, |94-100|) = 12
        # TR2 = max(107-93, |107-101|, |93-101|) = 14
        # ATR = (12 + 14) / 2 = 13
        self.assertAlmostEqual(atr, 13.0, places=2)

    def test_calculate_atr_insufficient_data(self):
        highs = [105]
        lows = [95]
        closes = [100]
        with self.assertRaises(ValueError):
            calculate_atr(highs, lows, closes, period=2)

class TestBinanceExchange(unittest.IsolatedAsyncioTestCase):
    @patch('ccxt.async_support.binance')
    async def test_get_price(self, mock_binance):
        mock_binance.return_value.fetch_ticker = AsyncMock(return_value={'last': 50000.0})
        exchange = BinanceExchange("api_key", "api_secret")
        price = await exchange.get_price()
        self.assertEqual(price, 50000.0)

# ======== MAIN EXECUTION ========
bot_states: Dict[str, BotState] = {}

async def main():
    """
    Main entry point for the trading bot.
    """
    exchange_manager = ExchangeManager()

    for exchange_name, exchange in exchange_manager.exchanges.items():
        await run_simulated_backtest(exchange)

    for exchange_name, exchange in exchange_manager.exchanges.items():
        try:
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
        except ValueError as e:
            logging.warning(f"Backtest failed on {exchange_name}: {str(e)}")

    cpu_usage = psutil.cpu_percent(interval=1)
    max_workers = min(len(exchange_manager.exchanges), max(1, int(psutil.cpu_count() * (1 - cpu_usage / 100))))
    logging.info(f"Starting bot with {max_workers} workers based on CPU usage: {cpu_usage}%")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for exchange in exchange_manager.exchanges.values():
            executor.submit(lambda ex=exchange: asyncio.run(run_bot_for_exchange(ex, bot_states)))

    app.run(debug=False, host='0.0.0.0', port=8050)

if __name__ == "__main__":
    # Run unit tests
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
    # Run the bot
    asyncio.run(main())