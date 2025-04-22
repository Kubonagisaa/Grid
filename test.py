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
from dotenv import load_dotenv
import ccxt.async_support as ccxt_async
import aiohttp
import signal
import sys
import threading
import json

import nest_asyncio
nest_asyncio.apply()

# ======== INITIAL SETUP ========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grid_trading_bot.log'),
        logging.StreamHandler(),
    ]
)

STATE_FILE = "bot_state.json"
order_lock = asyncio.Lock()

# ======== CONFIGURATION MANAGEMENT (from .env) ========
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges: List[str] = os.getenv("ENABLED_EXCHANGES", "paper_trading").split(",")
        self.exchange_credentials: Dict[str, Tuple[str, str]] = {
            ex: (os.getenv(f"{ex.upper()}_API_KEY", "your_api_key"),
                 os.getenv(f"{ex.upper()}_API_SECRET", "your_api_secret"))
            for ex in self.enabled_exchanges
        }
        self.symbol: str = os.getenv("TRADING_SYMBOL", "BTCUSDT")
        self.initial_investment: float = float(os.getenv("INITIAL_INVESTMENT", 20))
        self.min_quantity: float = float(os.getenv("MIN_QUANTITY", 0.0005))
        self.max_position: float = float(os.getenv("MAX_POSITION", 0.01))
        self.base_stop_loss_percent: float = float(os.getenv("BASE_STOP_LOSS_PERCENT", 2.0))
        self.base_take_profit_percent: float = float(os.getenv("BASE_TAKE_PROFIT_PERCENT", 2.0))
        self.price_drop_threshold: float = float(os.getenv("PRICE_DROP_THRESHOLD", 1.0))
        self.adaptive_grid_enabled: bool = os.getenv("ADAPTIVE_GRID_ENABLED", "True").lower() == "true"
        self.min_grid_levels: int = int(os.getenv("MIN_GRID_LEVELS", 5))
        self.max_grid_levels: int = int(os.getenv("MAX_GRID_LEVELS", 15))
        self.base_grid_step_percent: float = float(os.getenv("BASE_GRID_STEP_PERCENT", 2.0))
        self.grid_rebalance_interval: int = int(os.getenv("GRID_REBALANCE_INTERVAL", 300))
        self.trailing_stop_enabled: bool = os.getenv("TRAILING_STOP_ENABLED", "True").lower() == "true"
        self.trailing_up_activation: float = float(os.getenv("TRAILING_UP_ACTIVATION", 90000))
        self.trailing_down_activation: float = float(os.getenv("TRAILING_DOWN_ACTIVATION", 80000))
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
        self.status_update_interval: int = int(os.getenv("STATUS_UPDATE_INTERVAL", 1800))
        self.volatility_window: int = int(os.getenv("VOLATILITY_WINDOW", 200))
        self.ma_short_period: int = int(os.getenv("MA_SHORT_PERIOD", 20))
        self.ma_long_period: int = int(os.getenv("MA_LONG_PERIOD", 50))
        self.atr_period: int = int(os.getenv("ATR_PERIOD", 14))
        self.rsi_period: int = int(os.getenv("RSI_PERIOD", 14))
        self.bb_period: int = int(os.getenv("BB_PERIOD", 20))
        self.bb_std_dev: float = float(os.getenv("BB_STD_DEV", 1.8))
        self.min_acceptable_volume: float = float(os.getenv("MIN_ACCEPTABLE_VOLUME", 100))
        self.fee_threshold: float = float(os.getenv("FEE_THRESHOLD", 0.1))
        self.websocket_enabled: bool = os.getenv("WEBSOCKET_ENABLED", "False").lower() == "true"
        self.websocket_reconnect_interval: int = int(os.getenv("WEBSOCKET_RECONNECT_INTERVAL", 60))
        self.order_cooldown_sec: int = int(os.getenv("ORDER_COOLDOWN_SECONDS", 180))
        self.paper_trading_enabled: bool = os.getenv("PAPER_TRADING", "False").lower() == "true"
        self.live_paper_trading: bool = os.getenv("LIVE_PAPER_TRADING", "False").lower() == "true"
        self.dashboard_username: str = os.getenv("DASHBOARD_USERNAME", "admin")
        self.dashboard_password: str = os.getenv("DASHBOARD_PASSWORD", "admin123")

        # Strategy config (1: Wide Grid + RSI, 2: Dense Grid no RSI)
        self.strategy_mode: int = int(os.getenv("STRATEGY_MODE", 1))
        self.wide_grid_step_percent_min: float = float(os.getenv("WIDE_GRID_STEP_PERCENT_MIN", 2.0))
        self.wide_grid_step_percent_max: float = float(os.getenv("WIDE_GRID_STEP_PERCENT_MAX", 3.0))
        self.dense_grid_step_percent_min: float = float(os.getenv("DENSE_GRID_STEP_PERCENT_MIN", 0.5))
        self.dense_grid_step_percent_max: float = float(os.getenv("DENSE_GRID_STEP_PERCENT_MAX", 1.0))
        self.capital_reserve_percent: float = float(os.getenv("CAPITAL_RESERVE_PERCENT", 0.0))
        if self.strategy_mode == 1:
            self.base_grid_step_percent = self.wide_grid_step_percent_min
        elif self.strategy_mode == 2:
            self.base_grid_step_percent = self.dense_grid_step_percent_min

config = AdvancedConfig()
logging.info(f"Selected strategy {config.strategy_mode}: {'Wide Grid (~2-3%, uses RSI)' if config.strategy_mode == 1 else 'Dense Grid (~0.5%, no RSI)'}")
logging.info(f"Base grid step: {config.base_grid_step_percent:.2f}%, min levels: {config.min_grid_levels}, max levels: {config.max_grid_levels}")
# ======== UTILITY FUNCTIONS (ATR, RSI, drawdown, etc.) ========
def calculate_atr(highs: List[float], lows: List[float], prices: List[float], period: int) -> float:
    """Calculate ATR (Average True Range) indicator from given data."""
    if len(highs) < period or len(lows) < period or len(prices) < period:
        return 0.0
    tr_values = []
    for i in range(1, len(prices)):
        high = highs[i]
        low = lows[i]
        prev_close = prices[i-1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_values.append(tr)
    atr = sum(tr_values[-period:]) / period if len(tr_values) >= period else 0.0
    return atr

def calculate_rsi(prices: List[float], period: int) -> float:
    """Calculate RSI (Relative Strength Index) from closing prices."""
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices)
    ups = deltas.clip(min=0)
    downs = -1 * deltas.clip(max=0)
    avg_gain = np.mean(ups[-period:]) if len(ups) >= period else 0
    avg_loss = np.mean(downs[-period:]) if len(downs) >= period else 0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    """Calculate max drawdown (%) from equity curve."""
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = (peak - value) / peak if peak > 0 else 0
        max_dd = max(max_dd, drawdown)
    return max_dd * 100

def auto_adjust_rsi_thresholds(prices: List[float], default_low=30, default_high=70) -> Tuple[float, float]:
    """Dynamically adjust RSI thresholds based on volatility."""
    rsi = calculate_rsi(prices, config.rsi_period)
    if rsi < 40:
        return (25, 65)
    elif rsi > 60:
        return (35, 75)
    else:
        return (default_low, default_high)

# ======== ADVANCED PROTECTION SYSTEM ========
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
        """Update price/volume history."""
        if price <= 0 or high <= 0 or low <= 0 or volume < 0:
            logging.warning(f"Invalid data: price={price}, high={high}, low={low}, volume={volume}. Skipping update.")
            return
        self.price_history.append(price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(volume)

    def check_pump_protection(self, price: float) -> bool:
        """Trigger pump protection on sudden price surge."""
        if len(self.price_history) < 2:
            return False
        price_change = (price - self.price_history[-2]) / self.price_history[-2]
        return price_change > config.pump_protection_threshold

    async def check_circuit_breaker(self, price: float) -> bool:
        """Trigger circuit breaker on abnormal volatility."""
        if len(self.price_history) < 2:
            return False
        price_change = abs(price - self.price_history[-2]) / self.price_history[-2]
        if price_change > config.circuit_breaker_threshold:
            self.circuit_breaker_triggered = True
            self.circuit_breaker_start = time.time()
            logging.warning(f"Circuit breaker triggered: Price moved {price_change:.2%}")
            await send_telegram_alert("Pump protection: market too volatile, trading paused.")
            return True
        return False

    async def check_circuit_breaker_status(self) -> bool:
        """Check if circuit breaker duration expired."""
        if self.circuit_breaker_triggered and self.circuit_breaker_start:
            elapsed = time.time() - self.circuit_breaker_start
            if elapsed > config.circuit_breaker_duration:
                self.circuit_breaker_triggered = False
                self.circuit_breaker_start = None
                logging.info("Circuit breaker reset.")
                await send_telegram_alert("Circuit breaker reset. Trading resumed.")
                return False
            return True
        return False

    def check_abnormal_activity(self, volume: float) -> bool:
        """Detect abnormal trading volume activity."""
        if len(self.volume_history) < 2:
            return False
        avg_volume = np.mean(list(self.volume_history)[:-1])
        return volume > avg_volume * config.abnormal_activity_threshold
     def update_trailing_stop(self, price: float) -> Tuple[bool, float]:
        """(Unused in this version) Update global trailing stop."""
        if not config.trailing_stop_enabled:
            return False, self.trailing_stop_price
        if price > self.trailing_stop_price and price > config.trailing_up_activation:
            self.trailing_stop_price = price * (1 - config.base_stop_loss_percent / 100)
        elif price < self.trailing_stop_price and price < config.trailing_down_activation:
            self.trailing_stop_price = price * (1 + config.base_stop_loss_percent / 100)
        should_stop = price <= self.trailing_stop_price
        return should_stop, self.trailing_stop_price

    def update_trailing_buy(self, price: float) -> Tuple[bool, float]:
        """(Unused in this version) Update trailing buy for bottom-catching entry."""
        if not config.trailing_buy_stop_enabled:
            return False, self.trailing_buy_price
        if price < self.trailing_buy_price or self.trailing_buy_price == 0:
            self.trailing_buy_price = price * (1 + config.trailing_buy_activation_percent / 100)
        should_buy = price >= self.trailing_buy_price * (1 + config.trailing_buy_distance_percent / 100)
        return should_buy, self.trailing_buy_price

# ======== ADVANCED GRID MANAGER ========
class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels: List[Tuple[float, float]] = []
        self.static_levels: int = 3
        if config.strategy_mode == 2:
            self.static_levels = 0
        self.static_orders_placed: bool = False
        self.static_order_ids: List[str] = []
        self.order_ids: List[str] = []
        self.last_rebalance: float = time.time()
        self.last_fallback_time: float = 0
        self.price_history: deque = deque(maxlen=config.volatility_window)
        self.high_history: deque = deque(maxlen=config.volatility_window)
        self.low_history: deque = deque(maxlen=config.volatility_window)
        self.grid_cooldown: Dict[Tuple[str, float], float] = {}
        self.trailing_stops: Dict[Tuple[str, float], Any] = {}
        self.price_precision: int = 2
        self.FALLBACK_COOLDOWN: int = 300

    async def calculate_adaptive_grid(self, current_price: float, exchange: Optional['ExchangeInterface'] = None) -> None:
        """Calculate dynamic grid levels using ATR and volatility."""
        if not isinstance(current_price, (int, float)) or current_price <= 0:
            logging.warning(f"Invalid current_price: {current_price}")
            return

        self.price_history.append(current_price)
        if not config.adaptive_grid_enabled or len(self.price_history) < config.volatility_window:
            self.grid_levels = self._create_static_grid(current_price)
            return

        if len(self.high_history) != len(self.low_history) or len(self.low_history) != len(self.price_history):
            logging.warning("Price histories out of sync. Trimming to match lengths.")
            min_len = min(len(self.high_history), len(self.low_history), len(self.price_history))
            self.high_history = deque(list(self.high_history)[-min_len:], maxlen=config.volatility_window)
            self.low_history = deque(list(self.low_history)[-min_len:], maxlen=config.volatility_window)
            self.price_history = deque(list(self.price_history)[-min_len:], maxlen=config.volatility_window)

        atr = calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)
        if atr == 0:
            atr = current_price * 0.001

        volatility = min(atr / current_price, 0.1)
        num_levels = max(config.min_grid_levels, min(config.max_grid_levels, int(volatility / (config.base_grid_step_percent / 100))))
        raw_step = volatility / num_levels if num_levels > 0 else (config.base_grid_step_percent / 100)

        if config.strategy_mode == 1:
            step_percent = max(config.wide_grid_step_percent_min / 100, min(config.wide_grid_step_percent_max / 100, raw_step))
        elif config.strategy_mode == 2:
            step_percent = max(config.dense_grid_step_percent_min / 100, min(config.dense_grid_step_percent_max / 100, raw_step))
        else:
            step_percent = raw_step

        self.grid_levels = []
        for i in range(num_levels):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                logging.warning(f"Invalid price: buy={buy_price}, sell={sell_price}")
                continue
            if buy_price < current_price * 0.9 or sell_price > current_price * 1.1:
                logging.warning(f"Out-of-range grid level skipped: buy={buy_price}, sell={sell_price}")
                continue
            self.grid_levels.append((buy_price, sell_price))

        logging.info(f"Adaptive grid computed: {len(self.grid_levels)} levels, step {step_percent:.2%}")
        
        def _create_static_grid(self, current_price: float) -> List[Tuple[float, float]]:
        """Create static grid levels around the current price with fixed steps."""
        grid = []
        step_percent = config.base_grid_step_percent / 100
        for i in range(max(self.static_levels, config.min_grid_levels)):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                logging.warning(f"Invalid static level: buy={buy_price}, sell={sell_price}")
                continue
            if buy_price < current_price * 0.9 or sell_price > current_price * 1.1:
                logging.warning(f"Static level out of bounds: buy={buy_price}, sell={sell_price}")
                continue
            grid.append((buy_price, sell_price))
        return grid

    def duplicate_order_exists(self, side: str, price: float, open_orders: List[Dict]) -> bool:
        """Check if there's already an open order at a similar price."""
        target = round(price, self.price_precision)
        for order in open_orders:
            if 'price' not in order or order['price'] is None:
                continue
            order_price = round(float(order['price']), self.price_precision)
            if order['side'].upper() == side.upper() and order_price == target:
                return True
        return False

    def set_cooldown(self, side: str, price: float):
        """Set cooldown for a price level after execution."""
        key = (side.upper(), round(price, self.price_precision))
        self.grid_cooldown[key] = time.time() + config.order_cooldown_sec
        logging.info(f"Set cooldown for {key} until {self.grid_cooldown[key]:.2f}")

    def get_grid_range(self) -> Tuple[Optional[float], Optional[float]]:
        """Get min and max price range of the current grid."""
        if not self.grid_levels:
            return None, None
        lows = [l for l, _ in self.grid_levels]
        highs = [h for _, h in self.grid_levels]
        return (min(lows) if lows else None, max(highs) if highs else None)

    def _price_out_of_grid(self, current_price: float) -> bool:
        """Check if the current price has exited the grid range."""
        low_grid, high_grid = self.get_grid_range()
        return ((low_grid is not None and current_price < low_grid) or (high_grid is not None and current_price > high_grid))

    async def rebalance_if_needed(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        """Recalculate and place new grid orders if out-of-range or time expired."""
        need_rebalance = (
            time.time() - self.last_rebalance > config.grid_rebalance_interval or
            self._price_out_of_grid(current_price)
        )
        if need_rebalance:
            async with order_lock:
                await self.cancel_all_orders(exchange, preserve_static=False)
                await self.calculate_adaptive_grid(current_price, exchange)
                await self.place_static_orders(current_price, exchange)
                await self.place_grid_orders(current_price, exchange)
                self.last_rebalance = time.time()
                logging.info(f"Rebalanced grid at price {current_price:.2f}")
                
                # ======== SAFE API CALL WRAPPER ========
async def safe_api_call(func, *args, **kwargs):
    """Safely execute API calls with error logging."""
    try:
        result = func(*args, **kwargs)
        if hasattr(result, "__await__"):
            return await result
        return result
    except Exception as e:
        logging.error(f"API call error: {str(e)}")
        raise

# ======== MAIN ENTRYPOINT ========
if __name__ == "__main__":
    async def main():
        config.paper_trading_enabled = True
        exchange = PaperTradingExchange(config.symbol)
        grid_manager = EnhancedSmartGridManager()
        strategy = AdaptiveGridStrategy(grid_manager) if config.strategy_mode == 1 else DenseGridStrategy(grid_manager)
        state = BotState(exchange, strategy)
        states = {exchange.exchange_name: state}
        load_state(states)
        await exchange.run_paper_trading_live(state, interval=1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot manually stopped.")