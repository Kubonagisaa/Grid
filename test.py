import time
import math
import json
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

# Nếu cần chạy trong notebook hay môi trường có event loop sẵn, bật nest_asyncio
import nest_asyncio
nest_asyncio.apply()

# ======== CÀI ĐẶT BAN ĐẦU ========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grid_trading_bot.log'),
        logging.StreamHandler(),
    ]
)

# File để lưu trạng thái bot
STATE_FILE = "bot_state.json"

# Lock để đồng bộ hóa đặt lệnh
order_lock = asyncio.Lock()

# ======== QUẢN LÝ CẤU HÌNH (Lấy từ .env) ========
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

logging.info(f"Đã chọn chiến lược {config.strategy_mode}: {'Grid thưa (bước ~2-3%, có RSI)' if config.strategy_mode == 1 else 'Grid dày (bước ~0.5%, không dùng RSI)'}")
logging.info(f"Bước lưới cơ bản: {config.base_grid_step_percent:.2f}%, Mức grid tối thiểu: {config.min_grid_levels}, tối đa: {config.max_grid_levels}")

# ======== HÀM TIỆN ÍCH (ATR, RSI, tính drawdown, Sharpe, tự điều chỉnh RSI) ========
def calculate_atr(highs: List[float], lows: List[float], prices: List[float], period: int) -> float:
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
    rsi = calculate_rsi(prices, config.rsi_period)
    if rsi < 40:
        return (25, 65)
    elif rsi > 60:
        return (35, 75)
    else:
        return (default_low, default_high)

# ======== HỆ THỐNG BẢO VỆ NÂNG CAO ========
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
        if price <= 0 or high <= 0 or low <= 0 or volume < 0:
            logging.warning(f"Dữ liệu không hợp lệ: price={price}, high={high}, low={low}, volume={volume}")
            return
        self.price_history.append(price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(volume)

    def check_pump_protection(self, price: float) -> bool:
        if len(self.price_history) < 2:
            return False
        price_change = (price - self.price_history[-2]) / self.price_history[-2]
        return price_change > config.pump_protection_threshold

    async def check_circuit_breaker(self, price: float) -> bool:
        if len(self.price_history) < 2:
            return False
        price_change = abs(price - self.price_history[-2]) / self.price_history[-2]
        if price_change > config.circuit_breaker_threshold:
            self.circuit_breaker_triggered = True
            self.circuit_breaker_start = time.time()
            logging.warning(f"Ngắt mạch được kích hoạt: Giá thay đổi {price_change:.2%}")
            await send_telegram_alert("Pump protection: biến động giá quá lớn, tạm dừng giao dịch")
            return True
        return False

    async def check_circuit_breaker_status(self) -> bool:
        if self.circuit_breaker_triggered and self.circuit_breaker_start:
            elapsed = time.time() - self.circuit_breaker_start
            if elapsed > config.circuit_breaker_duration:
                self.circuit_breaker_triggered = False
                self.circuit_breaker_start = None
                logging.info("Ngắt mạch đã được đặt lại")
                await send_telegram_alert("Ngắt mạch đã được đặt lại")
                return False
            return True
        return False

    def check_abnormal_activity(self, volume: float) -> bool:
        if len(self.volume_history) < 2:
            return False
        avg_volume = np.mean(list(self.volume_history)[:-1])
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

# ======== QUẢN LÝ GRID NÂNG CAO ========
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
        if not isinstance(current_price, (int, float)) or current_price <= 0:
            logging.warning(f"Giá hiện tại không hợp lệ: {current_price}")
            return

        self.price_history.append(current_price)
        if not config.adaptive_grid_enabled or len(self.price_history) < config.volatility_window:
            self.grid_levels = self._create_static_grid(current_price)
            return

        atr = calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)
        atr = atr or current_price * 0.001
        volatility = min(atr / current_price, 0.1)
        num_levels = max(config.min_grid_levels, min(config.max_grid_levels, int(volatility / (config.base_grid_step_percent / 100))))
        raw_step = volatility / num_levels if num_levels > 0 else (config.base_grid_step_percent / 100)
        
        if config.strategy_mode == 1:
            step_percent = max(config.wide_grid_step_percent_min / 100, min(config.wide_grid_step_percent_max / 100, raw_step))
        else:
            step_percent = max(config.dense_grid_step_percent_min / 100, min(config.dense_grid_step_percent_max / 100, raw_step))

        self.grid_levels = []
        max_distance = 0.05
        for i in range(num_levels):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                continue
            if abs(buy_price - current_price) / current_price > max_distance or abs(sell_price - current_price) / current_price > max_distance:
                continue
            self.grid_levels.append((buy_price, sell_price))
        logging.info(f"Grid thích ứng: {len(self.grid_levels)} mức, bước {step_percent:.2%}")

    def _create_static_grid(self, current_price: float) -> List[Tuple[float, float]]:
        grid = []
        step_percent = config.base_grid_step_percent / 100
        for i in range(max(self.static_levels, config.min_grid_levels)):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                continue
            if abs(buy_price - current_price) / current_price > 0.05 or abs(sell_price - current_price) / current_price > 0.05:
                continue
            grid.append((buy_price, sell_price))
        return grid

    def duplicate_order_exists(self, side: str, price: float, open_orders: List[Dict]) -> bool:
        target = round(price, self.price_precision)
        for order in open_orders:
            if 'price' not in order or order['price'] is None:
                continue
            order_price = round(float(order['price']), self.price_precision)
            if order['side'].upper() == side.upper() and order_price == target:
                return True
        return False

    async def place_static_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        if config.strategy_mode == 2:
            logging.info("Chế độ 2: Bỏ qua đặt lệnh static.")
            return
        current_time = time.time()
        last_time = getattr(self, 'last_static_order_time', 0)
        if current_time - last_time < config.order_cooldown_sec:
            wait = config.order_cooldown_sec - (current_time - last_time)
            logging.info(f"Chưa hết cooldown (còn {wait:.1f}s) cho lệnh static")
            return

        rsi_val = calculate_rsi(list(self.price_history), config.rsi_period) if len(self.price_history) >= config.rsi_period else 50.0
        if not (40 <= rsi_val <= 60):
            logging.info(f"Thị trường không ổn định (RSI={rsi_val:.2f}), bỏ qua lệnh static")
            return

        if self.static_orders_placed:
            logging.info("Lệnh static đã được đặt, bỏ qua")
            return

        async with order_lock:
            try:
                open_orders = await safe_api_call(exchange.get_open_orders)
            except Exception as e:
                logging.error(f"Lỗi khi lấy lệnh mở: {str(e)}")
                open_orders = []
            filters = await exchange.get_symbol_filters()
            num_levels = self.static_levels
            allocated_funds_per_order = (config.initial_investment * (1 - config.capital_reserve_percent / 100)) / (2 * num_levels)
            quantity_per_level = allocated_funds_per_order / current_price if current_price > 0 else 0.0
            quantity = max(filters['minQty'], quantity_per_level)
            quantity = min(quantity, filters['maxQty'])
            step_size = filters.get('stepSize', 0.00001)
            if step_size <= 0:
                step_size = 0.00001
            quantity = round(quantity - (quantity % step_size), 6)
            if quantity < filters['minQty'] or quantity > filters['maxQty']:
                logging.warning(f"Số lượng không hợp lệ: {quantity}")
                await send_telegram_alert(f"Số lượng không hợp lệ: {quantity}")
                return

            static_grid = self._create_static_grid(current_price)[:num_levels]
            for buy_price, sell_price in static_grid:
                try:
                    if buy_price <= 0 or sell_price <= 0:
                        continue
                    if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                        buy_order = await safe_api_call(
                            exchange.place_order,
                            side='BUY',
                            order_type='LIMIT',
                            quantity=quantity,
                            price=buy_price
                        )
                        if 'orderId' not in buy_order:
                            continue
                        self.order_ids.append(buy_order['orderId'])
                        self.static_order_ids.append(buy_order['orderId'])
                        logging.info(f"(Static) Đã đặt lệnh MUA tại {buy_price:.2f}, SL: {quantity:.6f}")
                except Exception as e:
                    logging.error(f"(Static) Lỗi khi đặt lệnh: {str(e)}")
                    await send_telegram_alert(f"(Static) Lỗi khi đặt lệnh: {str(e)}")
            self.static_orders_placed = True
            self.last_static_order_time = current_time
            logging.info("Đã hoàn tất đặt lệnh static")

    async def place_grid_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        async with order_lock:
            try:
                open_orders = await safe_api_call(exchange.get_open_orders)
            except Exception as e:
                logging.error(f"Lỗi khi lấy lệnh mở: {str(e)}")
                open_orders = []
            filters = await exchange.get_symbol_filters()
            num_levels = len(self.grid_levels)
            if num_levels == 0:
                logging.warning("Không có mức grid nào được tính toán")
                return
            allocated_funds_per_order = (config.initial_investment * (1 - config.capital_reserve_percent / 100)) / (2 * num_levels)
            quantity_per_level = allocated_funds_per_order / current_price if current_price > 0 else 0.0
            quantity = max(filters['minQty'], quantity_per_level)
            quantity = min(quantity, filters['maxQty'])
            step_size = filters.get('stepSize', 0.00001)
            if step_size <= 0:
                step_size = 0.00001
            quantity = round(quantity - (quantity % step_size), 6)
            if quantity < filters['minQty'] or quantity > filters['maxQty']:
                logging.warning(f"Số lượng không hợp lệ: {quantity}")
                await send_telegram_alert(f"Số lượng không hợp lệ: {quantity}")
                return

            open_count = len(open_orders)
            current_time = time.time()

            for i, (buy_price, sell_price) in enumerate(self.grid_levels[self.static_levels:], start=self.static_levels):
                if config.max_open_orders and open_count >= config.max_open_orders:
                    logging.warning(f"Đạt giới hạn lệnh tối đa ({open_count}/{config.max_open_orders})")
                    await send_telegram_alert(f"Đạt giới hạn lệnh tối đa ({open_count}/{config.max_open_orders})")
                    break
                try:
                    if buy_price <= 0 or sell_price <= 0:
                        continue
                    if buy_price < current_price:
                        price_key = ("BUY", round(buy_price, self.price_precision))
                        if current_time < self.grid_cooldown.get(price_key, 0):
                            continue
                        if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                            buy_order = await safe_api_call(
                                exchange.place_order,
                                side='BUY',
                                order_type='LIMIT',
                                quantity=quantity,
                                price=buy_price
                            )
                            if 'orderId' not in buy_order:
                                continue
                            self.order_ids.append(buy_order['orderId'])
                            open_count += 1
                            logging.info(f"(Dynamic) Đã đặt lệnh MUA tại {buy_price:.2f}, SL: {quantity:.6f}")
                    if sell_price > current_price:
                        price_key = ("SELL", round(sell_price, self.price_precision))
                        if current_time < self.grid_cooldown.get(price_key, 0):
                            continue
                        if not self.duplicate_order_exists("SELL", sell_price, open_orders):
                            if config.max_open_orders and open_count >= config.max_open_orders:
                                logging.warning("Đạt giới hạn lệnh tối đa")
                                break
                            sell_order = await safe_api_call(
                                exchange.place_order,
                                side='SELL',
                                order_type='LIMIT',
                                quantity=quantity,
                                price=sell_price
                            )
                            if 'orderId' not in sell_order:
                                continue
                            self.order_ids.append(sell_order['orderId'])
                            open_count += 1
                            logging.info(f"(Dynamic) Đã đặt lệnh BÁN tại {sell_price:.2f}, SL: {quantity:.6f}")
                except Exception as e:
                    logging.error(f"(Dynamic) Lỗi khi đặt lệnh: {str(e)}")
                    await send_telegram_alert(f"(Dynamic) Lỗi khi đặt lệnh: {str(e)}")

            has_buy_order = any(o['side'].upper() == 'BUY' for o in open_orders)
            if not has_buy_order and len([o for o in open_orders if o['side'].upper() == 'BUY']) < 2 and current_time - self.last_fallback_time > self.FALLBACK_COOLDOWN:
                buy_price = current_price * (1 - config.base_grid_step_percent / 200)
                if buy_price > 0 and not self.duplicate_order_exists("BUY", buy_price, open_orders):
                    buy_order = await safe_api_call(
                        exchange.place_order,
                        side='BUY',
                        order_type='LIMIT',
                        quantity=quantity,
                        price=buy_price
                    )
                    if 'orderId' in buy_order:
                        self.order_ids.append(buy_order['orderId'])
                        self.last_fallback_time = current_time
                        logging.info(f"(Fallback) Đã đặt lệnh MUA tại {buy_price:.2f}, SL: {quantity:.6f}")

    async def cancel_all_orders(self, exchange: 'ExchangeInterface', preserve_static: bool = True) -> None:
        async with order_lock:
            orders_to_cancel = self.order_ids.copy()
            if preserve_static:
                orders_to_cancel = [oid for oid in orders_to_cancel if oid not in self.static_order_ids]
            for order_id in orders_to_cancel:
                try:
                    await safe_api_call(exchange.cancel_order, order_id)
                    if order_id in self.order_ids:
                        self.order_ids.remove(order_id)
                except Exception as e:
                    logging.warning(f"Lỗi khi hủy lệnh {order_id}: {str(e)}")
            if not preserve_static:
                self.order_ids.clear()
                self.static_order_ids.clear()
                self.static_orders_placed = False

    def set_cooldown(self, side: str, price: float):
        key = (side.upper(), round(price, self.price_precision))
        self.grid_cooldown[key] = time.time() + config.order_cooldown_sec
        logging.info(f"Đặt cooldown cho {key} đến {self.grid_cooldown[key]:.2f}")

    def get_grid_range(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.grid_levels:
            return None, None
        lows = [l for l, _ in self.grid_levels]
        highs = [h for _, h in self.grid_levels]
        return (min(lows) if lows else None, max(highs) if highs else None)

    def _price_out_of_grid(self, current_price: float) -> bool:
        low_grid, high_grid = self.get_grid_range()
        return ((low_grid is not None and current_price < low_grid) or (high_grid is not None and current_price > high_grid))

    def update_trailing_stop_for_level(self, grid_level: Tuple[float, float], current_price: float) -> float:
        _, sell_price = grid_level
        key = ("SELL", round(sell_price, self.price_precision))
        if key not in self.trailing_stops:
            self.trailing_stops[key] = sell_price
        if current_price > self.trailing_stops[key] and current_price > config.trailing_up_activation:
            self.trailing_stops[key] = current_price * (1 - config.base_stop_loss_percent / 100)
        return self.trailing_stops[key]

    async def rebalance_if_needed(self, current_price: float, exchange: 'ExchangeInterface') -> None:
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
                logging.info(f"Đã rebalance grid mới tại giá {current_price:.2f}")

# ======== QUẢN LỆNH NÂNG CAO ========
class EnhancedOrderManager:
    def __init__(self):
        self.open_orders: List[Dict] = []
        self.is_paused: bool = False
        self.completed_orders: List[Dict] = []

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        try:
            open_orders = await safe_api_call(exchange.get_open_orders)
            for order in open_orders:
                if 'orderId' not in order:
                    logging.warning(f"Lệnh không có orderId: {order}")
                    continue
                await safe_api_call(exchange.cancel_order, order['orderId'])
            self.open_orders.clear()
            logging.info("Đã hủy tất cả lệnh mở")
        except Exception as e:
            logging.error(f"Lỗi khi hủy tất cả lệnh: {str(e)}")

    async def check_and_handle_orders(self, current_price: float, exchange: 'ExchangeInterface',
                                      profit_tracker: 'EnhancedProfitTracker', grid_manager: EnhancedSmartGridManager) -> None:
        try:
            orders = await safe_api_call(exchange.get_all_orders)
            for order in orders:
                if 'orderId' not in order:
                    continue
                if order['status'] == 'FILLED':
                    side = order['side']
                    try:
                        price = float(order['price']) if order['price'] is not None else 0.0
                        qty = float(order['executedQty']) if order['executedQty'] is not None else 0.0
                    except (ValueError, TypeError) as e:
                        logging.error(f"Giá hoặc số lượng không hợp lệ trong lệnh {order['orderId']}: {str(e)}")
                        continue
                    if price <= 0 or qty <= 0:
                        logging.warning(f"Giá hoặc số lượng không hợp lệ trong lệnh {order['orderId']}: price={price}, qty={qty}")
                        continue
                    if order['orderId'] in grid_manager.static_order_ids and side.upper() == 'BUY':
                        grid_manager.trailing_stops[("BUY", round(price, grid_manager.price_precision))] = [price, qty]
                        grid_manager.static_order_ids.remove(order['orderId'])
                        grid_manager.static_orders_placed = False
                        logging.info(f"Đã khớp lệnh static BUY tại {price:.2f}, qty {qty:.6f}")
                        await send_telegram_alert(f"Static BUY filled @ {price:.2f}, qty {qty:.6f}")
                        self.completed_orders.append({"side": side.upper(), "price": price, "quantity": qty})
                        if order['orderId'] in grid_manager.order_ids:
                            grid_manager.order_ids.remove(order['orderId'])
                        continue
                    fee = qty * price * (config.maker_fee if order['type'] == 'LIMIT' else config.taker_fee)
                    profit = (current_price - price) * qty if side.upper() == 'BUY' else (price - current_price) * qty
                    profit_tracker.record_trade(profit, fee)
                    if order['orderId'] in grid_manager.order_ids:
                        grid_manager.order_ids.remove(order['orderId'])
                    msg = (
                        f"Lệnh {side} đã khớp\n"
                        f"Giá: {price:.2f}\n"
                        f"Số lượng: {qty:.6f}\n"
                        f"Lợi nhuận: {profit:.2f} USDT\n"
                        f"Phí: {fee:.4f} USDT"
                    )
                    logging.info(f"[PAPER] Lệnh {side.upper()} đã khớp: Giá {price:.2f}, SL: {qty:.6f}, Lợi nhuận {profit:.2f} USDT")
                    self.completed_orders.append({"side": side.upper(), "price": price, "quantity": qty})
                    await send_telegram_alert(msg)
                    if side.upper() == "BUY":
                        grid_manager.set_cooldown("BUY", price)
                    if side.upper() == "SELL":
                        grid_manager.set_cooldown("SELL", price)
                    self.open_orders = [o for o in self.open_orders if o['orderId'] != order['orderId']]
                else:
                    if not any(o['orderId'] == order['orderId'] for o in self.open_orders):
                        self.open_orders.append(order)
        except Exception as e:
            logging.error(f"Lỗi khi kiểm tra lệnh: {str(e)}")
            await send_telegram_alert(f"Lỗi khi kiểm tra lệnh: {str(e)}")

# ======== PROFIT TRACKER NÂNG CAO ========
@dataclass
class MarketData:
    price: float
    high: float
    low: float
    volume: float

    def __post_init__(self):
        if self.price <= 0 or self.high <= 0 or self.low <= 0 or self.volume < 0:
            raise ValueError(f"Dữ liệu không hợp lệ: price={self.price}, high={self.high}, low={self.low}, volume={self.volume}")

@dataclass
class Signal:
    side: str
    price: float
    quantity: float

class EnhancedProfitTracker:
    def __init__(self):
        self.total_profit: float = 0.0
        self.trade_count: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0
        self.equity_curve: List[float] = []
        self.total_fees: float = 0.0

    def record_trade(self, profit: float, fee: float) -> None:
        self.total_profit += profit
        self.trade_count += 1
        self.total_fees += fee
        if profit > 0:
            self.winning_trades += 1
        elif profit < 0:
            self.losing_trades += 1
        self.equity_curve.append(self.total_profit)

    def get_stats(self) -> Dict[str, float]:
        win_rate = (self.winning_trades / self.trade_count * 100) if self.trade_count > 0 else 0.0
        max_drawdown = calculate_max_drawdown(self.equity_curve)
        sharpe_ratio = 0.0
        return {
            "Total Profit": self.total_profit,
            "Trade Count": self.trade_count,
            "Win Rate": win_rate,
            "Max Drawdown": max_drawdown,
            "Sharpe Ratio": sharpe_ratio,
            "Total Fees": self.total_fees
        }

# ======== LƯU VÀ KHÔI PHỤC TRẠNG THÁI BOT ========
def save_state(states: Dict[str, 'BotState']) -> None:
    try:
        state_data = {}
        for name, state in states.items():
            state_data[name] = {
                "total_profit": state.tracker.total_profit,
                "trade_count": state.tracker.trade_count,
                "winning_trades": state.tracker.winning_trades,
                "losing_trades": state.tracker.losing_trades,
                "equity_curve": state.tracker.equity_curve,
                "total_fees": state.tracker.total_fees,
                "order_ids": state.grid_manager.order_ids,
                "static_order_ids": state.grid_manager.static_order_ids,
                "grid_cooldown": state.grid_manager.grid_cooldown,
                "trailing_stops": state.grid_manager.trailing_stops,
                "open_orders": state.order_manager.open_orders
            }
        with open(STATE_FILE, 'w') as f:
            json.dump(state_data, f)
        logging.info("Đã lưu trạng thái bot vào file")
    except Exception as e:
        logging.error(f"Lỗi khi lưu trạng thái bot: {str(e)}")

def load_state(states: Dict[str, 'BotState']) -> None:
    try:
        if not os.path.exists(STATE_FILE):
            logging.info("Không tìm thấy file trạng thái, khởi tạo trạng thái mới")
            return
        with open(STATE_FILE, 'r') as f:
            state_data = json.load(f)
        for name, state in states.items():
            if name in state_data:
                data = state_data[name]
                state.tracker.total_profit = data.get("total_profit", 0.0)
                state.tracker.trade_count = data.get("trade_count", 0)
                state.tracker.winning_trades = data.get("winning_trades", 0)
                state.tracker.losing_trades = data.get("losing_trades", 0)
                state.tracker.equity_curve = data.get("equity_curve", [])
                state.tracker.total_fees = data.get("total_fees", 0.0)
                state.grid_manager.order_ids = data.get("order_ids", [])
                state.grid_manager.static_order_ids = data.get("static_order_ids", [])
                state.grid_manager.grid_cooldown = data.get("grid_cooldown", {})
                ts = data.get("trailing_stops", {})
                new_ts = {}
                for k, v in ts.items():
                    if isinstance(k, str) and k.startswith("('"):
                        try:
                            tup = eval(k)
                        except Exception:
                            continue
                        new_ts[tup] = v
                    else:
                        new_ts[k] = v
                state.grid_manager.trailing_stops = new_ts
                state.order_manager.open_orders = data.get("open_orders", [])
                try:
                    if hasattr(state.exchange, "simulated_balance"):
                        base_asset = state.exchange.symbol.replace("USDT", "")
                        for (side, entry_price), val in list(state.grid_manager.trailing_stops.items()):
                            if side.upper() == "BUY" and isinstance(val, (list, tuple)) and len(val) >= 2:
                                qty = float(val[1])
                                cost = float(entry_price) * qty
                                state.exchange.simulated_balance["USDT"] -= cost
                                if base_asset not in state.exchange.simulated_balance:
                                    state.exchange.simulated_balance[base_asset] = 0.0
                                state.exchange.simulated_balance[base_asset] += qty
                except Exception as e:
                    logging.error(f"Lỗi đồng bộ số dư: {str(e)}")
                logging.info(f"Đã khôi phục trạng thái cho {name}")
    except Exception as e:
        logging.error(f"Lỗi khi khôi phục trạng thái bot: {str(e)}")

# ======== CHIẾN LƯỢC GRID NÂNG CAO ========
class GridStrategy(ABC):
    @abstractmethod
    async def generate_signals(self, data: MarketData) -> List[Signal]:
        pass

class AdaptiveGridStrategy(GridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[Signal]:
        signals = []
        if data.price is None or data.price <= 0:
            return signals
        
        self.grid_manager.price_history.append(data.price)
        self.grid_manager.high_history.append(data.high)
        self.grid_manager.low_history.append(data.low)
        await self.grid_manager.calculate_adaptive_grid(data.price)
        
        prices = list(self.grid_manager.price_history)
        rsi = calculate_rsi(prices, config.rsi_period) if len(prices) >= config.rsi_period else 50.0
        rsi_low, rsi_high = auto_adjust_rsi_thresholds(prices)
        
        logging.info(f"RSI: {rsi:.2f}, Ngưỡng: {rsi_low}/{rsi_high}")
        if rsi < rsi_low:
            signals.append(Signal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))
        elif rsi > rsi_high:
            signals.append(Signal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))
        
        return signals

class DenseGridStrategy(GridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[Signal]:
        signals = []
        if data.price is None or data.price <= 0:
            return signals
        self.grid_manager.price_history.append(data.price)
        self.grid_manager.high_history.append(data.high)
        self.grid_manager.low_history.append(data.low)
        await self.grid_manager.calculate_adaptive_grid(data.price)
        return signals

# ======== TRẠNG THÁI BOT ========
class BotState:
    def __init__(self, exchange: 'ExchangeInterface', strategy: GridStrategy):
        self.exchange: 'ExchangeInterface' = exchange
        self.exchange_name: str = exchange.exchange_name
        self.tracker: EnhancedProfitTracker = EnhancedProfitTracker()
        self.protection: EnhancedProtectionSystem = EnhancedProtectionSystem()
        self.current_price: float = 0.0
        self.current_volume: float = 0.0
        self.current_high: float = 0.0
        self.current_low: float = 0.0
        self.grid_manager: EnhancedSmartGridManager = EnhancedSmartGridManager()
        self.order_manager: EnhancedOrderManager = EnhancedOrderManager()
        self.strategy: GridStrategy = strategy
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.last_price_update: float = 0.0
        self.live_paper_trading: bool = config.live_paper_trading and self.exchange_name == "paper_trading"
        self.last_report_time = time.time()

    async def process_events(self) -> None:
        start_time = time.time()
        current_time = time.time()
        if current_time - self.last_price_update >= 5:
            for attempt in range(config.api_retry_count):
                try:
                    async with asyncio.timeout(10):  # Increased timeout to 10 seconds
                        price_task = self.exchange.get_price()
                        volume_task = self.exchange.get_volume()
                        highlow_task = self.exchange.get_high_low()
                        self.current_price, self.current_volume, high_low = await asyncio.gather(
                            price_task, volume_task, highlow_task
                        )
                        self.current_high, self.current_low = high_low
                        self.last_price_update = current_time
                        logging.info(f"Lấy dữ liệu thị trường thành công tại lần thử {attempt + 1}")
                        break
                except asyncio.TimeoutError:
                    logging.warning(f"Lần thử {attempt + 1}/{config.api_retry_count} hết thời gian")
                    if attempt < config.api_retry_count - 1:
                        await asyncio.sleep(config.api_retry_delay)
                except Exception as e:
                    logging.error(f"Lần thử {attempt + 1}/{config.api_retry_count} lỗi: {str(e)}")
                    if attempt < config.api_retry_count - 1:
                        await asyncio.sleep(config.api_retry_delay)
            else:
                logging.error("Không thể lấy dữ liệu thị trường sau tất cả các lần thử")
                await send_telegram_alert("Không thể lấy dữ liệu thị trường sau nhiều lần thử")
                return

        self.protection.update(self.current_price, self.current_high, self.current_low, self.current_volume)
        if await check_protections(self, self.exchange, self.current_price, self.current_volume, self.current_high, self.current_low):
            return

        if config.strategy_mode != 2:
            await self.grid_manager.place_static_orders(self.current_price, self.exchange)
        if config.strategy_mode == 2:
            self.grid_manager.high_history.append(self.current_high)
            self.grid_manager.low_history.append(self.current_low)
        await self.grid_manager.calculate_adaptive_grid(self.current_price, self.exchange)
        await self.grid_manager.place_grid_orders(self.current_price, self.exchange)
        signals = await self.strategy.generate_signals(MarketData(
            price=self.current_price, high=self.current_high, low=self.current_low, volume=self.current_volume
        ))
        async with order_lock:
            for signal in signals:
                try:
                    order = await self.exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
                    if 'orderId' not in order:
                        logging.error(f"Lệnh không trả về orderId: {order}")
                        continue
                    self.grid_manager.order_ids.append(order['orderId'])
                except Exception as e:
                    logging.error(f"Lỗi khi đặt lệnh: {signal.side} tại {signal.price}: {str(e)}")
        await self.order_manager.check_and_handle_orders(self.current_price, self.exchange, self.tracker, self.grid_manager)
        if config.trailing_stop_enabled:
            await self.handle_trailing_stops()
        await self.grid_manager.rebalance_if_needed(self.current_price, self.exchange)
                logging.info(f"process_events chạy mất {time.time() - start_time:.2f}s")
if config.telegram_enabled and current_time - self.last_report_time >= 3600:
            await self.send_hourly_report()

    async def handle_trailing_stops(self):
        to_remove = []
        for (side, entry_price), data in self.grid_manager.trailing_stops.items():
            if side.upper() != "BUY" or not isinstance(data, (list, tuple)) or len(data) < 2:
                continue
            highest_price, quantity = data[0], data[1]
            if self.current_price > highest_price:
                self.grid_manager.trailing_stops[(side, entry_price)][0] = self.current_price
                highest_price = self.current_price
            target_price = entry_price * (1 + config.base_take_profit_percent / 100)
            if highest_price >= target_price:
                stop_price = highest_price * (1 - config.base_stop_loss_percent / 100)
                if self.current_price <= stop_price and self.current_price >= entry_price:
                    sell_price = self.current_price
                    base_asset = config.symbol.replace("USDT", "")
                    if base_asset not in self.exchange.simulated_balance:
                        self.exchange.simulated_balance[base_asset] = 0.0
                    if self.exchange.simulated_balance[base_asset] < quantity:
                        logging.warning(f"Số dư {base_asset} không đủ: {self.exchange.simulated_balance[base_asset]} < {quantity}")
                    else:
                        self.exchange.simulated_balance[base_asset] -= quantity
                        proceeds = sell_price * quantity
                        fee = proceeds * config.taker_fee
                        self.exchange.simulated_balance["USDT"] += proceeds - fee
                        profit = (sell_price - entry_price) * quantity
                        self.tracker.record_trade(profit, fee)
                        logging.info(f"Chốt lời: Bán {quantity:.6f} {base_asset} @ {sell_price:.2f}, lợi nhuận {profit:.2f}")
                        await send_telegram_alert(f"Chốt lời: Bán {quantity:.6f} {base_asset} @ {sell_price:.2f}, lợi nhuận {profit:.2f}")
                    to_remove.append((side, entry_price))
        for key in to_remove:
            self.grid_manager.trailing_stops.pop(key, None)

    async def send_hourly_report(self):
        if not config.telegram_enabled or not config.telegram_bot_token or not config.telegram_chat_id:
            logging.error("Cấu hình Telegram không đầy đủ")
            return
        try:
            usdt_balance, coin_balance = await safe_api_call(self.exchange.get_balance)
        except Exception as e:
            logging.error(f"Lỗi khi lấy số dư: {e}")
            usdt_balance, coin_balance = 0.0, 0.0
        base_asset = config.symbol.replace("USDT", "")
        report_msg = (
            f"📊 Báo cáo sau 1 giờ:\n"
            f"Giá hiện tại: {self.current_price:.2f} {config.symbol}\n"
            f"Số dư: {usdt_balance:.2f} USDT, {coin_balance:.6f} {base_asset}\n"
            f"Tổng lợi nhuận: {self.tracker.total_profit:.2f} USDT\n"
            f"Tổng phí: {self.tracker.total_fees:.4f} USDT\n"
            f"Lệnh đang mở: {len(self.order_manager.open_orders)}\n"
            f"Lệnh đã khớp: {len(self.order_manager.completed_orders)}"
        )
        reply_markup = {
            "inline_keyboard": [
                [{"text": "Lệnh đang mở", "callback_data": "open_orders"}],
                [{"text": "Lệnh đã khớp", "callback_data": "completed_orders"}]
            ]
        }
        try:
            await send_telegram_alert(report_msg, reply_markup=reply_markup)
            logging.info("Đã gửi báo cáo hàng giờ qua Telegram")
            self.last_report_time = time.time()
        except Exception as e:
            logging.error(f"Lỗi gửi báo cáo Telegram: {e}")

async def check_protections(state: BotState, exchange: 'ExchangeInterface', price: float, volume: float, high: float, low: float) -> bool:
    if await state.protection.check_circuit_breaker(price):
        return True
    if state.protection.check_pump_protection(price):
        logging.warning("Bảo vệ bơm giá được kích hoạt")
        await send_telegram_alert("Pump protection: biến động giá quá lớn, tạm dừng giao dịch")
        return True
    if state.protection.check_abnormal_activity(volume):
        logging.warning("Phát hiện khối lượng bất thường")
        await send_telegram_alert("Khối lượng giao dịch bất thường, tạm dừng giao dịch")
        return True
    return False

# ======== GỬI CẢNH BÁO TELEGRAM ========
last_telegram_message_time = 0
TELEGRAM_RATE_LIMIT_SECONDS = 2
last_update_id = 0

async def send_telegram_alert(message: str, parse_mode: str = "Markdown", reply_markup: dict = None) -> None:
    global last_telegram_message_time
    if not config.telegram_enabled:
        return
    if not config.telegram_bot_token or not config.telegram_chat_id:
        logging.error("Token bot hoặc Chat ID của Telegram không được cấu hình đúng")
        return

    current_time = time.time()
    time_since_last_message = current_time - last_telegram_message_time
    if time_since_last_message < TELEGRAM_RATE_LIMIT_SECONDS:
        delay = TELEGRAM_RATE_LIMIT_SECONDS - time_since_last_message
        logging.warning(f"Đang bị giới hạn tốc độ Telegram, chờ {delay:.2f} giây")
        await asyncio.sleep(delay)

    try:
        message = message.replace("=", ": ").replace("%", " percent")
        url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": config.telegram_chat_id,
            "text": message[:4096],
            "parse_mode": parse_mode
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                response.raise_for_status()
                last_telegram_message_time = time.time()
                logging.info(f"Đã gửi cảnh báo Telegram: {message}")
    except aiohttp.ClientResponseError as e:
        if e.status == 429:
            retry_after = 60
            logging.warning(f"Telegram API bị giới hạn, chờ {retry_after} giây")
            await asyncio.sleep(retry_after)
            return await send_telegram_alert(message, parse_mode, reply_markup)
        elif e.status == 400:
            logging.error(f"HTTP 400: {e.message} — Thử lại không dùng parse_mode")
            payload["parse_mode"] = None
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as response:
                    response.raise_for_status()
                    last_telegram_message_time = time.time()
                    logging.info(f"Đã gửi cảnh báo Telegram không dùng định dạng: {message}")
        else:
            logging.error(f"HTTP Error {e.status}: {e.message}")
            raise
    except Exception as e:
        logging.error(f"Lỗi khi gửi cảnh báo Telegram: {str(e)}")
        raise

async def send_open_orders(state: BotState) -> None:
    try:
        open_orders = await safe_api_call(state.exchange.get_open_orders)
    except Exception as e:
        logging.error(f"Không thể lấy lệnh mở: {str(e)}")
        await send_telegram_alert("Lỗi: không thể lấy danh sách lệnh mở")
        return
    if not open_orders:
        await send_telegram_alert("Không có lệnh đang mở")
    else:
        message = "📋 Lệnh đang mở:\n"
        for o in open_orders:
            status = o.get("status", "")
            if status and status not in ["OPEN", "PARTIALLY_FILLED"]:
                continue
            side = o.get("side", "")
            price = float(o.get("price", 0))
            qty = float(o.get("quantity", 0))
            message += f"- {side} {price:.2f} x {qty:.6f}\n"
        await send_telegram_alert(message)

async def send_completed_orders(state: BotState) -> None:
    completed_orders = state.order_manager.completed_orders
    if not completed_orders:
        await send_telegram_alert("Chưa có lệnh hoàn thành nào")
    else:
        message = "✅ Lệnh đã hoàn thành:\n"
        for o in completed_orders:
            side = o.get("side", "")
            price = o.get("price", 0.0)
            qty = o.get("quantity", 0.0)
            message += f"- {side} {price:.2f} x {qty:.6f}\n"
        await send_telegram_alert(message)

async def handle_telegram_commands(state: BotState) -> None:
    global last_update_id
    if not config.telegram_enabled:
        return
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/getUpdates?offset={last_update_id + 1}&timeout=0"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=5) as response:
            data = await response.json()
            if not data.get("ok", False):
                logging.error(f"Lỗi khi lấy cập nhật Telegram: {data}")
                return
            for update in data.get("result", []):
                if 'update_id' in update:
                    last_update_id = max(last_update_id, update['update_id'])
                if "message" in update:
                    chat_id = str(update["message"]["chat"]["id"])
                    if config.telegram_chat_id and chat_id != str(config.telegram_chat_id):
                        continue
                    text = update["message"].get("text", "").lower()
                    if text == "lệnh đang mở" or text == "lenh dang mo":
                        await send_open_orders(state)
                    elif text == "lệnh đã khớp" or text == "lenh da khop":
                        await send_completed_orders(state)
                if "callback_query" in update:
                    callback = update["callback_query"]
                    data_cb = callback.get("data", "")
                    chat_id = str(callback["message"]["chat"]["id"])
                    if config.telegram_chat_id and chat_id != str(config.telegram_chat_id):
                        continue
                    if data_cb == "open_orders":
                        await send_open_orders(state)
                    elif data_cb == "completed_orders":
                        await send_completed_orders(state)
                    cb_id = callback.get("id")
                    if cb_id:
                        answer_url = f"https://api.telegram.org/bot{config.telegram_bot_token}/answerCallbackQuery"
                        async with session.post(answer_url, json={"callback_query_id": cb_id}, timeout=5):
                            pass

# ======== GIAO DIỆN SÀN GIAO DỊCH MÔ PHỎNG ========
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
        self.client: Any = None

    @abstractmethod
    async def get_price(self) -> float: pass

    @abstractmethod
    async def get_volume(self) -> float: pass

    @abstractmethod
    async def get_high_low(self) -> Tuple[float, float]: pass

    @abstractmethod
    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict: pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None: pass

    @abstractmethod
    async def get_open_orders(self) -> List[Dict]: pass

    @abstractmethod
    async def get_all_orders(self) -> List[Dict]: pass

    @abstractmethod
    async def get_balance(self) -> Tuple[float, float]: pass

    @abstractmethod
    async def get_order_book(self) -> Dict: pass

    @abstractmethod
    async def get_symbol_filters(self) -> Dict: pass

    @abstractmethod
    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame: pass

    @abstractmethod
    async def start_websocket(self, state: BotState) -> None: pass

    @abstractmethod
    async def close(self) -> None: pass

    async def health_check(self) -> Dict[str, Any]:
        try:
            start_time = time.time()
            await self.get_price()
            latency = (time.time() - start_time) * 1000
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

class PaperTradingExchange(ExchangeInterface):
    def __init__(self, symbol: str):
        super().__init__("paper_trading", "", "")
        self.symbol = symbol
        base_asset = symbol.replace("USDT", "")
        self.simulated_balance = {"USDT": config.initial_investment, base_asset: 0.0}
        self.paper_orders = []
        self.current_market_price = 100.0
        self.order_book = {"bids": [], "asks": []}

    async def get_price(self) -> float:
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            price = float(ticker.get("last", ticker.get("close", 0.0)))
            if price <= 0:
                raise ValueError("Không lấy được giá từ API")
            self.current_market_price = price
            await self._update_order_book(price)
            return price
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá thị trường: {str(e)}")
            return self.current_market_price
        finally:
            try:
                await mexc.close()
            except Exception as e_close:
                logging.error(f"Lỗi đóng kết nối MEXC: {str(e_close)}")

    async def _update_order_book(self, price: float):
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            order_book = await mexc.fetch_order_book(symbol_str, limit=10)
            self.order_book = {
                "bids": [[float(b[0]), float(b[1])] for b in order_book.get("bids", [])],
                "asks": [[float(a[0]), float(a[1])] for a in order_book.get("asks", [])]
            }
        except Exception as e:
            logging.error(f"Lỗi khi lấy order book: {str(e)}")
            spread = price * 0.001
            self.order_book = {
                "bids": [[price - spread * (i + 1), 1] for i in range(5)],
                "asks": [[price + spread * (i + 1), 1] for i in range(5)]
            }
        finally:
            await mexc.close()

    async def get_volume(self) -> float:
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            volume = float(ticker.get("quoteVolume", ticker.get("baseVolume", 0.0)))
            if volume < 0:
                volume = 0.0
            return volume
        except Exception as e:
            logging.error(f"Lỗi khi lấy khối lượng: {str(e)}")
            return 0.0
        finally:
            try:
                await mexc.close()
            except Exception as e_close:
                logging.error(f"Lỗi đóng kết nối MEXC: {str(e_close)}")

    async def get_high_low(self) -> Tuple[float, float]:
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            high_price = float(ticker.get("high", ticker.get("highPrice", 0.0)))
            low_price = float(ticker.get("low", ticker.get("lowPrice", 0.0)))
            if high_price <= 0 or low_price <= 0:
                raise ValueError("Không lấy được giá cao/thấp")
            return high_price, low_price
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá cao/thấp: {str(e)}")
            price = await self.get_price()
            return price * 1.01, price * 0.99
        finally:
            try:
                await mexc.close()
            except Exception as e_close:
                logging.error(f"Lỗi đóng kết nối MEXC: {str(e_close)}")

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        if order_type.upper() != "LIMIT" or price is None:
            raise ValueError("Chỉ hỗ trợ LIMIT order trong paper trading")
        order = {
            "orderId": str(len(self.paper_orders) + 1),
            "side": side.upper(),
            "price": price,
            "quantity": quantity,
            "type": order_type.upper(),
            "status": "OPEN",
            "timestamp": time.time(),
            "executedQty": 0.0
        }
        self.paper_orders.append(order)
        logging.info(f"PaperTradingExchange.place_order: Đã thêm lệnh {order}")
        return order

    async def _match_orders(self):
        price = await self.get_price()
        await self._update_order_book(price)
        base_asset = self.symbol.replace("USDT", "")
        if base_asset not in self.simulated_balance:
            self.simulated_balance[base_asset] = 0.0
        
        for order in self.paper_orders:
            if order["status"] != "OPEN":
                continue
            order_price = float(order["price"])
            order_qty = float(order["quantity"])
            
            if order["side"] == "BUY":
                for ask_price, ask_qty in self.order_book["asks"]:
                    if order_price >= ask_price and ask_qty >= order_qty:
                        order["status"] = "FILLED"
                        order["executedQty"] = order_qty
                        cost = order_price * order_qty
                        if self.simulated_balance["USDT"] < cost:
                            order["status"] = "CANCELED"
                            logging.warning(f"Hủy lệnh MUA: USDT không đủ {self.simulated_balance['USDT']} < {cost}")
                            break
                        self.simulated_balance["USDT"] -= cost
                        self.simulated_balance[base_asset] += order_qty
                        logging.info(f"Khớp lệnh MUA tại {order_price:.2f}, SL: {order_qty:.6f}")
                        await send_telegram_alert(f"Khớp lệnh MUA tại {order_price:.2f}, SL: {order_qty:.6f}")
                        break
            elif order["side"] == "SELL":
                for bid_price, bid_qty in self.order_book["bids"]:
                    if order_price <= bid_price and bid_qty >= order_qty:
                        order["status"] = "FILLED"
                        order["executedQty"] = order_qty
                        if self.simulated_balance[base_asset] < order_qty:
                            order["status"] = "CANCELED"
                            logging.warning(f"Hủy lệnh BÁN: {base_asset} không đủ {self.simulated_balance[base_asset]} < {order_qty}")
                            break
                        self.simulated_balance[base_asset] -= order_qty
                        self.simulated_balance["USDT"] += order_price * order_qty
                        logging.info(f"Khớp lệnh BÁN tại {order_price:.2f}, SL: {order_qty:.6f}")
                        await send_telegram_alert(f"Khớp lệnh BÁN tại {order_price:.2f}, SL: {order_qty:.6f}")
                        break

    async def get_all_orders(self) -> List[Dict]:
        await self._match_orders()
        return self.paper_orders

    async def get_open_orders(self) -> List[Dict]:
        await self._match_orders()
        return [o for o in self.paper_orders if o["status"] == "OPEN"]

    async def cancel_order(self, order_id: str) -> None:
        for order in self.paper_orders:
            if str(order["orderId"]) == str(order_id) and order["status"] == "OPEN":
                order["status"] = "CANCELED"
                break

    async def get_balance(self) -> Tuple[float, float]:
        base_asset = self.symbol.replace("USDT", "")
        if base_asset not in self.simulated_balance:
            self.simulated_balance[base_asset] = 0.0
        return self.simulated_balance["USDT"], self.simulated_balance[base_asset]

    async def get_order_book(self) -> Dict:
        await self.get_price()
        return self.order_book

    async def get_symbol_filters(self) -> Dict:
        return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            binance = ccxt_async.binance({'enableRateLimit': True})
            ohlcv = await binance.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"Không có dữ liệu lịch sử cho {self.symbol}")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            logging.info(f"Đã lấy {len(df)} dòng dữ liệu lịch sử cho {self.symbol}")
            return df
        except Exception as e:
            logging.error(f"Lỗi khi lấy dữ liệu lịch sử: {str(e)}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        finally:
            await binance.close()

    async def start_websocket(self, state: BotState) -> None:
        logging.info(f"WebSocket bị tắt cho {self.exchange_name}")
        return

    async def close(self) -> None:
        try:
            if self.client:
                await self.client.close()
                logging.info(f"Đã đóng client của {self.exchange_name}")
        except Exception as e:
            logging.error(f"Lỗi khi đóng client của {self.exchange_name}: {str(e)}")

    async def run_paper_trading_live(self, state: BotState, interval: int = 1) -> None:
        logging.info(f"Bắt đầu chạy chế độ giả lập với interval {interval} giây")
        last_ping_time = time.time()
        while True:
            try:
                health = await self.health_check()
                if health['status'] != 'healthy':
                    logging.error(f"Sàn {self.exchange_name} không ổn định: {health['status']}")
                    await asyncio.sleep(60)
                    continue
                await asyncio.wait_for(state.process_events(), timeout=30)
                if time.time() - last_ping_time >= 600:
                    logging.info("Bot vẫn đang chạy")
                    last_ping_time = time.time()
            except asyncio.TimeoutError:
                logging.warning("Vòng lặp xử lý quá thời gian, bỏ qua vòng này")
            except Exception as e:
                logging.error(f"Lỗi trong vòng lặp xử lý: {str(e)}")
                await send_telegram_alert(f"Lỗi vòng lặp: {str(e)}")
                await asyncio.sleep(60)
            await asyncio.sleep(interval)

# ======== QUẢN LÝ CÁC SÀN GIAO DỊCH ========
class ExchangeManager:
    def __init__(self):
        self.exchanges: Dict[str, ExchangeInterface] = {}
        self.exchanges["paper_trading"] = PaperTradingExchange(config.symbol)

async def safe_api_call(func, *args, **kwargs):
    try:
        result = func(*args, **kwargs)
        if hasattr(result, "__await__"):
            return await result
        return result
    except Exception as e:
        logging.error(f"API call error: {str(e)}")
        raise

# ======== MAIN ========
if __name__ == "__main__":
    async def main():
        config.paper_trading_enabled = True
        exchange = PaperTradingExchange(config.symbol)
        grid_manager = EnhancedSmartGridManager()
        strategy = AdaptiveGridStrategy(grid_manager) if config.strategy_mode == 1 else DenseGridStrategy(grid_manager)
        state = BotState(exchange, strategy)
        
        logging.info("Hủy tất cả lệnh mở trước khi khởi động bot")
        await state.order_manager.cancel_all_orders(exchange)
        await grid_manager.cancel_all_orders(exchange, preserve_static=False)
        
        states = {exchange.exchange_name: state}
        load_state(states)
        await asyncio.gather(
        exchange.run_paper_trading_live(state, interval=1),
        rebalance_loop(state)
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot đã dừng lại bởi người dùng")

async def rebalance_loop(state: BotState):
    while True:
        try:
            await state.grid_manager.rebalance_if_needed(state.current_price, state.exchange)
        except Exception as e:
            logging.error(f"Lỗi rebalance: {str(e)}")
        await asyncio.sleep(10)
