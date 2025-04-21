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

        # Cấu hình chiến lược (1: Grid thưa + RSI, 2: Grid dày không RSI)
        self.strategy_mode: int = int(os.getenv("STRATEGY_MODE", 1))
        self.wide_grid_step_percent_min: float = float(os.getenv("WIDE_GRID_STEP_PERCENT_MIN", 2.0))
        self.wide_grid_step_percent_max: float = float(os.getenv("WIDE_GRID_STEP_PERCENT_MAX", 3.0))
        self.dense_grid_step_percent_min: float = float(os.getenv("DENSE_GRID_STEP_PERCENT_MIN", 0.5))
        self.dense_grid_step_percent_max: float = float(os.getenv("DENSE_GRID_STEP_PERCENT_MAX", 1.0))
        self.capital_reserve_percent: float = float(os.getenv("CAPITAL_RESERVE_PERCENT", 0.0))
        # Điều chỉnh base_grid_step_percent theo chiến lược
        if self.strategy_mode == 1:
            self.base_grid_step_percent = self.wide_grid_step_percent_min
        elif self.strategy_mode == 2:
            self.base_grid_step_percent = self.dense_grid_step_percent_min

config = AdvancedConfig()

# Thông báo chế độ chiến lược đã chọn
logging.info(f"Đã chọn chiến lược {config.strategy_mode}: {'Grid thưa (bước ~2-3%, có RSI)' if config.strategy_mode == 1 else 'Grid dày (bước ~0.5%, không dùng RSI)'}")
logging.info(f"Bước lưới cơ bản: {config.base_grid_step_percent:.2f}%, Mức grid tối thiểu: {config.min_grid_levels}, tối đa: {config.max_grid_levels}")

# ======== HÀM TIỆN ÍCH (ATR, RSI, tính drawdown, Sharpe, tự điều chỉnh RSI) ========
def calculate_atr(highs: List[float], lows: List[float], prices: List[float], period: int) -> float:
    """Tính chỉ báo ATR (Average True Range) trên danh sách dữ liệu."""
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
    """Tính RSI (Relative Strength Index) dựa trên danh sách giá đóng cửa."""
    if len(prices) < period + 1:
        return 50.0  # Không đủ dữ liệu, trả về trung tính
    deltas = np.diff(prices)
    ups = deltas.clip(min=0)
    downs = -1 * deltas.clip(max=0)
    avg_gain = np.mean(ups[-period:]) if len(ups) >= period else 0
    avg_loss = np.mean(downs[-period:]) if len(downs) >= period else 0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100. - (100. / (1. + rs))
    return rsi

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    """Tính tỷ lệ sụt giảm lớn nhất (max drawdown) từ đường vốn."""
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
    """
    Tự điều chỉnh ngưỡng RSI dựa trên biến động giá.
    Ví dụ: nếu thị trường biến động thấp, thu hẹp ngưỡng; biến động cao, mở rộng.
    """
    rsi = calculate_rsi(prices, config.rsi_period)
    if rsi < 40:
        return (25, 65)
    elif rsi > 60:
        return (35, 75)
    else:
        return (default_low, default_high)

# ======== HỆ THỐNG BẢO VỆ NÂNG CAO (ngắt mạch, bảo vệ pump, phát hiện bất thường) ========
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
        """Cập nhật lịch sử giá/khối lượng cho hệ thống bảo vệ."""
        if price <= 0 or high <= 0 or low <= 0 or volume < 0:
            logging.warning(f"Dữ liệu không hợp lệ: price={price}, high={high}, low={low}, volume={volume}. Bỏ qua lần cập nhật này.")
            return
        self.price_history.append(price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(volume)

    def check_pump_protection(self, price: float) -> bool:
        """Kích hoạt bảo vệ pump khi giá tăng quá nhanh."""
        if len(self.price_history) < 2:
            return False
        price_change = (price - self.price_history[-2]) / self.price_history[-2]
        return price_change > config.pump_protection_threshold

    async def check_circuit_breaker(self, price: float) -> bool:
        """
        Kích hoạt ngắt mạch (tạm dừng giao dịch) khi biến động giá vượt quá ngưỡng.
        Trả về True nếu ngắt mạch đang kích hoạt.
        """
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
        """Kiểm tra xem ngắt mạch có hết hiệu lực chưa (dựa trên thời gian)."""
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
        """Phát hiện khối lượng giao dịch bất thường (tăng vọt so với trung bình)."""
        if len(self.volume_history) < 2:
            return False
        avg_volume = np.mean(list(self.volume_history)[:-1])
        return volume > avg_volume * config.abnormal_activity_threshold

    def update_trailing_stop(self, price: float) -> Tuple[bool, float]:
        """(Không sử dụng trực tiếp trong phiên bản này) Cập nhật trailing stop toàn cục."""
        if not config.trailing_stop_enabled:
            return False, self.trailing_stop_price
        if price > self.trailing_stop_price and price > config.trailing_up_activation:
            self.trailing_stop_price = price * (1 - config.base_stop_loss_percent / 100)
        elif price < self.trailing_stop_price and price < config.trailing_down_activation:
            self.trailing_stop_price = price * (1 + config.base_stop_loss_percent / 100)
        should_stop = price <= self.trailing_stop_price
        return should_stop, self.trailing_stop_price

    def update_trailing_buy(self, price: float) -> Tuple[bool, float]:
        """(Không sử dụng trực tiếp trong phiên bản này) Cập nhật trailing buy để mua bắt đáy."""
        if not config.trailing_buy_stop_enabled:
            return False, self.trailing_buy_price
        if price < self.trailing_buy_price or self.trailing_buy_price == 0:
            # Đặt mức giá mua khi giá giảm một tỷ lệ nhất định
            self.trailing_buy_price = price * (1 + config.trailing_buy_activation_percent / 100)
        should_buy = price >= self.trailing_buy_price * (1 + config.trailing_buy_distance_percent / 100)
        return should_buy, self.trailing_buy_price

# ======== QUẢN LÝ GRID NÂNG CAO ========
class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels: List[Tuple[float, float]] = []
        self.static_levels: int = 3  # Số cặp lệnh static cố định (3 buy static)

        if config.strategy_mode == 2:
            self.static_levels = 0  # Chế độ 2: không sử dụng lệnh static
        self.static_orders_placed: bool = False
        self.static_order_ids: List[str] = []
        self.order_ids: List[str] = []         # Track all open order IDs
        self.last_rebalance: float = time.time()
        self.last_fallback_time: float = 0
        self.price_history: deque = deque(maxlen=config.volatility_window)
        self.high_history: deque = deque(maxlen=config.volatility_window)
        self.low_history: deque = deque(maxlen=config.volatility_window)
        self.grid_cooldown: Dict[Tuple[str, float], float] = {}
        self.trailing_stops: Dict[Tuple[str, float], Any] = {}  # Lưu trailing stop cho từng lệnh (entry_price -> [highest_price, quantity])
        self.price_precision: int = 2
        self.FALLBACK_COOLDOWN: int = 300  # 5 phút cooldown cho fallback orders

    async def calculate_adaptive_grid(self, current_price: float, exchange: Optional['ExchangeInterface'] = None) -> None:
        """
        Tính toán các mức giá cho grid động dựa trên ATR và độ biến động.
        """
        if not isinstance(current_price, (int, float)) or current_price <= 0:
            logging.warning(f"Giá hiện tại không hợp lệ: {current_price}. Bỏ qua tính toán lưới.")
            return

        # Cập nhật lịch sử giá (phục vụ tính ATR & RSI)
        self.price_history.append(current_price)
        if not config.adaptive_grid_enabled or len(self.price_history) < config.volatility_window:
            # Nếu không bật grid thích ứng hoặc chưa đủ dữ liệu lịch sử, sử dụng grid tĩnh
            self.grid_levels = self._create_static_grid(current_price)
            return

        # Đồng bộ độ dài history (tránh lệch nhau)
        if len(self.high_history) != len(self.low_history) or len(self.low_history) != len(self.price_history):
            logging.warning("Đồng bộ các lịch sử giá do độ dài không khớp")
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
            # Đảm bảo bước lưới nằm trong khoảng 2-3% (chiến lược 1)
            step_percent = max(config.wide_grid_step_percent_min / 100, min(config.wide_grid_step_percent_max / 100, raw_step))
        elif config.strategy_mode == 2:
            # Đảm bảo bước lưới khoảng ~0.5% (chiến lược 2)
            step_percent = max(config.dense_grid_step_percent_min / 100, min(config.dense_grid_step_percent_max / 100, raw_step))
        else:
            step_percent = raw_step

        self.grid_levels = []
        for i in range(num_levels):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            if buy_price < current_price * 0.9 or sell_price > current_price * 1.1:
                logging.warning(f"Giá vượt quá ngưỡng cho phép: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            self.grid_levels.append((buy_price, sell_price))
        logging.info(f"Đã tính toán lưới thích ứng: {len(self.grid_levels)} mức, bước {step_percent:.2%}")

    def _create_static_grid(self, current_price: float) -> List[Tuple[float, float]]:
        """
        Tạo các mức giá cho grid tĩnh xung quanh current_price theo step cố định.
        """
        grid = []
        step_percent = config.base_grid_step_percent / 100
        for i in range(max(self.static_levels, config.min_grid_levels)):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            if buy_price < current_price * 0.9 or sell_price > current_price * 1.1:
                logging.warning(f"Giá vượt quá ngưỡng cho phép: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            grid.append((buy_price, sell_price))
        return grid

    def duplicate_order_exists(self, side: str, price: float, open_orders: List[Dict]) -> bool:
        """Kiểm tra xem đã có lệnh mở nào cùng side và gần giá đó chưa (để tránh đặt trùng lệnh)."""
        target = round(price, self.price_precision)
        for order in open_orders:
            if 'price' not in order or order['price'] is None:
                continue
            order_price = round(float(order['price']), self.price_precision)
            if order['side'].upper() == side.upper() and order_price == target:
                return True
        return False

    async def place_static_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        # Static orders chỉ được đặt nếu thị trường ổn định (theo ATR & RSI) và đã hết cooldown
        if config.strategy_mode == 2:
            logging.info("Chế độ 2: Bỏ qua đặt lệnh static.")
            return
        current_time = time.time()
        last_time = getattr(self, 'last_static_order_time', 0)
        if current_time - last_time < config.order_cooldown_sec:
            wait = config.order_cooldown_sec - (current_time - last_time)
            logging.info(f"Chưa hết thời gian cooldown (còn {wait:.1f}s) cho lệnh static, bỏ qua.")
            return

        # Kiểm tra điều kiện thị trường ổn định (static thông minh)
        market_data = MarketData(price=current_price, high=current_price * 1.01, low=current_price * 0.99, volume=1000)
        rsi_val = calculate_rsi(list(self.price_history), config.rsi_period) if len(self.price_history) >= config.rsi_period else 50.0
        if not (40 <= rsi_val <= 60):
            logging.info(f"Thị trường không đủ ổn định (RSI={rsi_val:.2f}), bỏ qua lệnh static.")
            return

        if self.static_orders_placed:
            logging.info("Lệnh static đã được đặt, bỏ qua.")
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
                logging.warning(f"stepSize không hợp lệ: {step_size}. Sử dụng giá trị mặc định 0.00001")
                step_size = 0.00001
            quantity = round(quantity - (quantity % step_size), 6)
            if quantity < filters['minQty'] or quantity > filters['maxQty']:
                logging.warning(f"Số lượng không hợp lệ: {quantity} (min: {filters['minQty']}, max: {filters['maxQty']})")
                await send_telegram_alert(f"Số lượng không hợp lệ: {quantity} (min: {filters['minQty']}, max: {filters['maxQty']})")
                return

            static_grid = self._create_static_grid(current_price)[:num_levels]
            for buy_price, sell_price in static_grid:
                try:
                    if buy_price <= 0 or sell_price <= 0:
                        logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
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
                            logging.error(f"Lệnh MUA static không trả về orderId: {buy_order}")
                            continue
                        self.order_ids.append(buy_order['orderId'])
                        self.static_order_ids.append(buy_order['orderId'])
                        logging.info(f"(Static) Đã đặt lệnh MUA tại {buy_price:.2f}, số lượng: {quantity:.6f}, orderId: {buy_order['orderId']}")
                        await send_telegram_alert(f"(Static) BUY: {buy_price:.2f}, SL: {quantity:.6f}")
                    # Lệnh SELL static KHÔNG được đặt trước – trailing stop sẽ xử lý chốt lời
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
                logging.warning("Không có mức grid nào được tính toán.")
                return
            allocated_funds_per_order = (config.initial_investment * (1 - config.capital_reserve_percent / 100)) / (2 * num_levels)
            quantity_per_level = allocated_funds_per_order / current_price if current_price > 0 else 0.0
            quantity = max(filters['minQty'], quantity_per_level)
            quantity = min(quantity, filters['maxQty'])
            step_size = filters.get('stepSize', 0.00001)
            if step_size <= 0:
                logging.warning(f"stepSize không hợp lệ: {step_size}. Sử dụng giá trị mặc định 0.00001")
                step_size = 0.00001
            quantity = round(quantity - (quantity % step_size), 6)
            if quantity < filters['minQty'] or quantity > filters['maxQty']:
                logging.warning(f"Số lượng không hợp lệ: {quantity} (min: {filters['minQty']}, max: {filters['maxQty']})")
                await send_telegram_alert(f"Số lượng không hợp lệ: {quantity} (min: {filters['minQty']}, max: {filters['maxQty']})")
                return

            open_count = len(open_orders)
            current_time = time.time()

            # Đặt các lệnh dynamic (ngoài các mức static cố định)
            for i, (buy_price, sell_price) in enumerate(self.grid_levels[self.static_levels:], start=self.static_levels):
                if config.max_open_orders and open_count >= config.max_open_orders:
                    logging.warning(f"Số lệnh mở đạt giới hạn tối đa ({open_count}/{config.max_open_orders}), dừng đặt thêm lệnh grid.")
                    break
                try:
                    if buy_price <= 0 or sell_price <= 0:
                        logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                        continue
                    if buy_price < current_price:
                        price_key = ("BUY", round(buy_price, self.price_precision))
                        if current_time < self.grid_cooldown.get(price_key, 0):
                            logging.info(f"(Dynamic) Chưa hết cooldown cho giá BUY {buy_price:.2f}, bỏ qua.")
                        elif not self.duplicate_order_exists("BUY", buy_price, open_orders):
                            buy_order = await safe_api_call(
                                exchange.place_order,
                                side='BUY',
                                order_type='LIMIT',
                                quantity=quantity,
                                price=buy_price
                            )
                            if 'orderId' not in buy_order:
                                logging.error(f"Lệnh MUA động không trả về orderId: {buy_order}")
                                continue
                            self.order_ids.append(buy_order['orderId'])
                            open_count += 1
                            has_buy_order = True
                            logging.info(f"(Dynamic) Đã đặt lệnh MUA tại {buy_price:.2f}, số lượng: {quantity:.6f}, orderId: {buy_order['orderId']}")
                            await send_telegram_alert(f"(Dynamic) BUY: {buy_price:.2f}, SL: {quantity:.6f}")
                        else:
                            logging.info(f"(Dynamic) Bỏ qua lệnh BUY tại {buy_price:.2f} do đã tồn tại lệnh trùng.")
                    if sell_price > current_price:
                        price_key = ("SELL", round(sell_price, self.price_precision))
                        if current_time < self.grid_cooldown.get(price_key, 0):
                            logging.info(f"(Dynamic) Chưa hết cooldown cho giá SELL {sell_price:.2f}, bỏ qua.")
                        elif not self.duplicate_order_exists("SELL", sell_price, open_orders):
                            if config.max_open_orders and open_count >= config.max_open_orders:
                                logging.warning("Số lệnh mở đạt giới hạn tối đa, dừng đặt thêm lệnh grid.")
                                break
                            sell_order = await safe_api_call(
                                exchange.place_order,
                                side='SELL',
                                order_type='LIMIT',
                                quantity=quantity,
                                price=sell_price
                            )
                            if 'orderId' not in sell_order:
                                logging.error(f"Lệnh BÁN động không trả về orderId: {sell_order}")
                                continue
                            self.order_ids.append(sell_order['orderId'])
                            open_count += 1
                            logging.info(f"(Dynamic) Đã đặt lệnh BÁN tại {sell_price:.2f}, số lượng: {quantity:.6f}, orderId: {sell_order['orderId']}")
                            await send_telegram_alert(f"(Dynamic) SELL: {sell_price:.2f}, SL: {quantity:.6f}")
                        else:
                            logging.info(f"(Dynamic) Bỏ qua lệnh SELL tại {sell_price:.2f} do đã tồn tại lệnh trùng.")
                except Exception as e:
                    logging.error(f"(Dynamic) Lỗi khi đặt lệnh: {str(e)}")
                    await send_telegram_alert(f"(Dynamic) Lỗi khi đặt lệnh: {str(e)}")
            # Fallback: nếu không có lệnh BUY nào, đặt một lệnh mua gần để đảm bảo luôn có vị thế
            has_buy_order = any(o['side'].upper() == 'BUY' for o in open_orders)

            if (not has_buy_order and
                len([o for o in open_orders if o['side'].upper() == 'BUY']) < 2 and
                current_time - self.last_fallback_time > self.FALLBACK_COOLDOWN):
                try:
                    buy_price = current_price * (1 - config.base_grid_step_percent / 200)
                    if buy_price > 0 and not self.duplicate_order_exists("BUY", buy_price, open_orders):
                        buy_order = await safe_api_call(
                            exchange.place_order,
                            side='BUY',
                            order_type='LIMIT',
                            quantity=quantity,
                            price=buy_price
                        )
                        if 'orderId' not in buy_order:
                            logging.error(f"Lệnh MUA fallback không trả về orderId: {buy_order}")
                            return
                        self.order_ids.append(buy_order['orderId'])
                        self.last_fallback_time = current_time
                        logging.info(f"(Fallback) Đã đặt lệnh MUA tại {buy_price:.2f}, số lượng: {quantity:.6f}, orderId: {buy_order['orderId']}")
                        await send_telegram_alert(f"(Fallback) BUY: {buy_price:.2f}, SL: {quantity:.6f}")
                    else:
                        logging.info(f"(Fallback) Bỏ qua lệnh BUY tại {buy_price:.2f} do đã tồn tại lệnh trùng.")
                except Exception as e:
                    logging.error(f"(Fallback) Lỗi khi đặt lệnh MUA: {str(e)}")
                    await send_telegram_alert(f"(Fallback) Lỗi khi đặt lệnh MUA:  {str(e)}")

    async def cancel_all_orders(self, exchange: 'ExchangeInterface', preserve_static: bool = True) -> None:
        """
        Hủy tất cả các lệnh đang mở. Nếu preserve_static=True, giữ lại các lệnh static chưa khớp.
        """
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
        """Đặt khoảng thời gian cooldown cho một mức giá sau khi vừa khớp lệnh."""
        key = (side.upper(), round(price, self.price_precision))
        self.grid_cooldown[key] = time.time() + config.order_cooldown_sec
        logging.info(f"Đặt cooldown cho {key} đến {self.grid_cooldown[key]:.2f}")

    def get_grid_range(self) -> Tuple[Optional[float], Optional[float]]:
        """Lấy khoảng giá thấp nhất và cao nhất của grid hiện tại."""
        if not self.grid_levels:
            return None, None
        lows = [l for l, _ in self.grid_levels]
        highs = [h for _, h in self.grid_levels]
        return (min(lows) if lows else None, max(highs) if highs else None)

    def _price_out_of_grid(self, current_price: float) -> bool:
        """Kiểm tra xem giá đã thoát khỏi phạm vi grid hiện tại chưa."""
        low_grid, high_grid = self.get_grid_range()
        return ((low_grid is not None and current_price < low_grid) or 
                (high_grid is not None and current_price > high_grid))

    def update_trailing_stop_for_level(self, grid_level: Tuple[float, float], current_price: float) -> float:
        """
        (Không dùng trong phiên bản mới) Cập nhật mức giá trailing stop cho một cặp grid cụ thể.
        """
        _, sell_price = grid_level
        key = ("SELL", round(sell_price, self.price_precision))
        if key not in self.trailing_stops:
            self.trailing_stops[key] = sell_price
        if current_price > self.trailing_stops[key] and current_price > config.trailing_up_activation:
            self.trailing_stops[key] = current_price * (1 - config.base_stop_loss_percent / 100)
        return self.trailing_stops[key]

    async def rebalance_if_needed(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        """
        Rebalance lưới nếu cần (khi vượt quá khoảng giá hoặc sau khoảng thời gian nhất định).
        """
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

# ======== QUẢN LÝ LỆNH NÂNG CAO ========
class EnhancedOrderManager:
    def __init__(self):
        self.open_orders: List[Dict] = []
        self.is_paused: bool = False
        self.completed_orders: List[Dict] = []

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        """Hủy tất cả các lệnh mở (không phân biệt static/dynamic)."""
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
        """
        Kiểm tra trạng thái các lệnh (đã khớp hay chưa) và xử lý chốt lời/cập nhật trạng thái khi lệnh khớp.
        """
        try:
            orders = await safe_api_call(exchange.get_all_orders)
            for order in orders:
                if 'orderId' not in order:
                    logging.warning(f"Lệnh không có orderId: {order}")
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
                    # Xử lý lệnh static BUY: thêm vị thế vào trailing_stops, không chốt lời ngay
                    if order['orderId'] in grid_manager.static_order_ids and side.upper() == 'BUY':
                        grid_manager.trailing_stops[("BUY", round(price, grid_manager.price_precision))] = [price, qty]
                        grid_manager.static_order_ids.remove(order['orderId'])
                        grid_manager.static_orders_placed = False
                        logging.info(f"Đã khớp lệnh static BUY tại {price:.2f}, qty {qty:.6f}. Thêm trailing stop để chốt lời.")
                        await send_telegram_alert(f"Static BUY filled @ {price:.2f}, qty {qty:.6f} -> sẽ chốt lời với trailing stop")
                        self.completed_orders.append({"side": side.upper(), "price": price, "quantity": qty})
                        # Không tính lợi nhuận ngay cho lệnh BUY static
                        if order['orderId'] in grid_manager.order_ids:
                            grid_manager.order_ids.remove(order['orderId'])
                        continue
                    # Tính phí và lợi nhuận cho các lệnh còn lại (dynamic hoặc static SELL)
                    fee = qty * price * (config.maker_fee if order['type'] == 'LIMIT' else config.taker_fee)
                    profit = (current_price - price) * qty if side.upper() == 'BUY' else (price - current_price) * qty
                    profit_tracker.record_trade(profit, fee)
                    # Xóa order_id khỏi danh sách order_ids tránh trùng lệnh
                    if order['orderId'] in grid_manager.order_ids:
                        grid_manager.order_ids.remove(order['orderId'])
                    msg = (f"Lệnh {side} đã khớp\n"
                           f"Giá: {price:.2f}\n"
                           f"Số lượng: {qty:.6f}\n"
                           f"Lợi nhuận: {profit:.2f} USDT\n"
                           f"Phí: {fee:.4f} USDT")
                    logging.info(f"Lệnh {side.upper()} đã khớp: Giá {price:.2f}, Số lượng {qty:.6f}, Lợi nhuận {profit:.2f} USDT, Phí {fee:.4f} USDT")
                    self.completed_orders.append({"side": side.upper(), "price": price, "quantity": qty})
                    await send_telegram_alert(msg)
                    if side.upper() == "BUY":
                        grid_manager.set_cooldown("BUY", price)
                    if side.upper() == "SELL":
                        grid_manager.set_cooldown("SELL", price)
                    # Cập nhật danh sách lệnh mở
                    self.open_orders = [o for o in self.open_orders if o['orderId'] != order['orderId']]
                else:
                    # Cập nhật self.open_orders (thêm các lệnh mới khớp một phần hoặc chưa khớp)
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
        """Ghi nhận kết quả của một trade (sau khi lệnh được khớp hoàn toàn)."""
        self.total_profit += profit
        self.trade_count += 1
        self.total_fees += fee
        if profit > 0:
            self.winning_trades += 1
        elif profit < 0:
            self.losing_trades += 1
        self.equity_curve.append(self.total_profit)

    def get_stats(self) -> Dict[str, float]:
        """Trả về thống kê hiệu suất hiện tại."""
        win_rate = (self.winning_trades / self.trade_count * 100) if self.trade_count > 0 else 0.0
        max_drawdown = calculate_max_drawdown(self.equity_curve)
        sharpe_ratio = 0.0  # (có thể bổ sung tính Sharpe nếu cần)
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
    """Lưu trạng thái bot (hiệu suất, lệnh mở, trailing stops,...) vào file JSON."""
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
    """Khôi phục trạng thái bot từ file JSON đã lưu."""
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

                # Chuyển đổi khóa trailing_stops từ chuỗi về tuple nếu cần
                ts = data.get("trailing_stops", {})
                new_ts = {}
                for k, v in ts.items():
                    if isinstance(k, str) and k.startswith("('"):
                        try:
                            tup = eval(k)  # chuyển chuỗi tuple thành tuple Python
                        except Exception as e:
                            continue
                        new_ts[tup] = v
                    else:
                        new_ts[k] = v
                state.grid_manager.trailing_stops = new_ts
                state.order_manager.open_orders = data.get("open_orders", [])
                # Đồng bộ số dư cho các vị thế đang giữ (static buys chưa bán)
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

# ======== CHIẾN LƯỢC GRID NÂNG CAO (Adaptive Grid + RSI) ========
class GridStrategy(ABC):
    @abstractmethod
    async def generate_signals(self, data: MarketData) -> List[Signal]:
        pass

class AdaptiveGridStrategy(GridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[Signal]:
        """
        Phát sinh các tín hiệu giao dịch động dựa trên giá hiện tại và chỉ báo RSI.
        Bao gồm tín hiệu BUY/SELL chủ động nếu RSI vượt ngưỡng (30/70).
        """
        try:
            signals: List[Signal] = []
            if data.price is None or data.high is None or data.low is None or data.price <= 0:
                logging.warning(f"Dữ liệu không đầy đủ: price={data.price}, high={data.high}, low={data.low}. Bỏ qua lần cập nhật này.")
                return signals
            # Cập nhật lịch sử giá cao/thấp/đóng để phục vụ tính RSI và grid động
            self.grid_manager.price_history.append(data.price)
            self.grid_manager.high_history.append(data.high)
            self.grid_manager.low_history.append(data.low)
            await self.grid_manager.calculate_adaptive_grid(data.price)
            # Tính RSI hiện tại
            prices = list(self.grid_manager.price_history)
            rsi = calculate_rsi(prices, config.rsi_period) if len(prices) >= config.rsi_period else 50.0
            if rsi > 70:
                logging.info(f"RSI {rsi:.2f} > 70, ưu tiên tín hiệu BÁN")
            elif rsi < 30:
                logging.info(f"RSI {rsi:.2f} < 30, ưu tiên tín hiệu MUA")
            else:
                logging.info(f"RSI {rsi:.2f}, không có xu hướng rõ ràng")
            # Tín hiệu động từ RSI (mua/bán tích cực khi RSI quá mua/quá bán)
            if rsi < 30:
                # Mua tích cực khi RSI quá thấp (oversold)
                buy_quantity = config.min_quantity
                signals.append(Signal(side="BUY", price=data.price, quantity=buy_quantity))
            if rsi > 70:
                # Bán tích cực khi RSI quá cao (overbought)
                sell_quantity = config.min_quantity
                signals.append(Signal(side="SELL", price=data.price, quantity=sell_quantity))
            return signals
        except Exception as e:
            logging.error(f"Lỗi khi tạo tín hiệu: {str(e)}")
            return []

class DenseGridStrategy(GridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[Signal]:
        """Chiến lược Grid dày: không dùng RSI, chỉ cập nhật lưới liên tục."""
        try:
            signals: List[Signal] = []
            if data.price is None or data.price <= 0:
                return signals
            # Cập nhật lịch sử giá để tính ATR (không dùng RSI)
            self.grid_manager.price_history.append(data.price)
            self.grid_manager.high_history.append(data.high)
            self.grid_manager.low_history.append(data.low)
            # Tính toán lưới thích ứng với dữ liệu mới
            await self.grid_manager.calculate_adaptive_grid(data.price)
            # Không tạo tín hiệu BUY/SELL chủ động trong chiến lược grid dày
            return signals
        except Exception as e:
            logging.error(f"Lỗi khi tạo tín hiệu (grid dày): {str(e)}")
            return []

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
        # Nếu sử dụng paper trading trực tiếp (live_paper_trading), có thể kích hoạt thêm logic đặc biệt
        self.live_paper_trading: bool = config.live_paper_trading and self.exchange_name == "paper_trading"
        self.last_report_time = time.time()

    async def process_events(self) -> None:
        """Xử lý một chu kỳ sự kiện: cập nhật giá, kiểm tra bảo vệ, đặt lệnh và xử lý kết quả."""
        current_time = time.time()
        # Cập nhật giá thị trường mỗi 5 giây một lần (hoặc theo chu kỳ cấu hình)
        if current_time - self.last_price_update >= 5:
            try:
                self.current_price = await safe_api_call(self.exchange.get_price)
                self.current_volume = await safe_api_call(self.exchange.get_volume)
                self.current_high, self.current_low = await safe_api_call(self.exchange.get_high_low)
                self.last_price_update = current_time
            except Exception as e:
                logging.error(f"Lỗi khi lấy dữ liệu thị trường: {str(e)}")
                await asyncio.sleep(60)
                return
        else:
            logging.debug("Bỏ qua cập nhật giá do chưa đủ thời gian")

        # Cập nhật hệ thống bảo vệ với dữ liệu mới
        self.protection.update(self.current_price, self.current_high, self.current_low, self.current_volume)
        # Kiểm tra các điều kiện bảo vệ (pump, circuit breaker, volume bất thường)
        if await check_protections(self, self.exchange, self.current_price, self.current_volume, self.current_high, self.current_low):
            # Nếu bảo vệ kích hoạt, bỏ qua chu kỳ này (tạm dừng đặt lệnh)
            return

        # Đặt lệnh static (nếu chưa đặt và thị trường ổn định)
        if config.strategy_mode != 2:
            await self.grid_manager.place_static_orders(self.current_price, self.exchange)
        # Tính toán lại grid động (nếu bật)
        if config.strategy_mode == 2:
            self.grid_manager.high_history.append(self.current_high)
            self.grid_manager.low_history.append(self.current_low)
        await self.grid_manager.calculate_adaptive_grid(self.current_price, self.exchange)
        # Đặt các lệnh grid động (ngoài static)
        await self.grid_manager.place_grid_orders(self.current_price, self.exchange)
        # Phát sinh các tín hiệu mua/bán tích cực từ chiến lược (ví dụ RSI)
        signals = await self.strategy.generate_signals(MarketData(price=self.current_price, high=self.current_high, low=self.current_low, volume=self.current_volume))
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
        # Kiểm tra và xử lý kết quả khớp lệnh (chốt lời, cập nhật số dư, v.v.)
        await self.order_manager.check_and_handle_orders(self.current_price, self.exchange, self.tracker, self.grid_manager)
        # Kiểm tra và thực hiện chốt lời cho các vị thế static (trailing stop)
        if config.trailing_stop_enabled:
            to_remove = []
            for (side, entry_price), data in self.grid_manager.trailing_stops.items():
                if side.upper() != "BUY":
                    continue
                if not isinstance(data, list) and not isinstance(data, tuple):
                    continue
                highest_price = data[0]
                quantity = data[1]
                # Cập nhật giá cao nhất kể từ khi mua
                if self.current_price > highest_price:
                    highest_price = self.current_price
                    self.grid_manager.trailing_stops[(side, entry_price)][0] = highest_price
                # Kiểm tra điều kiện trailing stop (đã đạt ngưỡng lợi nhuận mục tiêu và giá giảm lại)
                target_price = entry_price * (1 + config.base_take_profit_percent/100)
                if highest_price >= target_price:
                    stop_price = highest_price * (1 - config.base_stop_loss_percent/100)
                    if self.current_price <= stop_price and self.current_price >= entry_price:
                        # Chốt lời: bán với giá hiện tại
                        sell_price = self.current_price
                        base_asset = config.symbol.replace("USDT", "")
                        # Cập nhật số dư mô phỏng
                        if base_asset not in self.exchange.simulated_balance:
                            self.exchange.simulated_balance[base_asset] = 0.0
                        if self.exchange.simulated_balance[base_asset] < quantity:
                            logging.warning(f"Số dư {base_asset} không đủ để bán chốt lời: có {self.exchange.simulated_balance[base_asset]}, cần {quantity}")
                        else:
                            self.exchange.simulated_balance[base_asset] -= quantity
                            proceeds = sell_price * quantity
                            fee = proceeds * config.taker_fee
                            net_proceeds = proceeds - fee
                            self.exchange.simulated_balance["USDT"] += net_proceeds
                            profit = (sell_price - entry_price) * quantity
                            self.tracker.record_trade(profit, fee)
                            logging.info(f"Đã chốt lời {quantity:.6f} {base_asset} ở giá {sell_price:.2f}, lợi nhuận {profit:.2f} USDT (đã trừ phí {fee:.4f})")
                            await send_telegram_alert(f"Chốt lời: Bán {quantity:.6f} {base_asset} @ {sell_price:.2f}, lợi nhuận {profit:.2f} USDT")
                        to_remove.append((side, entry_price))
            # Xóa các vị thế đã chốt lời khỏi danh sách theo dõi
            for key in to_remove:
                if key in self.grid_manager.trailing_stops:
                    del self.grid_manager.trailing_stops[key]
        # Rebalance lưới nếu cần (sau khi biến động mạnh hoặc định kỳ)
        await self.grid_manager.rebalance_if_needed(self.current_price, self.exchange)
        # Gửi báo cáo định kỳ hàng giờ qua Telegram và xử lý yêu cầu người dùng
        if config.telegram_enabled:
            if current_time - self.last_report_time >= 3600:
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
                        [ {"text": "Lệnh đang mở", "callback_data": "open_orders"} ],
                        [ {"text": "Lệnh đã khớp", "callback_data": "completed_orders"} ]
                    ]
                }
                await send_telegram_alert(report_msg, reply_markup=reply_markup)
                logging.info("Đã gửi báo cáo hàng giờ qua Telegram.")
                self.last_report_time = current_time
            try:
                await handle_telegram_commands(self)
            except Exception as e:
                logging.error(f"Lỗi khi xử lý lệnh Telegram: {e}")

async def check_protections(state: BotState, exchange: 'ExchangeInterface', price: float, volume: float, high: float, low: float) -> bool:
    """
    Kiểm tra các cơ chế bảo vệ trước khi tiếp tục giao dịch:
    - Ngắt mạch (giá biến động quá mạnh)
    - Bảo vệ pump (tăng giá quá nhanh)
    - Hoạt động bất thường về khối lượng
    """
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
    """Gửi cảnh báo/ thông báo qua Telegram (nếu được cấu hình)."""
    global last_telegram_message_time
    if not config.telegram_enabled:
        # Telegram chưa được bật
        return
    if not config.telegram_bot_token or not config.telegram_chat_id:
        logging.error("Token bot hoặc Chat ID của Telegram không được cấu hình đúng.")
        return

    current_time = time.time()
    time_since_last_message = current_time - last_telegram_message_time
    if time_since_last_message < TELEGRAM_RATE_LIMIT_SECONDS:
        # Chống spam: nếu gửi quá nhanh, đợi thêm
        delay = TELEGRAM_RATE_LIMIT_SECONDS - time_since_last_message
        logging.warning(f"Đang bị giới hạn tốc độ Telegram, chờ {delay:.2f} giây")
        await asyncio.sleep(delay)

    try:
        message = message.replace("=", ": ").replace("%", " percent")
        logging.info(f"Chuẩn bị gửi cảnh báo Telegram: {message}")
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
            logging.warning(f"Telegram API bị giới hạn, chờ {retry_after} giây...")
            await asyncio.sleep(retry_after)
            return await send_telegram_alert(message, parse_mode, reply_markup)
        elif e.status == 400:
            logging.error(f"HTTP 400: {e.message} — Thử lại không dùng parse_mode.")
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
        await send_telegram_alert("Lỗi: không thể lấy danh sách lệnh mở.")
        return
    if not open_orders:
        await send_telegram_alert("Không có lệnh đang mở.")
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
        await send_telegram_alert("Chưa có lệnh hoàn thành nào.")
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
                # Xử lý tin nhắn văn bản từ người dùng
                if "message" in update:
                    chat_id = str(update["message"]["chat"]["id"])
                    if config.telegram_chat_id and chat_id != str(config.telegram_chat_id):
                        continue
                    text = update["message"].get("text", "").lower()
                    if text == "lệnh đang mở" or text == "lenh dang mo":
                        await send_open_orders(state)
                    elif text == "lệnh đã khớp" or text == "lenh da khop":
                        await send_completed_orders(state)
                # Xử lý callback query từ nút bấm
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

# ======== GIAO DIỆN SÀN GIAO DỊCH MÔ PHỎNG (PAPER TRADING) ========
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
        """Kiểm tra kết nối API (lấy giá một lần để đo độ trễ)."""
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

class BaseExchange(ExchangeInterface):
    async def get_price(self) -> float:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            if 'last' not in ticker or ticker['last'] is None:
                raise ValueError("Không lấy được giá từ sàn")
            return float(ticker['last'])
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá trên {self.exchange_name}: {str(e)}")
            raise

    async def get_volume(self) -> float:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            if 'quoteVolume' not in ticker or ticker['quoteVolume'] is None:
                raise ValueError("Không lấy được khối lượng từ sàn")
            return float(ticker['quoteVolume'])
        except Exception as e:
            logging.error(f"Lỗi khi lấy khối lượng trên {self.exchange_name}: {str(e)}")
            raise

    async def get_high_low(self) -> Tuple[float, float]:
        try:
            ticker = await self.client.fetch_ticker(self.symbol)
            if 'high' not in ticker or 'low' not in ticker or ticker['high'] is None or ticker['low'] is None:
                raise ValueError("Không lấy được giá cao/thấp từ sàn")
            return float(ticker['high']), float(ticker['low'])
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá cao/thấp trên {self.exchange_name}: {str(e)}")
            raise

# Sàn Mô Phỏng (Paper Trading)
class PaperTradingExchange(ExchangeInterface):
    def __init__(self, symbol: str):
        super().__init__("paper_trading", "", "")
        self.symbol = symbol
        base_asset = symbol.replace("USDT", "")
        # Khởi tạo số dư mô phỏng: tất cả vốn ban đầu ở USDT, không có coin
        self.simulated_balance = {"USDT": config.initial_investment, base_asset: 0.0}
        self.paper_orders = []
        self.current_market_price = 100.0
        self.order_book = {"bids": [], "asks": []}

    async def get_price(self) -> float:
        try:
            mexc = ccxt_async.mexc()
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            price = float(ticker.get("last", ticker.get("close", 0.0)))
            await mexc.close()
            if price <= 0:
                raise ValueError("Không lấy được giá từ API")
            self.current_market_price = price
            self._update_order_book(price)
            return price
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá thị trường: {str(e)}")
            return self.current_market_price

    def _update_order_book(self, price: float):
        """Cập nhật sổ lệnh mô phỏng xung quanh giá hiện tại (spread 0.1%)."""
        spread = price * 0.001
        self.order_book = {
            "bids": [[price - spread * (i + 1), 1] for i in range(5)],
            "asks": [[price + spread * (i + 1), 1] for i in range(5)]
        }

    async def get_volume(self) -> float:
        try:
            mexc = ccxt_async.mexc()
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            volume = float(ticker.get("quoteVolume", ticker.get("baseVolume", 0.0)))
            await mexc.close()
            return volume if volume >= 0 else 0.0
        except Exception as e:
            logging.error(f"Lỗi khi lấy khối lượng: {str(e)}")
            return 0.0

    async def get_high_low(self) -> Tuple[float, float]:
        try:
            mexc = ccxt_async.mexc()
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            high_price = float(ticker.get("high", ticker.get("highPrice", 0.0)))
            low_price = float(ticker.get("low", ticker.get("lowPrice", 0.0)))
            await mexc.close()
            if high_price <= 0 or low_price <= 0:
                raise ValueError("Không lấy được giá cao/thấp")
            return high_price, low_price
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá cao/thấp: {str(e)}")
            price = await self.get_price()
            return price * 1.01, price * 0.99

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        """Đặt lệnh mô phỏng (chỉ hỗ trợ Limit)."""
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
        """Mô phỏng khớp lệnh dựa trên order_book hiện tại."""
        price = await self.get_price()
        base_asset = self.symbol.replace("USDT", "")
        if base_asset not in self.simulated_balance:
            self.simulated_balance[base_asset] = 0.0
        for order in self.paper_orders:
            if order["status"] == "OPEN":
                best_bid = max([b[0] for b in self.order_book["bids"]], default=price * 0.99)
                best_ask = min([a[0] for a in self.order_book["asks"]], default=price * 1.01)
                if order["side"] == "BUY" and order["price"] >= best_ask:
                    # Khớp lệnh MUA
                    order["status"] = "FILLED"
                    order["executedQty"] = order["quantity"]
                    cost = order["price"] * order["quantity"]
                    if self.simulated_balance["USDT"] < cost:
                        order["status"] = "CANCELED"
                        logging.warning(f"Hủy lệnh MUA do số dư USDT không đủ: {self.simulated_balance['USDT']} < {cost}")
                        continue
                    self.simulated_balance["USDT"] -= cost
                    self.simulated_balance[base_asset] += order["quantity"]
                    logging.info(f"Khớp lệnh MUA tại {order['price']:.2f}, số lượng: {order['quantity']:.6f}")
                    await send_telegram_alert(f"Khớp lệnh MUA tại {order['price']:.2f}, qty: {order['quantity']:.6f}")
                elif order["side"] == "SELL" and order["price"] <= best_bid:
                    # Khớp lệnh BÁN
                    order["status"] = "FILLED"
                    order["executedQty"] = order["quantity"]
                    cost = order["price"] * order["quantity"]
                    if self.simulated_balance[base_asset] < order["quantity"]:
                        order["status"] = "CANCELED"
                        logging.warning(f"Hủy lệnh BÁN do số dư {base_asset} không đủ: {self.simulated_balance[base_asset]} < {order['quantity']}")
                        continue
                    self.simulated_balance[base_asset] -= order["quantity"]
                    self.simulated_balance["USDT"] += cost
                    logging.info(f"Khớp lệnh BÁN tại {order['price']:.2f}, số lượng: {order['quantity']:.6f}")
                    await send_telegram_alert(f"Khớp lệnh BÁN tại {order['price']:.2f}, qty: {order['quantity']:.6f}")

    async def get_all_orders(self) -> List[Dict]:
        # Trước mỗi lần lấy danh sách lệnh, tiến hành khớp giả lập các lệnh có thể khớp
        await self._match_orders()
        return self.paper_orders

    async def get_open_orders(self) -> List[Dict]:
        # Cập nhật khớp lệnh rồi trả về danh sách lệnh còn mở
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
        await self.get_price()  # cập nhật order_book dựa trên giá mới
        return self.order_book

    async def get_symbol_filters(self) -> Dict:
        # Trả về giới hạn tối thiểu/tối đa và bước cho quantity (giả lập)
        return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        """Lấy dữ liệu lịch sử cho backtest (sử dụng API qua ccxt, mặc định MEXC)."""
        try:
            binance = ccxt_async.binance({'enableRateLimit': True})
            ohlcv = await binance.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"Không có dữ liệu lịch sử cho {self.symbol}")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            logging.info(f"Đã lấy {len(df)} dòng dữ liệu lịch sử cho {self.symbol} trong paper trading")
            return df
        except Exception as e:
            logging.error(f"Lỗi khi lấy dữ liệu lịch sử trong paper trading: {str(e)}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        finally:
            await binance.close()

    async def start_websocket(self, state: BotState) -> None:
        # WebSocket không sử dụng trong chế độ giả lập
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
        """Chạy bot giao dịch giả lập trực tiếp (paper trading) với khoảng nghỉ mỗi chu kỳ."""
        logging.info(f"Bắt đầu chạy chế độ giả lập với interval {interval} giây")
        try:
            while True:
                await state.process_events()
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logging.info("Đã dừng run_paper_trading_live do bị hủy bỏ")
        except Exception as e:
            logging.error(f"Lỗi trong run_paper_trading_live: {str(e)}")

# ======== QUẢN LÝ CÁC SÀN GIAO DỊCH ========
class ExchangeManager:
    def __init__(self):
        self.exchanges: Dict[str, ExchangeInterface] = {}
        # Luôn sử dụng paper trading
        self.exchanges["paper_trading"] = PaperTradingExchange(config.symbol)

# Hàm safe_api_call: gọi hàm API an toàn (có thể bổ sung retry nếu cần)
def safe_api_call(func, *args, **kwargs):
    try:
        return asyncio.run(func(*args, **kwargs))
    except Exception as e:
        logging.error(f"API call error: {str(e)}")
        raise

# ----- MAIN -----
if __name__ == "__main__":
    async def main():
        # Chạy bot ở chế độ giả lập (paper trading)
        config.paper_trading_enabled = True
        exchange = PaperTradingExchange(config.symbol)
        grid_manager = EnhancedSmartGridManager()
        if config.strategy_mode == 1:
            strategy = AdaptiveGridStrategy(grid_manager)
        else:
            strategy = DenseGridStrategy(grid_manager)
        state = BotState(exchange, strategy)
        # Khôi phục trạng thái nếu có
        states = {exchange.exchange_name: state}
        load_state(states)
        await exchange.run_paper_trading_live(state, interval=1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot đã dừng lại bởi người dùng")