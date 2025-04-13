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
import json
from dotenv import load_dotenv
import ccxt.async_support as ccxt_async
import dash
from dash import dcc, html, Output, Input
import dash_auth
import plotly.graph_objs as go
from concurrent.futures import ThreadPoolExecutor
import cachetools
import psutil
import aiohttp
import signal
import sys

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

# Cache cho API với TTL 5 phút
api_cache = cachetools.TTLCache(maxsize=100, ttl=300)

# File để lưu trạng thái bot
STATE_FILE = "bot_state.json"

# ======== QUẢN LÝ CẤU HÌNH (Lấy từ .env) ========
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges: List[str] = os.getenv("ENABLED_EXCHANGES", "binance").split(",")
        self.exchange_credentials: Dict[str, Tuple[str, str]] = {
            ex: (os.getenv(f"{ex.upper()}_API_KEY", "your_api_key"),
                 os.getenv(f"{ex.upper()}_API_SECRET", "your_api_secret"))
            for ex in self.enabled_exchanges
        }
        self.symbol: str = os.getenv("TRADING_SYMBOL", "BTCUSDT")
        self.initial_investment: float = float(os.getenv("INITIAL_INVESTMENT", 200))
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
        # Thêm cấu hình cho xác thực dashboard
        self.dashboard_username: str = os.getenv("DASHBOARD_USERNAME", "admin")
        self.dashboard_password: str = os.getenv("DASHBOARD_PASSWORD", "admin123")

config = AdvancedConfig()

# ======== GỎI CẢNH BÁO TELEGRAM ========
last_telegram_message_time = 0
TELEGRAM_RATE_LIMIT_SECONDS = 2  # Giới hạn 1 tin nhắn mỗi 2 giây

def send_telegram_alert(message: str, parse_mode: str = "Markdown") -> None:
    global last_telegram_message_time
    if not config.telegram_enabled:
        return
    
    # Kiểm tra rate limit
    current_time = time.time()
    time_since_last_message = current_time - last_telegram_message_time
    if time_since_last_message < TELEGRAM_RATE_LIMIT_SECONDS:
        logging.warning(f"Đang bị giới hạn tốc độ Telegram, chờ {TELEGRAM_RATE_LIMIT_SECONDS - time_since_last_message:.2f} giây")
        time.sleep(TELEGRAM_RATE_LIMIT_SECONDS - time_since_last_message)
    
    try:
        # Kiểm tra và sửa lỗi cú pháp Markdown
        if parse_mode == "Markdown":
            message = message.replace("Giao Dịch", "Giao Dich")  # Thay thế ký tự đặc biệt
            message = message.replace("=", ": ")  # Thay thế dấu = để tránh lỗi Markdown
        
        logging.info(f"Chuẩn bị gửi cảnh báo Telegram: {message}")
        url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": config.telegram_chat_id,
            "text": message[:4096],  # Giới hạn độ dài tin nhắn
            "parse_mode": parse_mode
        }
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        last_telegram_message_time = time.time()  # Cập nhật thời gian gửi tin nhắn
        logging.info(f"Đã gửi cảnh báo Telegram: {message}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            retry_after = int(e.response.json().get("parameters", {}).get("retry_after", 60))
            logging.warning(f"Telegram API Too Many Requests, chờ {retry_after} giây trước khi thử lại")
            time.sleep(retry_after)
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            last_telegram_message_time = time.time()
            logging.info(f"Đã gửi cảnh báo Telegram sau khi chờ: {message}")
        elif e.response.status_code == 400:
            logging.error(f"HTTP Error 400: {e.response.text}")
            payload["parse_mode"] = None
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            last_telegram_message_time = time.time()
            logging.info(f"Đã gửi cảnh báo Telegram không dùng Markdown: {message}")
        else:
            logging.error(f"HTTP Error {e.response.status_code}: {e.response.text}")
            raise
    except requests.exceptions.RequestException as e:
        logging.error(f"Lỗi mạng khi gửi cảnh báo Telegram: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Lỗi khác khi gửi cảnh báo Telegram: {str(e)}")
        raise

# ======== DATA CLASSES ========
@dataclass
class MarketData:
    price: float
    high: float
    low: float
    volume: float

@dataclass
class Signal:
    side: str
    price: float
    quantity: float

# ======== LƯU VÀ KHÔI PHỤC TRẠNG THÁI BOT ========
def save_state(states: Dict[str, 'BotState']) -> None:
    """Lưu trạng thái bot vào file JSON"""
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
    """Khôi phục trạng thái bot từ file JSON"""
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
                state.grid_manager.grid_cooldown = data.get("grid_cooldown", {})
                state.grid_manager.trailing_stops = data.get("trailing_stops", {})
                state.order_manager.open_orders = data.get("open_orders", [])
                logging.info(f"Đã khôi phục trạng thái cho {name}")
    except Exception as e:
        logging.error(f"Lỗi khi khôi phục trạng thái bot: {str(e)}")

# ======== CÁC LỚP NÂNG CAO ========
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
        if price <= 0 or high <= 0 or low <= 0 or volume < 0:
            logging.warning(f"Dữ liệu không hợp lệ: price={price}, high={high}, low={low}, volume={volume}. Bỏ qua lần cập nhật này.")
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

    def check_circuit_breaker(self, price: float) -> bool:
        if len(self.price_history) < 2:
            return False
        price_change = abs(price - self.price_history[-2]) / self.price_history[-2]
        if price_change > config.circuit_breaker_threshold:
            self.circuit_breaker_triggered = True
            self.circuit_breaker_start = time.time()
            logging.warning(f"Ngắt mạch được kích hoạt: Giá thay đổi {price_change:.2%}")
            send_telegram_alert(f"⚠ Ngắt mạch được kích hoạt: Giá thay đổi {price_change:.2%}")
            return True
        return False

    def check_circuit_breaker_status(self) -> bool:
        if self.circuit_breaker_triggered and self.circuit_breaker_start:
            elapsed = time.time() - self.circuit_breaker_start
            if elapsed > config.circuit_breaker_duration:
                self.circuit_breaker_triggered = False
                self.circuit_breaker_start = None
                logging.info("Ngắt mạch đã được đặt lại")
                send_telegram_alert("✅ Ngắt mạch đã được đặt lại")
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

class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels: List[Tuple[float, float]] = []
        self.last_rebalance: float = time.time()
        self.order_ids: List[str] = []
        self.price_history: deque = deque(maxlen=config.volatility_window)
        self.high_history: deque = deque(maxlen=config.volatility_window)
        self.low_history: deque = deque(maxlen=config.volatility_window)
        self.grid_cooldown: Dict[Tuple[str, float], float] = {}
        self.trailing_stops: Dict[Tuple[str, float], float] = {}
        self.price_precision: int = 2

    async def calculate_adaptive_grid(self, current_price: float, exchange: Optional['ExchangeInterface'] = None) -> None:
        if not isinstance(current_price, (int, float)) or current_price <= 0:
            logging.warning(f"Giá hiện tại không hợp lệ: {current_price}. Bỏ qua tính toán lưới.")
            return

        self.price_history.append(current_price)
        if not config.adaptive_grid_enabled or len(self.price_history) < config.volatility_window:
            self.grid_levels = self._create_static_grid(current_price)
            return

        # Đồng bộ các deque để đảm bảo độ dài bằng nhau
        if len(self.high_history) != len(self.low_history) or len(self.low_history) != len(self.price_history):
            logging.warning("Đồng bộ các lịch sử giá do độ dài không khớp")
            min_len = min(len(self.high_history), len(self.low_history), len(self.price_history))
            self.high_history = deque(list(self.high_history)[-min_len:], maxlen=config.volatility_window)
            self.low_history = deque(list(self.low_history)[-min_len:], maxlen=config.volatility_window)
            self.price_history = deque(list(self.price_history)[-min_len:], maxlen=config.volatility_window)

        atr = calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)
        if atr == 0:
            atr = current_price * 0.001

        volatility = atr / current_price if current_price != 0 else 0.0
        volatility = min(volatility, 0.1)
        num_levels = max(config.min_grid_levels, min(config.max_grid_levels, int(volatility / (config.base_grid_step_percent / 100))))
        step_percent = volatility / num_levels if num_levels > 0 else config.base_grid_step_percent / 100
        step_percent = min(step_percent, 0.02)

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
        grid = []
        step_percent = config.base_grid_step_percent / 100
        for i in range(config.min_grid_levels):
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
        target = round(price, self.price_precision)
        for order in open_orders:
            if 'price' not in order or order['price'] is None:
                continue
            order_price = round(float(order['price']), self.price_precision)
            if order['side'].upper() == side.upper() and order_price == target:
                return True
        return False

    async def place_grid_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
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
        allocated_funds_per_order = config.initial_investment / (2 * num_levels)
        quantity_per_level = allocated_funds_per_order / current_price
        quantity = max(filters['minQty'], quantity_per_level)
        quantity = min(quantity, filters['maxQty'])
        step_size = filters.get('stepSize', 0.00001)
        quantity = round(quantity - (quantity % step_size), 6)

        if quantity < filters['minQty']:
            logging.warning(f"Số lượng {quantity} nhỏ hơn mức tối thiểu {filters['minQty']}")
            send_telegram_alert(f"⚠️ Số lượng {quantity} nhỏ hơn mức tối thiểu {filters['minQty']}")
            return

        has_buy_order = False
        current_time = time.time()
        for buy_price, sell_price in self.grid_levels:
            key_buy = ("BUY", round(buy_price, self.price_precision))
            key_sell = ("SELL", round(sell_price, self.price_precision))
            cooldown_buy = self.grid_cooldown.get(key_buy, 0)
            cooldown_sell = self.grid_cooldown.get(key_sell, 0)

            try:
                if buy_price <= 0 or sell_price <= 0:
                    logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                    continue

                if buy_price < current_price and current_time >= cooldown_buy:
                    if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                        buy_order = await safe_api_call(
                            exchange.place_order,
                            side='BUY',
                            order_type='LIMIT',
                            quantity=quantity,
                            price=buy_price
                        )
                        if 'orderId' not in buy_order:
                            logging.error(f"Lệnh MUA không trả về orderId: {buy_order}")
                            continue
                        self.order_ids.append(buy_order['orderId'])
                        has_buy_order = True
                        logging.info(f"Đã đặt lệnh MUA tại {buy_price:.2f}, số lượng: {quantity:.6f}, orderId: {buy_order['orderId']}")
                        send_telegram_alert(f"📥 Đã đặt lệnh MUA tại {buy_price:.2f}, số lượng: {quantity:.6f}")
                    else:
                        logging.info(f"Bỏ qua lệnh BUY tại {buy_price:.2f} do đã tồn tại lệnh trùng.")
                else:
                    logging.info(f"Bỏ qua lệnh BUY tại {buy_price:.2f} do cooldown hoặc giá không phù hợp.")

                if sell_price > current_price and current_time >= cooldown_sell:
                    if not self.duplicate_order_exists("SELL", sell_price, open_orders):
                        sell_order = await safe_api_call(
                            exchange.place_order,
                            side='SELL',
                            order_type='LIMIT',
                            quantity=quantity,
                            price=sell_price
                        )
                        if 'orderId' not in sell_order:
                            logging.error(f"Lệnh BÁN không trả về orderId: {sell_order}")
                            continue
                        self.order_ids.append(sell_order['orderId'])
                        logging.info(f"Đã đặt lệnh BÁN tại {sell_price:.2f}, số lượng: {quantity:.6f}, orderId: {sell_order['orderId']}")
                        send_telegram_alert(f"📤 Đã đặt lệnh BÁN tại {sell_price:.2f}, số lượng: {quantity:.6f}")
                    else:
                        logging.info(f"Bỏ qua lệnh SELL tại {sell_price:.2f} do đã tồn tại lệnh trùng.")
                else:
                    logging.info(f"Bỏ qua lệnh SELL tại {sell_price:.2f} do cooldown hoặc giá không phù hợp.")
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh: {str(e)}")
                send_telegram_alert(f"⚠️ Lỗi khi đặt lệnh: {str(e)}")

        if not has_buy_order:
            try:
                buy_price = current_price * 0.99
                if buy_price > 0:
                    if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                        buy_order = await safe_api_call(
                            exchange.place_order,
                            side='BUY',
                            order_type='LIMIT',
                            quantity=quantity,
                            price=buy_price
                        )
                        if 'orderId' not in buy_order:
                            logging.error(f"Lệnh MUA động không trả về orderId: {buy_order}")
                            return
                        self.order_ids.append(buy_order['orderId'])
                        logging.info(f"Đã đặt lệnh MUA động tại {buy_price:.2f}, số lượng: {quantity:.6f}, orderId: {buy_order['orderId']}")
                        send_telegram_alert(f"📥 Đã đặt lệnh MUA động tại {buy_price:.2f}, số lượng: {quantity:.6f}")
                    else:
                        logging.info(f"Bỏ qua lệnh MUA động tại {buy_price:.2f} do đã tồn tại lệnh trùng.")
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh MUA động: {str(e)}")
                send_telegram_alert(f"⚠️ Lỗi khi đặt lệnh MUA động: {str(e)}")

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        for order_id in self.order_ids.copy():
            try:
                await safe_api_call(exchange.cancel_order, order_id)
                self.order_ids.remove(order_id)
            except Exception as e:
                logging.warning(f"Lỗi khi hủy lệnh {order_id}: {str(e)}")
        self.order_ids.clear()

    def set_cooldown(self, side: str, price: float):
        key = (side.upper(), round(price, self.price_precision))
        self.grid_cooldown[key] = time.time() + config.order_cooldown_sec
        logging.info(f"Đặt cooldown cho {key} đến {self.grid_cooldown[key]:.2f}")

    def get_grid_range(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.grid_levels:
            return None, None
        lows = [l for l, _ in self.grid_levels]
        highs = [h for _, h in self.grid_levels]
        return min(lows) if lows else None, max(highs) if highs else None

    def update_trailing_stop_for_level(self, grid_level: Tuple[float, float], current_price: float) -> float:
        _, sell_price = grid_level
        key = ("SELL", round(sell_price, self.price_precision))
        if key not in self.trailing_stops:
            self.trailing_stops[key] = sell_price
        if current_price > self.trailing_stops[key] and current_price > config.trailing_up_activation:
            self.trailing_stops[key] = current_price * (1 - config.base_stop_loss_percent / 100)
        return self.trailing_stops[key]

    async def rebalance_if_needed(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        await self.cancel_all_orders(exchange)
        self.order_ids.clear()

        low_grid, high_grid = self.get_grid_range()
        need = (
            time.time() - self.last_rebalance > config.grid_rebalance_interval or
            (low_grid is not None and current_price < low_grid) or
            (high_grid is not None and current_price > high_grid)
        )
        if need:
            await self.calculate_adaptive_grid(current_price, exchange)
            await self.place_grid_orders(current_price, exchange)
            self.last_rebalance = time.time()
            logging.info(f"🔄 Đã rebalance grid mới tại giá {current_price:.2f}")

class EnhancedOrderManager:
    def __init__(self):
        self.open_orders: List[Dict] = []
        self.is_paused: bool = False

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
                    logging.warning(f"Lệnh không có orderId: {order}")
                    continue
                if order['status'] == 'FILLED':
                    side = order['side']
                    price = float(order['price'])
                    qty = float(order['executedQty'])
                    fee = qty * price * (config.maker_fee if order['type'] == 'LIMIT' else config.taker_fee)
                    profit = (current_price - price) * qty if side.upper() == 'BUY' else (price - current_price) * qty
                    profit_tracker.record_trade(profit, fee)
                    msg = (f"✅ Lệnh {side} đã khớp\n"
                           f"▪ Giá: {price:.2f}\n"
                           f"▪ Số lượng: {qty:.6f}\n"
                           f"▪ Lợi nhuận: {profit:.2f} USDT\n"
                           f"▪ Phí: {fee:.4f} USDT")
                    send_telegram_alert(msg)
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
            send_telegram_alert(f"⚠️ Lỗi khi kiểm tra lệnh: {str(e)}")

class GridStrategy(ABC):
    @abstractmethod
    async def generate_signals(self, data: MarketData) -> List[Signal]:
        pass

class AdaptiveGridStrategy(GridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[Signal]:
        if data.price is None or data.high is None or data.low is None or data.price <= 0:
            logging.warning(f"Dữ liệu không đầy đủ: price={data.price}, high={data.high}, low={data.low}. Bỏ qua lần cập nhật này.")
            return []
        self.grid_manager.price_history.append(data.price)
        self.grid_manager.high_history.append(data.high)
        self.grid_manager.low_history.append(data.low)
        await self.grid_manager.calculate_adaptive_grid(data.price)
        signals = []
        # Thêm RSI để lọc tín hiệu trong thị trường có xu hướng
        prices = list(self.grid_manager.price_history)
        if len(prices) >= config.rsi_period:
            rsi = calculate_rsi(prices, config.rsi_period)
            if rsi > 70:  # Quá mua, ưu tiên bán
                logging.info(f"RSI {rsi:.2f} > 70, ưu tiên tín hiệu BÁN")
            elif rsi < 30:  # Quá bán, ưu tiên mua
                logging.info(f"RSI {rsi:.2f} < 30, ưu tiên tín hiệu MUA")
            else:
                logging.info(f"RSI {rsi:.2f}, không có xu hướng rõ ràng")

        for buy_price, sell_price in self.grid_manager.grid_levels:
            if data.price <= buy_price and (rsi < 70 or rsi < 30):  # Chỉ mua nếu không quá mua
                signals.append(Signal(side="BUY", price=buy_price, quantity=config.min_quantity))
            elif data.price >= sell_price and (rsi > 30 or rsi > 70):  # Chỉ bán nếu không quá bán
                signals.append(Signal(side="SELL", price=sell_price, quantity=config.min_quantity))
        
        # Thêm tín hiệu mặc định nếu không có tín hiệu nào được tạo
        if not signals and len(self.grid_manager.grid_levels) > 0:
            buy_price, sell_price = self.grid_manager.grid_levels[0]
            if data.price < buy_price and (rsi < 70 or rsi < 30):
                signals.append(Signal(side="BUY", price=buy_price, quantity=config.min_quantity))
            elif data.price > sell_price and (rsi > 30 or rsi > 70):
                signals.append(Signal(side="SELL", price=sell_price, quantity=config.min_quantity))
        
        return signals

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

    async def process_events(self) -> None:
        # Giảm tần suất gọi API bằng cách chỉ cập nhật giá mỗi 5 giây
        current_time = time.time()
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

        market_data = MarketData(
            price=self.current_price,
            volume=self.current_volume,
            high=self.current_high,
            low=self.current_low
        )
        self.protection.update(market_data.price, market_data.high, market_data.low, market_data.volume)
        await self.grid_manager.calculate_adaptive_grid(self.current_price, self.exchange)
        if await check_protections(self, self.exchange, market_data.price, market_data.volume, market_data.high, market_data.low):
            return
        signals = await self.strategy.generate_signals(market_data)
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
        await self.grid_manager.rebalance_if_needed(self.current_price, self.exchange)

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

    @abstractmethod
    async def close(self) -> None:
        pass

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

class PaperTradingExchange(ExchangeInterface):
    def __init__(self, symbol: str):
        super().__init__("paper_trading", "", "")
        self.symbol = symbol
        base_asset = symbol.replace("USDT", "")
        self.simulated_balance = {"USDT": config.initial_investment, base_asset: 0.0}
        self.paper_orders = []
        self.current_market_price = None

    async def get_price(self) -> float:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={self.symbol}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    data = await response.json()
                    price = float(data["price"])
                    self.current_market_price = price
                    return price
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá từ API Binance trong paper trading: {str(e)}")
            if self.current_market_price is None:
                self.current_market_price = 100.0
            return self.current_market_price

    async def get_volume(self) -> float:
        return 1000.0

    async def get_high_low(self) -> Tuple[float, float]:
        price = await self.get_price()
        return price * 1.01, price * 0.99

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        if order_type.upper() != "LIMIT" or price is None:
            raise ValueError("Chỉ hỗ trợ LIMIT order")
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
        base_asset = self.symbol.replace("USDT", "")
        if side.upper() == "BUY":
            cost = price * quantity
            if self.simulated_balance["USDT"] < cost:
                raise ValueError("Số dư USDT không đủ cho lệnh mua mô phỏng")
            self.simulated_balance["USDT"] -= cost
            self.simulated_balance[base_asset] += quantity
        elif side.upper() == "SELL":
            if self.simulated_balance[base_asset] < quantity:
                raise ValueError("Số dư không đủ để bán trong mô phỏng")
            self.simulated_balance[base_asset] -= quantity
            self.simulated_balance["USDT"] += price * quantity
        return order

    async def _match_orders(self):
        price = await self.get_price()
        base = self.symbol.replace("USDT", "")
        for o in self.paper_orders:
            if o["status"] == "OPEN":
                if (o["side"] == "BUY" and price <= o["price"]) or (o["side"] == "SELL" and price >= o["price"]):
                    o["status"] = "FILLED"
                    o["executedQty"] = o["quantity"]
                    cost = o["price"] * o["quantity"]
                    if o["side"] == "BUY":
                        self.simulated_balance["USDT"] -= cost
                        self.simulated_balance[base] += o["quantity"]
                    else:
                        self.simulated_balance[base] -= o["quantity"]
                        self.simulated_balance["USDT"] += cost

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

    async def cancel_all_orders(self) -> None:
        for o in self.paper_orders:
            if o["status"] == "OPEN":
                o["status"] = "CANCELED"

    async def get_balance(self) -> Tuple[float, float]:
        base_asset = self.symbol.replace("USDT", "")
        return self.simulated_balance["USDT"], self.simulated_balance[base_asset]

    async def get_order_book(self) -> Dict:
        price = await self.get_price()
        return {"bids": [[price * 0.99, 1]], "asks": [[price * 1.01, 1]]}

    async def get_symbol_filters(self) -> Dict:
        return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            binance = ccxt_async.binance({'enableRateLimit': True})
            ohlcv = await binance.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"Không có dữ liệu lịch sử từ Binance cho {self.symbol}")
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
        return

    async def close(self) -> None:
        logging.info("Đóng kết nối paper trading (không có client thật)")

class BinanceExchange(BaseExchange):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("binance", api_key, api_secret)
        self.client = ccxt_async.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'testnet': False
        })
        self.ws_client = None

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        try:
            order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
            if order_type == 'limit' and price is None:
                raise ValueError("Lệnh giới hạn (limit order) yêu cầu tham số price không được là None")
            if price is not None:
                if not isinstance(price, (int, float)) or price <= 0:
                    raise ValueError(f"Giá không hợp lệ: {price}. Giá phải là số dương.")
                price = round(float(price), self.price_precision)
                if price <= 0:
                    raise ValueError(f"Giá sau khi làm tròn không hợp lệ: {price}. Giá phải lớn hơn 0.")
            order = await self.client.create_order(
                symbol=self.symbol,
                side=side.lower(),
                type=order_type,
                amount=round(quantity, self.quantity_precision),
                price=price
            )
            if 'orderId' not in order:
                order['orderId'] = order.get('id', str(int(time.time() * 1000)))
            return order
        except Exception as e:
            logging.error(f"Lỗi khi đặt lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def cancel_order(self, order_id: str) -> None:
        try:
            await self.client.cancel_order(order_id, self.symbol)
        except Exception as e:
            logging.error(f"Lỗi khi hủy lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def get_open_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_open_orders(self.symbol)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 
                     'origQty': o['amount'], 'executedQty': o['filled'], 
                     'type': o['type'].upper(), 'status': 'OPEN'} for o in orders]
        except Exception as e:
            logging.error(f"Lỗi khi lấy danh sách lệnh mở trên {self.exchange_name}: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 
                     'origQty': o['amount'], 'executedQty': o['filled'], 
                     'type': o['type'].upper(), 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 
                     'updateTime': o['timestamp']} for o in orders]
        except Exception as e:
            logging.error(f"Lỗi khi lấy tất cả lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def get_balance(self) -> Tuple[float, float]:
        try:
            balance = await self.client.fetch_balance()
            usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
            base_asset = self.symbol.split('USDT')[0]
            btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
            return usdt_balance, btc_balance
        except Exception as e:
            logging.error(f"Lỗi khi lấy số dư trên {self.exchange_name}: {str(e)}")
            raise

    async def get_order_book(self) -> Dict:
        try:
            return await self.client.fetch_order_book(self.symbol, limit=50)
        except Exception as e:
            logging.error(f"Lỗi khi lấy sổ lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def get_symbol_filters(self) -> Dict:
        for attempt in range(config.api_retry_count):
            try:
                markets = await self.client.load_markets()
                if self.symbol not in markets:
                    logging.error(f"Không tìm thấy cặp giao dịch {self.symbol} trên {self.exchange_name}")
                    raise ValueError(f"Cặp giao dịch {self.symbol} không tồn tại trên {self.exchange_name}")
                market = markets[self.symbol]
                return {
                    'minQty': float(market['limits']['amount']['min']),
                    'maxQty': float(market['limits']['amount']['max']),
                    'stepSize': float(market['precision']['amount'])
                }
            except Exception as e:
                logging.error(f"Lỗi khi lấy thông tin LOT_SIZE từ {self.exchange_name} (lần thử {attempt+1}/{config.api_retry_count}): {str(e)}")
                if attempt == config.api_retry_count - 1:
                    logging.error(f"Không thể lấy thông tin LOT_SIZE sau {config.api_retry_count} lần thử")
                    return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}
                await asyncio.sleep(config.api_retry_delay)
        return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            await self.client.load_markets()
            ohlcv = await self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"Không có dữ liệu lịch sử cho {self.symbol} trên {self.exchange_name}")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            logging.info(f"Đã lấy {len(df)} dòng dữ liệu lịch sử cho {self.symbol}")
            return df
        except Exception as e:
            logging.error(f"Lỗi khi lấy dữ liệu lịch sử trên {self.exchange_name}: {str(e)}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

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

class MEXCExchange(BaseExchange):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("mexc", api_key, api_secret)
        self.client = ccxt_async.mexc({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True
        })
        self.symbol = self.symbol.replace("USDT", "/USDT")

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        try:
            order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
            if order_type == 'limit' and price is None:
                raise ValueError("Lệnh giới hạn (limit order) yêu cầu tham số price không được là None")
            if price is not None:
                if not isinstance(price, (int, float)) or price <= 0:
                    raise ValueError(f"Giá không hợp lệ: {price}. Giá phải là số dương.")
                price = round(float(price), self.price_precision)
                if price <= 0:
                    raise ValueError(f"Giá sau khi làm tròn không hợp lệ: {price}. Giá phải lớn hơn 0.")
            order = await self.client.create_order(
                symbol=self.symbol,
                side=side.lower(),
                type=order_type,
                amount=round(quantity, self.quantity_precision),
                price=price
            )
            if 'orderId' not in order:
                order['orderId'] = order.get('id', str(int(time.time() * 1000)))
            return order
        except Exception as e:
            logging.error(f"Lỗi khi đặt lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def cancel_order(self, order_id: str) -> None:
        try:
            await self.client.cancel_order(order_id, self.symbol)
        except Exception as e:
            logging.error(f"Lỗi khi hủy lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def get_open_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_open_orders(self.symbol)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'],
                     'origQty': o['amount'], 'executedQty': o['filled'],
                     'type': o['type'].upper(), 'status': 'OPEN'} for o in orders]
        except Exception as e:
            logging.error(f"Lỗi khi lấy danh sách lệnh mở trên {self.exchange_name}: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'],
                     'origQty': o['amount'], 'executedQty': o['filled'],
                     'type': o['type'].upper(), 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN',
                     'updateTime': o['timestamp']} for o in orders]
        except Exception as e:
            logging.error(f"Lỗi khi lấy tất cả lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def get_balance(self) -> Tuple[float, float]:
        try:
            balance = await self.client.fetch_balance()
            usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
            base_asset = self.symbol.split('/')[0]
            btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
            return usdt_balance, btc_balance
        except Exception as e:
            logging.error(f"Lỗi khi lấy số dư trên {self.exchange_name}: {str(e)}")
            raise

    async def get_order_book(self) -> Dict:
        try:
            return await self.client.fetch_order_book(self.symbol, limit=50)
        except Exception as e:
            logging.error(f"Lỗi khi lấy sổ lệnh trên {self.exchange_name}: {str(e)}")
            raise

    async def get_symbol_filters(self) -> Dict:
        for attempt in range(config.api_retry_count):
            try:
                markets = await self.client.load_markets()
                if self.symbol not in markets:
                    logging.error(f"Không tìm thấy cặp giao dịch {self.symbol} trên {self.exchange_name}")
                    raise ValueError(f"Cặp giao dịch {self.symbol} không tồn tại trên {self.exchange_name}")
                market = markets[self.symbol]
                return {
                    'minQty': float(market['limits']['amount']['min']),
                    'maxQty': float(market['limits']['amount']['max']),
                    'stepSize': float(market['precision']['amount'])
                }
            except Exception as e:
                logging.error(f"Lỗi khi lấy thông tin LOT_SIZE từ {self.exchange_name} (lần thử {attempt+1}/{config.api_retry_count}): {str(e)}")
                if attempt == config.api_retry_count - 1:
                    logging.error(f"Không thể lấy thông tin LOT_SIZE sau {config.api_retry_count} lần thử")
                    return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}
                await asyncio.sleep(config.api_retry_delay)
        return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        try:
            await self.client.load_markets()
            ohlcv = await self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) == 0:
                logging.warning(f"Không có dữ liệu lịch sử cho {self.symbol} trên {self.exchange_name}")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)
            logging.info(f"Đã lấy {len(df)} dòng dữ liệu lịch sử cho {self.symbol}")
            return df
        except Exception as e:
            logging.error(f"Lỗi khi lấy dữ liệu lịch sử trên {self.exchange_name}: {str(e)}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

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

class ExchangeManager:
    def __init__(self):
        self.exchanges: Dict[str, ExchangeInterface] = {}
        if config.paper_trading_enabled:
            self.exchanges["paper_trading"] = PaperTradingExchange(config.symbol)
            logging.info("Đã khởi tạo sàn giao dịch paper_trading (chế độ paper trading)")
        else:
            for exchange_name in config.enabled_exchanges:
                api_key, api_secret = config.exchange_credentials[exchange_name]
                if exchange_name == "binance":
                    self.exchanges[exchange_name] = BinanceExchange(api_key, api_secret)
                elif exchange_name == "mexc":
                    self.exchanges[exchange_name] = MEXCExchange(api_key, api_secret)
                else:
                    logging.error(f"Sàn giao dịch {exchange_name} không được hỗ trợ!")
                    raise ValueError(f"Sàn giao dịch không được hỗ trợ: {exchange_name}")
                logging.info(f"Đã khởi tạo sàn giao dịch {exchange_name}")

    async def check_price_disparity(self) -> None:
        prices = {}
        for name, ex in self.exchanges.items():
            prices[name] = await safe_api_call(ex.get_price)
        if not prices:
            return
        max_diff = max(prices.values()) - min(prices.values())
        if max_diff / min(prices.values()) > 0.01:
            logging.warning(f"Chênh lệch giá lớn: {max_diff:.2f} giữa {prices}")
            send_telegram_alert(f"⚠️ Chênh lệch giá lớn: {max_diff:.2f} giữa {prices}")

    async def close_all(self) -> None:
        for name, ex in self.exchanges.items():
            await ex.close()

# ======== XỬ LÝ LỖI ========
async def safe_api_call(func, *args, timeout: int = 10, max_retries: int = 3, **kwargs) -> Any:
    for attempt in range(max_retries):
        try:
            cache_key = f"{func.__name__}_{str(args)}_{str(kwargs)}"
            if cache_key in api_cache:
                return api_cache[cache_key]
            result = await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            api_cache[cache_key] = result
            return result
        except asyncio.TimeoutError:
            logging.warning(f"API call hết thời gian sau {timeout} giây (lần thử {attempt+1}/{max_retries})")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)
        except (ccxt_async.DDoSProtection, ccxt_async.RateLimitExceeded) as e:
            logging.warning(f"API bị giới hạn tốc độ: {str(e)}. Chờ {config.api_retry_delay} giây")
            await asyncio.sleep(config.api_retry_delay * (2 ** attempt))
        except ccxt_async.AuthenticationError as e:
            logging.error(f"Lỗi xác thực API: {str(e)}")
            send_telegram_alert(f"❌ Lỗi xác thực API trên {func.__qualname__}: {str(e)}")
            raise
        except ccxt_async.ExchangeNotAvailable as e:
            logging.error(f"Sàn giao dịch không khả dụng: {str(e)}. Tạm dừng 5 phút")
            send_telegram_alert(f"⚠️ Sàn giao dịch không khả dụng: {str(e)}")
            await asyncio.sleep(300)
        except (ccxt_async.NetworkError, ccxt_async.ExchangeError) as e:
            if attempt == max_retries - 1:
                logging.error(f"Gọi API thất bại sau {max_retries} lần thử: {str(e)}")
                raise
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Lỗi không xác định: {str(e)}")
            raise

# ======== TÍNH TOÁN CHỈ SỐ KỸ THUẬT ========
class RSICalculator:
    def __init__(self, period: int):
        self.period: int = period
        self.gains: deque = deque(maxlen=period)
        self.losses: deque = deque(maxlen=period)
        self.avg_gain: float = 0.0
        self.avg_loss: float = 0.0
        self.initialized: bool = False

    def update(self, price: float, prev_price: float) -> float:
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
    if len(prices) < 2:
        logging.warning("Không đủ dữ liệu để tính RSI")
        return 50.0
    rsi_calculator = RSICalculator(period)
    rsi = 50.0
    for i in range(1, len(prices)):
        rsi = rsi_calculator.update(prices[i], prices[i-1])
    return rsi

def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
    if len(highs) != len(lows) or len(lows) != len(closes):
        logging.warning(f"Độ dài danh sách không khớp: highs={len(highs)}, lows={len(lows)}, closes={len(closes)}. Trả về ATR = 0.0")
        return 0.0
    if len(closes) < 2:
        logging.warning("Không đủ dữ liệu để tính ATR")
        return 0.0
    tr_list = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
    if not tr_list or len(tr_list) < period:
        return 0.0
    atr = sum(tr_list[-period:]) / period
    return atr

def calculate_bollinger_bands(prices: List[float], period: int, std_dev: float) -> Tuple[float, float, float]:
    if len(prices) < period:
        return prices[-1] if prices else 0.0, prices[-1] if prices else 0.0, prices[-1] if prices else 0.0
    sma = float(np.mean(prices[-period:]))
    std = float(np.std(prices[-period:]))
    return sma, sma + std_dev * std, sma - std_dev * std

def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    if len(returns) < 2:
        return 0.0
    mean_return = float(np.mean(returns))
    std_return = float(np.std(returns))
    if std_return < 1e-6:
        return 0.0
    return (mean_return - risk_free_rate) / std_return * np.sqrt(252)

def calculate_max_drawdown(equity_curve: List[float]) -> float:
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

# ======== KIỂM TRA MÔ PHỎNG ========
async def run_simulated_backtest(exchange: ExchangeInterface) -> Dict[str, float]:
    logging.info(f"Chạy kiểm tra mô phỏng trên {exchange.exchange_name}")
    historical_data = await safe_api_call(exchange.fetch_historical_data, timeframe='1h', limit=500)
    if historical_data.empty:
        logging.warning(f"Không có dữ liệu lịch sử cho kiểm tra mô phỏng trên {exchange.exchange_name}")
        return {"Total Profit": 0.0, "Trade Count": 0, "Sharpe Ratio": 0.0, "Max Drawdown": 0.0, "Total Fees": 0.0}
    historical_data = historical_data.dropna(subset=['close', 'high', 'low', 'volume'])
    if historical_data.empty:
        logging.warning(f"Dữ liệu lịch sử sau khi lọc bị rỗng trên {exchange.exchange_name}")
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
        logging.info(f"Tín hiệu được tạo tại giá {price}: {signals}")
        for signal in signals:
            try:
                order = await exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
                if 'orderId' not in order:
                    logging.warning(f"Lệnh không có orderId trong mô phỏng: {order}")
                    continue
                grid_manager.order_ids.append(order['orderId'])
                # Giả lập khớp lệnh ngay lập tức trong mô phỏng, thêm trượt giá (slippage)
                slippage = price * 0.001  # Giả lập trượt giá 0.1%
                executed_price = price + slippage if signal.side == "BUY" else price - slippage
                order['status'] = 'FILLED'
                order['executedQty'] = order['quantity']
                profit = (executed_price * (1 + config.base_take_profit_percent / 100) - executed_price) * signal.quantity if signal.side == "BUY" else (executed_price - executed_price * (1 - config.base_stop_loss_percent / 100)) * signal.quantity
                fee = signal.quantity * executed_price * config.maker_fee
                profit_tracker.record_trade(profit, fee)
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh trong mô phỏng: {str(e)}")
    stats = profit_tracker.get_stats()
    logging.info(f"Kết quả kiểm tra mô phỏng trên {exchange.exchange_name}: {stats}")
send_telegram_alert(
    f"📉 Kết quả kiểm tra mô phỏng trên {exchange.exchange_name}\n"
    f"Total Profit: {stats['Total Profit']:.2f} USDT\n"
    f"Trade Count: {stats['Trade Count']}\n"
    f"Win Rate: {stats['Win Rate']:.2f}%\n"
    f"Max Drawdown: {stats['Max Drawdown']:.2f}%\n"
    f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}\n"
    f"Total Fees: {stats['Total Fees']:.2f} USDT"
)
    return stats

# ======== KIỂM TRA BẢO VỆ ========
async def check_protections(state: BotState, exchange: ExchangeInterface, price: float, volume: float, high: float, low: float) -> bool:
    if state.order_manager.is_paused:
        logging.info(f"Bot {state.exchange_name} đang tạm dừng")
        return True

    # Kiểm tra ngắt mạch
    if state.protection.check_circuit_breaker(price) or state.protection.check_circuit_breaker_status():
        logging.warning(f"Ngắt mạch đang hoạt động trên {state.exchange_name}")
        await state.order_manager.cancel_all_orders(exchange)
        return True

    # Kiểm tra bảo vệ bơm giá
    if state.protection.check_pump_protection(price):
        logging.warning(f"Phát hiện bơm giá trên {state.exchange_name}, tạm dừng giao dịch")
        await state.order_manager.cancel_all_orders(exchange)
        state.order_manager.is_paused = True
        await asyncio.sleep(300)  # Tạm dừng 5 phút
        state.order_manager.is_paused = False
        return True

    # Kiểm tra hoạt động bất thường
    if state.protection.check_abnormal_activity(volume):
        logging.warning(f"Phát hiện hoạt động bất thường trên {state.exchange_name}, tạm dừng giao dịch")
        await state.order_manager.cancel_all_orders(exchange)
        state.order_manager.is_paused = True
        await asyncio.sleep(300)  # Tạm dừng 5 phút
        state.order_manager.is_paused = False
        return True

    # Kiểm tra trailing stop
    should_stop, trailing_price = state.protection.update_trailing_stop(price)
    if should_stop:
        logging.warning(f"Trailing stop được kích hoạt trên {state.exchange_name} tại {trailing_price:.2f}")
        await state.order_manager.cancel_all_orders(exchange)
        state.order_manager.is_paused = True
        send_telegram_alert(f"🛑 Trailing stop được kích hoạt trên {state.exchange_name} tại {trailing_price:.2f}")
        await asyncio.sleep(300)  # Tạm dừng 5 phút
        state.order_manager.is_paused = False
        return True

    # Kiểm tra trailing buy
    should_buy, trailing_buy_price = state.protection.update_trailing_buy(price)
    if should_buy:
        logging.info(f"Trailing buy được kích hoạt trên {state.exchange_name} tại {trailing_buy_price:.2f}")
        try:
            order = await safe_api_call(
                exchange.place_order,
                side='BUY',
                order_type='LIMIT',
                quantity=config.min_quantity,
                price=trailing_buy_price
            )
            state.grid_manager.order_ids.append(order['orderId'])
            send_telegram_alert(f"📥 Trailing buy được kích hoạt trên {state.exchange_name} tại {trailing_buy_price:.2f}")
        except Exception as e:
            logging.error(f"Lỗi khi đặt lệnh trailing buy: {str(e)}")
            send_telegram_alert(f"⚠️ Lỗi khi đặt lệnh trailing buy trên {state.exchange_name}: {str(e)}")

    return False

# ======== QUẢN LÝ BOT CHÍNH ========
class GridTradingBot:
    def __init__(self):
        self.exchange_manager = ExchangeManager()
        self.states: Dict[str, BotState] = {}
        self.running = True
        self.last_status_update = 0
        self.executor = ThreadPoolExecutor(max_workers=4)
        for name, ex in self.exchange_manager.exchanges.items():
            grid_manager = EnhancedSmartGridManager()
            strategy = AdaptiveGridStrategy(grid_manager)
            self.states[name] = BotState(ex, strategy)
        load_state(self.states)  # Khôi phục trạng thái bot

    async def run(self):
        try:
            for name, state in self.states.items():
                if config.websocket_enabled:
                    await state.exchange.start_websocket(state)
            while self.running:
                tasks = []
                for name, state in self.states.items():
                    tasks.append(asyncio.create_task(state.process_events()))
                await asyncio.gather(*tasks, return_exceptions=True)
                await self.exchange_manager.check_price_disparity()
                await self.update_status()
                await asyncio.sleep(5)  # Giảm tần suất vòng lặp chính để tiết kiệm tài nguyên
                save_state(self.states)  # Lưu trạng thái bot định kỳ
        except asyncio.CancelledError:
            logging.info("Bot đã bị dừng bởi tín hiệu hệ thống")
        except Exception as e:
            logging.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            send_telegram_alert(f"❌ Lỗi trong vòng lặp chính: {str(e)}")
        finally:
            await self.shutdown()

    async def update_status(self):
        current_time = time.time()
        if current_time - self.last_status_update < config.status_update_interval:
            return
        self.last_status_update = current_time
        for name, state in self.states.items():
            stats = state.tracker.get_stats()
            usdt_balance, btc_balance = await safe_api_call(state.exchange.get_balance)
            health = await state.exchange.health_check()
            msg = (f"📊 Trạng thái bot trên {name}\n"
                   f"▪ Giá hiện tại: {state.current_price:.2f} USDT\n"
                   f"▪ Tổng lợi nhuận: {stats['Total Profit']:.2f} USDT\n"
                   f"▪ Số giao dịch: {stats['Trade Count']}\n"
                   f"▪ Tỷ lệ thắng: {stats['Win Rate']:.2f}%\n"
                   f"▪ Drawdown tối đa: {stats['Max Drawdown']:.2f}%\n"
                   f"▪ Sharpe Ratio: {stats['Sharpe Ratio']:.2f}\n"
                   f"▪ Tổng phí: {stats['Total Fees']:.2f} USDT\n"
                   f"▪ Số dư USDT: {usdt_balance:.2f}\n"
                   f"▪ Số dư BTC: {btc_balance:.6f}\n"
                   f"▪ Trạng thái sàn: {health['status']}\n"
                   f"▪ Độ trễ API: {health['api_latency_ms']:.2f}ms")
            logging.info(msg)
            send_telegram_alert(msg)

    async def shutdown(self):
        self.running = False
        for name, state in self.states.items():
            await state.order_manager.cancel_all_orders(state.exchange)
            await state.exchange.close()
        self.executor.shutdown(wait=True)
        save_state(self.states)  # Lưu trạng thái trước khi tắt
        logging.info("Bot đã dừng hoàn toàn")

# ======== DASHBOARD GIAO DIỆN NGƯỜI DÙNG ========
def create_dashboard(bot: GridTradingBot):
    app = dash.Dash(__name__, title="Grid Trading Dashboard")
    
    # Thêm xác thực cơ bản cho dashboard
    VALID_USERNAME_PASSWORD_PAIRS = {
        config.dashboard_username: config.dashboard_password
    }
    auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)

    app.layout = html.Div([
        html.H1("Grid Trading Bot Dashboard", style={'textAlign': 'center'}),
        dcc.Tabs(id="tabs", value='tab-1', children=[
            dcc.Tab(label='Overview', value='tab-1'),
            dcc.Tab(label='Performance', value='tab-2'),
            dcc.Tab(label='Orders', value='tab-3'),
            dcc.Tab(label='System Health', value='tab-4'),
        ]),
        html.Div(id='tabs-content'),
        dcc.Interval(id='interval-component', interval=10*1000, n_intervals=0)
    ])

    @app.callback(
        Output('tabs-content', 'children'),
        Input('tabs', 'value'),
        Input('interval-component', 'n_intervals')
    )
    def render_content(tab, n_intervals):
        content = []
        for name, state in bot.states.items():
            stats = state.tracker.get_stats()
            if tab == 'tab-1':
                content.append(html.Div([
                    html.H3(f"Exchange: {name}"),
                    html.P(f"Current Price: {state.current_price:.2f} USDT"),
                    html.P(f"Total Profit: {stats['Total Profit']:.2f} USDT"),
                    html.P(f"Trade Count: {stats['Trade Count']}"),
                    html.P(f"Win Rate: {stats['Win Rate']:.2f}%"),
                    html.P(f"Max Drawdown: {stats['Max Drawdown']:.2f}%"),
                    html.P(f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}"),
                    html.P(f"Total Fees: {stats['Total Fees']:.2f} USDT"),
                    html.Hr()
                ]))
            elif tab == 'tab-2':
                equity_curve = state.tracker.equity_curve
                content.append(html.Div([
                    html.H3(f"Performance: {name}"),
                    dcc.Graph(
                        figure={
                            'data': [
                                go.Scatter(
                                    x=list(range(len(equity_curve))),
                                    y=equity_curve,
                                    mode='lines',
                                    name='Equity Curve'
                                )
                            ],
                            'layout': go.Layout(
                                title='Equity Curve',
                                xaxis={'title': 'Trade Number'},
                                yaxis={'title': 'Total Profit (USDT)'}
                            )
                        }
                    ),
                    html.Hr()
                ]))
            elif tab == 'tab-3':
                open_orders = state.order_manager.open_orders
                orders_data = []
                for order in open_orders:
                    if 'orderId' not in order:
                        continue
                    orders_data.append([
                        order['orderId'],
                        order['side'],
                        order.get('price', 'N/A'),
                        order.get('origQty', 'N/A'),
                        order.get('executedQty', 'N/A'),
                        order.get('type', 'N/A'),
                        order.get('status', 'N/A')
                    ])
                content.append(html.Div([
                    html.H3(f"Open Orders: {name}"),
                    dash_table.DataTable(
                        columns=[
                            {"name": "Order ID", "id": "orderId"},
                            {"name": "Side", "id": "side"},
                            {"name": "Price", "id": "price"},
                            {"name": "Quantity", "id": "quantity"},
                            {"name": "Executed", "id": "executed"},
                            {"name": "Type", "id": "type"},
                            {"name": "Status", "id": "status"}
                        ],
                        data=[{
                            "orderId": o[0],
                            "side": o[1],
                            "price": o[2],
                            "quantity": o[3],
                            "executed": o[4],
                            "type": o[5],
                            "status": o[6]
                        } for o in orders_data],
                        style_table={'overflowX': 'auto'},
                        style_cell={'textAlign': 'left'}
                    ),
                    html.Hr()
                ]))
            elif tab == 'tab-4':
                health = asyncio.run(state.exchange.health_check())
                cpu_usage = psutil.cpu_percent(interval=1)
                memory_usage = psutil.virtual_memory().percent
                content.append(html.Div([
                    html.H3(f"System Health: {name}"),
                    html.P(f"Exchange Status: {health['status']}"),
                    html.P(f"API Latency: {health['api_latency_ms']:.2f}ms" if health['api_latency_ms'] else "N/A"),
                    html.P(f"CPU Usage: {cpu_usage:.2f}%"),
                    html.P(f"Memory Usage: {memory_usage:.2f}%"),
                    html.Hr()
                ]))
        return content

    return app

# ======== XỬ LÝ TÍN HIỆU HỆ THỐNG ========
def signal_handler(sig, frame):
    logging.info("Nhận tín hiệu dừng, đang tắt bot...")
    asyncio.create_task(bot.shutdown())
    sys.exit(0)

# ======== HÀM CHÍNH ========
async def main():
    global bot
    bot = GridTradingBot()
    
    # Chạy kiểm tra mô phỏng trước khi khởi động bot
    for name, state in bot.states.items():
        await run_simulated_backtest(state.exchange)

    # Khởi động bot
    bot_task = asyncio.create_task(bot.run())

    # Khởi động dashboard
    app = create_dashboard(bot)
    dashboard_thread = Thread(target=lambda: app.run_server(debug=False, host='0.0.0.0', port=8050))
    dashboard_thread.start()

    # Đợi bot hoàn thành
    await bot_task
    dashboard_thread.join()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    asyncio.run(main())