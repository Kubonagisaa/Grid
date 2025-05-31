import asyncio
import platform
import time
import math
import json
from datetime import datetime, timedelta
import os
from typing import List, Tuple, Dict, Optional, Any
from collections import deque
from abc import ABC, abstractmethod
from dataclasses import dataclass
import logging
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import ccxt.async_support as ccxt_async
import ccxt
import aiohttp
import signal
import sys
import threading
import smtplib
from email.mime.text import MIMEText
import requests
import websockets
import psutil
import xgboost as xgb
from textblob import TextBlob
import matplotlib.pyplot as plt
import nest_asyncio
import traceback

nest_asyncio.apply()

# ======== Initial Setup ======== 
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

last_health_alert_time = 0
HEALTH_ALERT_COOLDOWN = 600

# ======== Discord Notifier ======== 
class DiscordNotifier:
    def __init__(self):
        self.webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        self.enabled = bool(self.webhook_url)
        self.last_report_time = 0
        self.REPORT_INTERVAL = 1200  # 20 phút

    async def _send_embed(self, embed_data: dict) -> None:
        if not self.enabled:
            return
            
        payload = {
            "embeds": [embed_data],
            "username": "Grid Trading Bot",
            "avatar_url": "https://cdn-icons-png.flaticon.com/512/2472/2472019.png"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload) as response:
                    if response.status != 204:
                        logging.error(f"Discord API error: {response.status}")
        except Exception as e:
            logging.error(f"Lỗi gửi Discord: {str(e)}")

    async def send_report(self, state: 'BotState') -> None:
        if not self.enabled or time.time() - self.last_report_time < self.REPORT_INTERVAL:
            return

        current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        usdt_balance, coin_balance = await safe_api_call(state.exchange.get_balance, alert_on_failure=True)
        coin_balance = coin_balance if coin_balance is not None else 0.0
        total_value = usdt_balance + (coin_balance * state.current_price if state.current_price > 0 else 0)
        
        embed = {
            "title": f"📊 Báo cáo giao dịch {state.symbol}",
            "color": 0x00FF00 if state.tracker.total_profit >= 0 else 0xFF0000,
            "fields": [
                {"name": "🏦 Sàn giao dịch", "value": state.exchange_name.upper(), "inline": True},
                {"name": "🪙 Cặp tiền", "value": state.symbol.replace("USDT", "/USDT"), "inline": True},
                {"name": "💰 Vốn hiện tại", "value": f"{total_value:.2f} USDT", "inline": True},
                {"name": "🕒 Thời gian", "value": current_time, "inline": False},
                {"name": "📈 Giá hiện tại", "value": f"{state.current_price:.8f}", "inline": True},
                {"name": "💵 Lời/Lỗ", "value": f"{state.tracker.total_profit:.2f} USDT", "inline": True},
                {"name": "📊 Lệnh đang mở", "value": str(len(state.order_manager.open_orders)), "inline": True},
                {"name": "✅ Lệnh đã khớp", "value": str(len(state.order_manager.completed_orders)), "inline": True},
            ],
            "footer": {
                "text": "Grid Trading Bot",
                "icon_url": "https://cdn-icons-png.flaticon.com/512/2472/2472019.png"
            },
            "thumbnail": {
                "url": "https://cdn-icons-png.flaticon.com/512/2472/2472019.png"
            }
        }

        await self._send_embed(embed)
        self.last_report_time = time.time()

    async def send_alert(self, message: str, is_error: bool = False) -> None:
        if not self.enabled:
            return
            
        embed = {
            "title": "⚠️ Cảnh báo" if is_error else "ℹ️ Thông báo",
            "description": message,
            "color": 0xFF0000 if is_error else 0x00FF00,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self._send_embed(embed)

# ======== Configuration Management ======== 
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges = os.getenv("ENABLED_EXCHANGES", "paper_trading").split(",")
        self.exchange_credentials = {
            ex: (os.getenv(f"{ex.upper()}_API_KEY", "your_api_key"),
                 os.getenv(f"{ex.upper()}_API_SECRET", "your_api_secret"))
            for ex in self.enabled_exchanges
        }
        self.symbol = os.getenv("TRADING_SYMBOL", "DOGEUSDT").upper()
        self.initial_investment = float(os.getenv("INITIAL_INVESTMENT", 100))
        self.min_quantity = float(os.getenv("MIN_QUANTITY", 1))
        self.max_position = float(os.getenv("MAX_POSITION", 100))
        self.base_stop_loss_percent = float(os.getenv("BASE_STOP_LOSS_PERCENT", 2.0))
        self.base_take_profit_percent = float(os.getenv("BASE_TAKE_PROFIT_PERCENT", 2.0))
        self.price_drop_threshold = float(os.getenv("PRICE_DROP_THRESHOLD", 1.0))
        self.adaptive_grid_enabled = os.getenv("ADAPTIVE_GRID_ENABLED", "True").lower() == "true"
        self.min_grid_levels = int(os.getenv("MIN_GRID_LEVELS", 5))
        self.max_grid_levels = int(os.getenv("MAX_GRID_LEVELS", 7))
        self.base_grid_step_percent = float(os.getenv("BASE_GRID_STEP_PERCENT", 1.0))
        self.grid_rebalance_interval = int(os.getenv("GRID_REBALANCE_INTERVAL", 300))
        self.trailing_stop_enabled = os.getenv("TRAILING_STOP_ENABLED", "True").lower() == "true"
        self.trailing_up_activation = float(os.getenv("TRAILING_UP_ACTIVATION", 0.7))
        self.trailing_down_activation = float(os.getenv("TRAILING_DOWN_ACTIVATION", 0.5))
        self.trailing_tp_enabled = os.getenv("TRAILING_TP_ENABLED", "True").lower() == "true"
        self.trailing_buy_stop_enabled = os.getenv("TRAILING_BUY_STOP_ENABLED", "True").lower() == "true"
        self.trailing_buy_activation_percent = float(os.getenv("TRAILING_BUY_ACTIVATION_PERCENT", 2.0))
        self.trailing_buy_distance_percent = float(os.getenv("TRAILING_BUY_DISTANCE_PERCENT", 1.0))
        self.pump_protection_threshold = float(os.getenv("PUMP_PROTECTION_THRESHOLD", 0.03))
        self.circuit_breaker_threshold = float(os.getenv("CIRCUIT_BREAKER_THRESHOLD", 0.07))
        self.circuit_breaker_duration = int(os.getenv("CIRCUIT_BREAKER_DURATION", 180))
        self.abnormal_activity_threshold = float(os.getenv("ABNORMAL_ACTIVITY_THRESHOLD", 3.0))
        self.maker_fee = float(os.getenv("MAKER_FEE", 0.0002))
        self.taker_fee = float(os.getenv("TAKER_FEE", 0.0004))
        self.api_retry_count = int(os.getenv("API_RETRY_COUNT", 3))
        self.api_retry_delay = int(os.getenv("API_RETRY_DELAY", 5))
        self.max_open_orders = int(os.getenv("MAX_OPEN_ORDERS", 5))
        self.status_update_interval = int(os.getenv("STATUS_UPDATE_INTERVAL", 1800))
        self.volatility_window = int(os.getenv("VOLATILITY_WINDOW", 200))
        self.ma_short_period = int(os.getenv("MA_SHORT_PERIOD", 20))
        self.ma_long_period = int(os.getenv("MA_LONG_PERIOD", 50))
        self.atr_period = int(os.getenv("ATR_PERIOD", 14))
        self.rsi_period = int(os.getenv("RSI_PERIOD", 14))
        self.bb_period = int(os.getenv("BB_PERIOD", 20))
        self.bb_std_dev = float(os.getenv("BB_STD_DEV", 1.8))
        self.min_acceptable_volume = float(os.getenv("MIN_ACCEPTABLE_VOLUME", 100))
        self.fee_threshold = float(os.getenv("FEE_THRESHOLD", 0.1))
        self.websocket_enabled = os.getenv("WEBSOCKET_ENABLED", "False").lower() == "true"
        self.websocket_reconnect_interval = int(os.getenv("WEBSOCKET_RECONNECT_INTERVAL", 60))
        self.order_cooldown_sec = int(os.getenv("ORDER_COOLDOWN_SECONDS", 60))
        self.paper_trading_enabled = os.getenv("PAPER_TRADING", "True").lower() == "true"
        self.live_paper_trading = os.getenv("LIVE_PAPER_TRADING", "False").lower() == "true"
        self.dashboard_username = os.getenv("DASHBOARD_USERNAME", "admin")
        self.dashboard_password = os.getenv("DASHBOARD_PASSWORD", "admin123")
        self.strategy_mode = int(os.getenv("STRATEGY_MODE", 2))
        self.wide_grid_step_percent_min = float(os.getenv("WIDE_GRID_STEP_PERCENT_MIN", 2.0))
        self.wide_grid_step_percent_max = float(os.getenv("WIDE_GRID_STEP_PERCENT_MAX", 3.0))
        self.dense_grid_step_percent_min = float(os.getenv("DENSE_GRID_STEP_PERCENT_MIN", 0.5))
        self.dense_grid_step_percent_max = float(os.getenv("DENSE_GRID_STEP_PERCENT_MAX", 1.0))
        self.capital_reserve_percent = float(os.getenv("CAPITAL_RESERVE_PERCENT", 0.0))
        self.min_profit_percent = float(os.getenv("MIN_PROFIT_PERCENT", 0.5))
        self.stop_loss_capital_percent = float(os.getenv("STOP_LOSS_CAPITAL_PERCENT", 30.0))
        self.backtest_enabled = os.getenv("BACKTEST_ENABLED", "False").lower() == "true"
        self.backtest_timeframe = os.getenv("BACKTEST_TIMEFRAME", "1h")
        self.backtest_limit = int(os.getenv("BACKTEST_LIMIT", 1000))
        self.telegram_report_interval = int(os.getenv("TELEGRAM_REPORT_INTERVAL", 3600))
        self.telegram_volatile_interval = int(os.getenv("TELEGRAM_VOLATILE_INTERVAL", 600))
        self.telegram_volatility_threshold = float(os.getenv("TELEGRAM_VOLATILITY_THRESHOLD", 1.0))
        if self.strategy_mode == 1:
            self.base_grid_step_percent = self.wide_grid_step_percent_min
        elif self.strategy_mode == 2:
            self.base_grid_step_percent = self.dense_grid_step_percent_min
        if "DOGE" in self.symbol:
            self.trailing_up_activation = float(os.getenv("TRAILING_UP_ACTIVATION_DOGE", 0.7))
            self.trailing_down_activation = float(os.getenv("TRAILING_DOWN_ACTIVATION_DOGE", 0.5))

config = AdvancedConfig()

logging.info(f"Đã chọn chiến lược {config.strategy_mode}: {'Grid thưa (bước ~2-3%, có RSI)' if config.strategy_mode == 1 else 'Grid dày (bước ~0.5%, không dùng RSI)'}")
logging.info(f"Bước lưới cơ bản: {config.base_grid_step_percent:.2f}%, Mức grid tối thiểu: {config.min_grid_levels}, tối đa: {config.max_grid_levels}")

# ======== Utility Functions ======== 
def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
    if len(highs) < period or len(lows) < period or len(closes) < period:
        logging.warning("Không đủ dữ liệu để tính ATR")
        return 0.0
    tr_values = []
    for i in range(1, len(closes)):
        high = highs[i] if highs[i] is not None else 0.0
        low = lows[i] if lows[i] is not None else 0.0
        prev_close = closes[i-1] if closes[i-1] is not None else 0.0
        if high <= 0 or low <= 0 or prev_close <= 0:
            continue
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_values.append(tr)
    atr = sum(tr_values[-period:]) / period if len(tr_values) >= period else 0.0
    return atr

def calculate_rsi(prices: List[float], period: int) -> float:
    prices = [p for p in prices if p is not None and p > 0]
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices)
    ups = np.clip(deltas, a_min=0, a_max=None)
    downs = -np.clip(deltas, a_max=0, a_min=None)
    avg_gain = np.mean(ups[-period:]) if len(ups) >= period else 0.0
    avg_loss = np.mean(downs[-period:]) if len(downs) >= period else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def calculate_moving_average(prices: List[float], period: int) -> float:
    prices = [p for p in prices if p is not None and p > 0]
    if len(prices) < period:
        return 0.0
    return np.mean(prices[-period:])

def calculate_bollinger_bands(prices: List[float], period: int, std_dev: float) -> Tuple[float, float]:
    prices = [p for p in prices if p is not None and p > 0]
    if len(prices) < period:
        return 0.0, 0.0
    sma = np.mean(prices[-period:])
    std = np.std(prices[-period:])
    upper_band = sma + std_dev * std
    lower_band = sma - std_dev * std
    return lower_band, upper_band

def calculate_macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> float:
    prices = [p for p in prices if p is not None and p > 0]
    if len(prices) < slow + signal:
        return 0.0
    df = pd.Series(prices)
    ema_fast = df.ewm(span=fast, adjust=False).mean()
    ema_slow = df.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    return macd.iloc[-1] - signal_line.iloc[-1]

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    equity_curve = [e for e in equity_curve if e is not None]
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if value is not None and value > peak:
            peak = value
        dd = (peak - value) / peak * 100 if peak != 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd

def auto_adjust_rsi_thresholds(prices: List[float], default_low=25, default_high=75) -> Tuple[float, float]:
    rsi = calculate_rsi(prices, config.rsi_period)
    if rsi < 40:
        return (25, 65)
    elif rsi > 60:
        return (35, 75)
    else:
        return (default_low, default_high)

async def check_internet_connection() -> bool:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://www.google.com", timeout=aiohttp.ClientTimeout(total=5)) as response:
                return response.status == 200
        except Exception as e:
            logging.error(f"Kiểm tra kết nối internet thất bại: {str(e)}")
            return False

def dynamic_position_sizing(current_volatility: float) -> float:
    risk_per_trade = 0.02
    if current_volatility is None or current_volatility <= 0:
        current_volatility = 0.001
    position_size = (config.initial_investment * risk_per_trade) / (current_volatility * 2)
    return round(position_size, 4)

def adaptive_take_profit(prices: List[float]) -> float:
    rsi = calculate_rsi(prices, config.rsi_period)
    if rsi > 70:
        return 1.5
    elif rsi < 30:
        return 3.0
    return 2.0

def adjust_risk_by_volatility(profit_curve: List[float]) -> None:
    profit_curve = [p for p in profit_curve if p is not None]
    if len(profit_curve) < 50:
        return
    std_dev = np.std(profit_curve[-50:])
    if std_dev > 5:
        config.min_quantity *= 0.5
        logging.info(f"Giảm rủi ro: min_quantity = {config.min_quantity}")
    elif std_dev < 2:
        config.min_quantity *= 1.2
        logging.info(f"Tăng rủi ro: min_quantity = {config.min_quantity}")

def optimize_parameters_based_on_market(volatility: float, trend_strength: float) -> None:
    volatility = volatility if volatility is not None and volatility > 0 else 0.001
    trend_strength = trend_strength if trend_strength is not None else 0.0
    if volatility > 0.05:
        config.base_grid_step_percent = 2.5
    else:
        config.base_grid_step_percent = 1.5
    if trend_strength > 25:
        config.min_profit_percent = 3
    else:
        config.min_profit_percent = 1.5
    logging.info(f"Tối ưu tham số: grid_step={config.base_grid_step_percent}%, min_profit={config.min_profit_percent}%")

async def fetch_historical_data(exchange, timeframe: str, limit: int) -> pd.DataFrame:
    mexc = ccxt_async.mexc()
    try:
        await mexc.load_markets()
        symbol_str = config.symbol.replace("USDT", "/USDT")
        ohlcv = await mexc.fetch_ohlcv(symbol_str, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df
    finally:
        await mexc.close()

async def monitor_system_health() -> None:
    global last_health_alert_time
    current_time = time.time()
    cpu = psutil.cpu_percent()
    memory = psutil.virtual_memory().percent
    if (cpu > 85 or memory > 80) and (current_time - last_health_alert_time > HEALTH_ALERT_COOLDOWN):
        await DiscordNotifier().send_alert(f"⚠️ Cảnh báo: CPU {cpu}% | RAM {memory}%", is_error=True)
        last_health_alert_time = current_time

# ======== Data Classes ======== 
@dataclass
class MarketData:
    price: float
    high: float
    low: float
    volume: float

@dataclass
class TradeSignal:
    side: str
    price: float
    quantity: float

# ======== Enhanced Protection System ======== 
class EnhancedProtectionSystem:
    def __init__(self):
        self.price_history = deque(maxlen=config.volatility_window)
        self.high_history = deque(maxlen=config.volatility_window)
        self.low_history = deque(maxlen=config.volatility_window)
        self.volume_history = deque(maxlen=config.volatility_window)
        self.trailing_stop_price = 0.0
        self.trailing_buy_price = 0.0
        self.circuit_breaker_triggered = False
        self.circuit_breaker_start = None

    def update(self, price: float, high: float, low: float, volume: float) -> None:
        price = price if price is not None and price > 0 else 0.0
        high = high if high is not None and high > 0 else price
        low = low if low is not None and low > 0 else price
        volume = volume if volume is not None and volume >= 0 else 0.0

        if price == 0.0:
            logging.warning("Dữ liệu không hợp lệ: Giá bằng 0 sau kiểm tra")
            return

        self.price_history.append(price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(volume)

    def check_pump_protection(self, price: float) -> bool:
        if len(self.price_history) < 2 or price == 0.0:
            return False
        last_price = self.price_history[-2]
        if last_price == 0.0:
            return False
        price_change = (price - last_price) / last_price
        return price_change > config.pump_protection_threshold

    async def check_circuit_breaker(self, price: float) -> bool:
        if len(self.price_history) < 2 or price == 0.0:
            return False
        last_price = self.price_history[-2]
        if last_price == 0.0:
            return False
        price_change = abs(price - last_price) / last_price
        if price_change > config.circuit_breaker_threshold:
            self.circuit_breaker_triggered = True
            self.circuit_breaker_start = time.time()
            logging.warning(f"Ngắt mạch được kích hoạt: Giá thay đổi {price_change:.2%}")
            await DiscordNotifier().send_alert(f"Circuit breaker: biên độ giá quá lớn ({price_change:.2%}), tạm dừng giao dịch", is_error=True)
            return True
        return False

    async def check_circuit_breaker_status(self) -> bool:
        if self.circuit_breaker_triggered and self.circuit_breaker_start:
            elapsed = time.time() - self.circuit_breaker_start
            if elapsed > config.circuit_breaker_duration:
                self.circuit_breaker_triggered = False
                self.circuit_breaker_start = None
                logging.info("Ngắt mạch đã được đặt lại")
                await DiscordNotifier().send_alert("Circuit breaker đã được đặt lại")
                return False
            return True
        return False

    def check_abnormal_activity(self, volume: float) -> bool:
        if len(self.volume_history) < 2:
            return False
        avg_volume = np.mean([v for v in list(self.volume_history)[:-1] if v is not None and v >= 0])
        return volume > avg_volume * config.abnormal_activity_threshold if avg_volume > 0 else False

    def update_trailing_stop(self, price: float) -> Tuple[bool, float]:
        if not config.trailing_stop_enabled or price == 0.0:
            return False, self.trailing_stop_price
        atr = calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)
        if atr == 0.0:
            return False, self.trailing_stop_price
        if price > self.trailing_stop_price and price > config.trailing_up_activation:
            self.trailing_stop_price = price - atr * 3
        elif price < self.trailing_stop_price:
            self.trailing_stop_price = price + atr * 3
        should_stop = price <= self.trailing_stop_price
        return should_stop, self.trailing_stop_price

    def update_trailing_buy(self, price: float) -> Tuple[bool, float]:
        if not config.trailing_buy_stop_enabled or price == 0.0:
            return False, self.trailing_buy_price
        if self.trailing_buy_price == 0 or price < self.trailing_buy_price:
            self.trailing_buy_price = price * (1 + config.trailing_buy_activation_percent / 100)
        should_buy = price >= self.trailing_buy_price * (1 + config.trailing_buy_distance_percent / 100)
        return should_buy, self.trailing_buy_price

# ======== Enhanced Order Manager ======== 
class EnhancedOrderManager:
    def __init__(self):
        self.open_orders: List[Dict] = []
        self.is_paused = False
        self.completed_orders: List[Dict] = []

    async def cancel_all_orders(self, exchange: 'IExchange') -> None:
        async with order_lock:
            try:
                open_orders = await safe_api_call(exchange.get_open_orders, alert_on_failure=True)
                if open_orders is None:
                    logging.error("Danh sách lệnh trả về None")
                    return
                for order in open_orders:
                    if not isinstance(order, dict) or 'orderId' not in order:
                        logging.warning(f"Lệnh không hợp lệ: {order}")
                        continue
                    await safe_api_call(exchange.cancel_order, order['orderId'], alert_on_failure=True)
                self.open_orders.clear()
                logging.info("Đã hủy tất cả lệnh mở")
            except Exception as e:
                logging.error(f"Lỗi khi hủy tất cả lệnh: {str(e)}")
                await DiscordNotifier().send_alert(f"Lỗi nghiêm trọng: Không thể hủy lệnh - {str(e)}", is_error=True)

    async def check_and_handle_orders(self, current_price: float, exchange: 'IExchange',
                                      profit_tracker: 'EnhancedProfitTracker',
                                      grid_manager: 'EnhancedSmartGridManager') -> None:
        if current_price == 0.0:
            logging.error("Giá hiện tại không hợp lệ, bỏ qua xử lý lệnh")
            return

        try:
            orders = await safe_api_call(exchange.get_all_orders, alert_on_failure=True)
            if orders is None:
                logging.error("Danh sách lệnh trả về None")
                return
            for order in orders:
                if not isinstance(order, dict) or 'orderId' not in order or 'status' not in order:
                    logging.warning(f"Lệnh không hợp lệ: {order}")
                    continue
                if order['status'] == 'FILLED':
                    side = order['side'].upper()
                    try:
                        price = float(order.get('price', 0.0))
                        qty = float(order.get('executedQty', 0.0))
                    except (ValueError, TypeError) as e:
                        logging.error(f"Giá hoặc số lượng không hợp lệ trong lệnh {order['orderId']}: {str(e)}")
                        continue
                    if price <= 0 or qty <= 0:
                        logging.warning(f"Giá hoặc số lượng không hợp lệ: price={price}, qty={qty}")
                        continue
                    if order['orderId'] in grid_manager.static_order_ids and side == 'BUY':
                        grid_manager.trailing_stops[("BUY", round(price, grid_manager.price_precision))] = [price, qty]
                        grid_manager.static_order_ids.remove(order['orderId'])
                        grid_manager.static_orders_placed = False
                        logging.info(f"Đã khớp lệnh static BUY tại {price:.2f}, qty {qty:.6f}")
                        await DiscordNotifier().send_alert(f"Static BUY filled @ {price:.2f}, qty {qty:.6f}")
                        self.completed_orders.append({"side": side, "price": price, "quantity": qty})
                        if order['orderId'] in grid_manager.order_ids:
                            grid_manager.order_ids.remove(order['orderId'])
                        profit = (current_price - price) * qty
                        profit_tracker.record_trade(profit, 0)
                        continue
                    fee = qty * price * (config.maker_fee if order.get('type') == 'LIMIT' else config.taker_fee)
                    profit = (current_price - price) * qty if side == 'BUY' else (price - current_price) * qty
                    profit_tracker.record_trade(profit, fee)
                    if order['orderId'] in grid_manager.order_ids:
                        grid_manager.order_ids.remove(order['orderId'])
                    msg = (
                        f"Lệnh {side} đã khớp\n"
                        f"Giá: {price:.2f}, SL: {qty:.6f}\n"
                        f"Lợi nhuận: {profit:.2f} USDT\n"
                        f"Phí: {fee:.4f} USDT"
                    )
                    logging.info(f"[PAPER] {msg}")
                    await DiscordNotifier().send_alert(msg)
                    self.completed_orders.append({"side": side, "price": price, "quantity": qty})
                elif order['status'] == 'CANCELED' and order['orderId'] in grid_manager.order_ids:
                    grid_manager.order_ids.remove(order['orderId'])
        except Exception as e:
            logging.error(f"Lỗi khi xử lý lệnh: {str(e)}")
            await DiscordNotifier().send_alert(f"Lỗi nghiêm trọng: Xử lý lệnh thất bại - {str(e)}", is_error=True)

# ======== Enhanced Profit Tracker ======== 
class EnhancedProfitTracker:
    def __init__(self):
        self.total_profit = 0.0
        self.trade_count = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_fees = 0.0
        self.equity_curve = []

    def record_trade(self, profit: float, fee: float) -> None:
        profit = profit if profit is not None else 0.0
        fee = fee if fee is not None and fee >= 0 else 0.0
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
        return {
            "Total Profit": self.total_profit,
            "Trade Count": self.trade_count,
            "Win Rate": win_rate,
            "Max Drawdown": max_drawdown,
            "Total Fees": self.total_fees
        }

# ======== State Management ======== 
def save_state(states: Dict[str, 'BotState']) -> None:
    try:
        state_data = {}
        for symbol, state in states.items():
            state_data[symbol] = {
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
        asyncio.create_task(DiscordNotifier().send_alert(f"Lỗi nghiêm trọng: Không thể lưu trạng thái - {str(e)}", is_error=True))

async def verify_order_state(exchange: 'IExchange', local_order_ids: List[str]) -> None:
    try:
        actual_orders = await safe_api_call(exchange.get_open_orders, alert_on_failure=True)
        if actual_orders is None:
            logging.error("Không lấy được danh sách lệnh mở để đồng bộ")
            return
        actual_ids = {order.get('orderId') for order in actual_orders if isinstance(order, dict) and 'orderId' in order}
        for oid in local_order_ids.copy():
            if oid not in actual_ids:
                logging.warning(f"Lệnh {oid} không tồn tại trên sàn, xóa khỏi trạng thái cục bộ")
                local_order_ids.remove(oid)
    except Exception as e:
        logging.error(f"Lỗi khi đồng bộ trạng thái lệnh: {str(e)}")

def load_state(states: Dict[str, 'BotState']) -> None:
    try:
        if not os.path.exists(STATE_FILE):
            logging.info("Không tìm thấy file trạng thái, khởi tạo mới")
            return
        with open(STATE_FILE, 'r') as f:
            state_data = json.load(f)
        for symbol, state in states.items():
            if symbol in state_data:
                data = state_data[symbol]
                state.tracker.total_profit = data.get("total_profit", 0.0)
                state.tracker.trade_count = data.get("trade_count", 0)
                state.tracker.winning_trades = data.get("winning_trades", 0)
                state.tracker.losing_trades = data.get("losing_trades", 0)
                state.tracker.equity_curve = data.get("equity_curve", [])
                state.tracker.total_fees = data.get("total_fees", 0.0)
                state.grid_manager.order_ids = data.get("order_ids", [])
                state.grid_manager.static_order_ids = data.get("static_order_ids", [])
                state.grid_manager.grid_cooldown = data.get("grid_cooldown", {})
                state.grid_manager.trailing_stops = data.get("trailing_stops", {})
                state.order_manager.open_orders = data.get("open_orders", [])
                asyncio.run(verify_order_state(state.exchange, state.grid_manager.order_ids))
        logging.info("Đã tải trạng thái bot từ file")
    except Exception as e:
        logging.error(f"Lỗi khi load trạng thái bot: {str(e)}")
        asyncio.create_task(DiscordNotifier().send_alert(f"Lỗi nghiêm trọng: Không thể tải trạng thái - {str(e)}", is_error=True))

# ======== Grid Manager ======== 
class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels = []
        self.static_levels = 3
        self.static_orders_placed = False
        self.static_order_ids = []
        self.order_ids = []
        self.trailing_stops = {}
        self.grid_cooldown = {}
        self.price_precision = 2
        self.FALLBACK_COOLDOWN = 300
        self.price_history = deque(maxlen=config.volatility_window)
        self.high_history = deque(maxlen=config.volatility_window)
        self.low_history = deque(maxlen=config.volatility_window)
        self.last_static_order_time = 0.0
        self.last_fallback_time = 0.0

    async def calculate_adaptive_grid(self, current_price: float, exchange: 'IExchange') -> None:
        if current_price == 0.0:
            logging.warning("Giá hiện tại không hợp lệ, bỏ qua tính toán grid")
            return

        self.price_history.append(current_price)
        if not config.adaptive_grid_enabled or len(self.price_history) < config.volatility_window:
            self.grid_levels = self._create_static_grid(current_price)
            return

        atr = calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)
        atr = atr if atr is not None and atr > 0 else current_price * 0.001
        volatility = min(atr / current_price, 0.1)

        order_book = await safe_api_call(exchange.get_order_book, alert_on_failure=True)
        if not isinstance(order_book, dict) or 'bids' not in order_book or 'asks' not in order_book:
            logging.warning("Không lấy được order book, sử dụng grid mặc định")
            self.grid_levels = self._create_static_grid(current_price)
            return

        best_bid = order_book['bids'][0][0] if order_book.get('bids') and len(order_book['bids']) > 0 else current_price
        best_ask = order_book['asks'][0][0] if order_book.get('asks') and len(order_book['asks']) > 0 else current_price
        best_bid = best_bid if best_bid is not None and best_bid > 0 else current_price
        best_ask = best_ask if best_ask is not None and best_ask > 0 else current_price
        spread = (best_ask - best_bid) / best_bid if best_bid > 0 else 0.01

        num_levels = max(config.min_grid_levels, min(config.max_grid_levels, int(volatility / (config.base_grid_step_percent / 100)))
        raw_step = max(volatility / num_levels, spread * 2) if num_levels > 0 else (config.base_grid_step_percent / 100)
        step_percent = (max(config.wide_grid_step_percent_min / 100, min(config.wide_grid_step_percent_max / 100, raw_step)) 
                        if config.strategy_mode == 1 
                        else max(config.dense_grid_step_percent_min / 100, min(config.dense_grid_step_percent_max / 100, raw_step))

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
        if current_price == 0.0:
            return []

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
        if price == 0.0:
            return False

        target = round(price, self.price_precision)
        for order in open_orders or []:
            if not isinstance(order, dict) or 'price' not in order or order['price'] is None:
                continue
            try:
                order_price = float(order['price'])
            except (ValueError, TypeError):
                continue
            order_price = round(order_price, self.price_precision)
            if order['side'].upper() == side.upper() and order_price == target:
                return True
        return False

    async def place_static_orders(self, current_price: float, exchange: 'IExchange') -> None:
        if current_price == 0.0:
            logging.warning("Giá hiện tại không hợp lệ, bỏ qua đặt lệnh static")
            return

        if config.strategy_mode == 2:
            logging.info("Chế độ 2: Bỏ qua đặt lệnh static.")
            return

        current_time = time.time()
        if current_time - self.last_static_order_time < config.order_cooldown_sec:
            wait = config.order_cooldown_sec - (current_time - self.last_static_order_time)
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
            open_orders = await safe_api_call(exchange.get_open_orders, alert_on_failure=True)
            if open_orders is None:
                logging.error("Không lấy được danh sách lệnh mở, bỏ qua đặt lệnh")
                return

            # Lấy thông tin filters và kiểm tra kỹ lưỡng
            filters = await safe_api_call(exchange.get_symbol_filters, alert_on_failure=True)
            if not filters:
                logging.error("Không lấy được filters, bỏ qua đặt lệnh")
                return

            # Kiểm tra và gán giá trị mặc định nếu cần
            try:
                min_qty = float(filters.get('minQty', 1.0))
                max_qty = float(filters.get('maxQty', 10000.0))
                min_price = float(filters.get('minPrice', 0.00001))
                max_price = float(filters.get('maxPrice', 10000.0))
                tick_size = float(filters.get('tickSize', filters.get('stepSize', 0.00001)))
                if tick_size <= 0:
                    tick_size = 0.00001
            except (ValueError, TypeError) as e:
                logging.error(f"Lỗi khi xử lý filters: {str(e)}")
                await DiscordNotifier().send_alert(f"Lỗi xử lý filters: {str(e)}", is_error=True)
                return

            usdt_balance, _ = await safe_api_call(exchange.get_balance, alert_on_force=True)
            usdt_balance = usdt_balance if usdt_balance is not None and usdt_balance >= 0 else 0.0
            if usdt_balance == 0.0:
                logging.error("Không lấy được số dư hoặc số dư bằng 0, bỏ qua đặt lệnh")
                return

            allocated_funds_per_order = (usdt_balance * (1 - config.capital_reserve_percent / 100)) / (2 * self.static_levels)
            quantity_per_level = allocated_funds_per_order / current_price if current_price > 0 else 0.0

            try:
                atr = calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)
                position_size = dynamic_position_sizing(atr if atr is not None else 0.001)
                quantity = max(min_qty, position_size)
                if max_qty is not None:
                    quantity = min(quantity, max_qty)
            except Exception as e:
                logging.error(f"Lỗi tính quantity: {str(e)}")
                return

            # Kiểm tra số lượng hợp lệ
            if quantity is None or quantity <= 0:
                logging.error("Số lượng không hợp lệ sau khi tính toán")
                return

            # Làm tròn với tick_size
            try:
                quantity = round(quantity / tick_size) * tick_size
            except Exception as e:
                logging.error(f"Lỗi làm tròn quantity: {str(e)}")
                return

            if (quantity < min_qty) or (max_qty is not None and quantity > max_qty):
                logging.warning(f"Số lượng không hợp lệ: {quantity} (min: {min_qty}, max: {max_qty})")
                await DiscordNotifier().send_alert(f"Số lượng không hợp lệ: {quantity}", is_error=True)
                return

            static_grid = self._create_static_grid(current_price)[:self.static_levels]
            for i, (buy_price, sell_price) in enumerate(static_grid, 1):
                if buy_price <= 0 or sell_price <= 0:
                    logging.warning(f"Mức lưới tĩnh {i} không hợp lệ: buy={buy_price}, sell={sell_price}")
                    continue

                if buy_price < min_price or buy_price > max_price:
                    logging.warning(f"Giá mua {buy_price} ngoài khoảng filter [{min_price}, {max_price}]")
                    continue

                profit_percent = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                if profit_percent < config.min_profit_percent:
                    logging.info(f"Mức lưới tĩnh {i} không đạt lợi nhuận tối thiểu: {profit_percent:.2f}% < {config.min_profit_percent}%")
                    continue

                required_usdt = buy_price * quantity
                if usdt_balance < required_usdt:
                    logging.warning(f"USDT không đủ để đặt lệnh MUA tĩnh (cần {required_usdt:.2f}, có {usdt_balance:.2f})")
                    break

                if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                    try:
                        buy_price_rounded = round(buy_price / tick_size) * tick_size
                        if buy_price_rounded < min_price or buy_price_rounded > max_price:
                            logging.warning(f"Giá làm tròn {buy_price_rounded} vẫn ngoài khoảng cho phép")
                            continue

                        # Đặt lệnh và kiểm tra kết quả
                        buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price_rounded, alert_on_failure=True)
                        if buy_order and isinstance(buy_order, dict) and 'orderId' in buy_order:
                            self.order_ids.append(buy_order['orderId'])
                            self.static_order_ids.append(buy_order['orderId'])
                            logging.info(f"(Static) Đã đặt lệnh MUA tại {buy_price_rounded:.2f}, SL: {quantity:.6f}")
                        else:
                            logging.warning(f"Lệnh {i} không đặt được do lỗi API: {buy_order}")
                    except Exception as e:
                        logging.error(f"Lỗi khi đặt lệnh static {i}: {str(e)}")
            self.static_orders_placed = True
            self.last_static_order_time = current_time
            logging.info("Đã hoàn tất đặt lệnh static")

    async def place_grid_orders(self, current_price: float, exchange: 'IExchange') -> None:
        if current_price == 0.0:
            logging.warning("Giá hiện tại không hợp lệ, bỏ qua đặt lệnh grid")
            return

        async with order_lock:
            open_orders = await safe_api_call(exchange.get_open_orders, alert_on_failure=True)
            if open_orders is None:
                logging.error("Không lấy được danh sách lệnh mở, bỏ qua đặt lệnh")
                return

            # Lấy thông tin filters và kiểm tra kỹ lưỡng
            filters = await safe_api_call(exchange.get_symbol_filters, alert_on_failure=True)
            if not filters:
                logging.error("Không lấy được filters, bỏ qua đặt lệnh")
                return

            # Kiểm tra và gán giá trị mặc định nếu cần
            try:
                min_qty = float(filters.get('minQty', 1.0))
                max_qty = float(filters.get('maxQty', 10000.0))
                min_price = float(filters.get('minPrice', 0.00001))
                max_price = float(filters.get('maxPrice', 10000.0))
                tick_size = float(filters.get('tickSize', filters.get('stepSize', 0.00001)))
                if tick_size <= 0:
                    tick_size = 0.00001
            except (ValueError, TypeError) as e:
                logging.error(f"Lỗi khi xử lý filters: {str(e)}")
                await DiscordNotifier().send_alert(f"Lỗi xử lý filters: {str(e)}", is_error=True)
                return

            usdt_balance, coin_balance = await safe_api_call(exchange.get_balance, alert_on_failure=True)
            usdt_balance = usdt_balance if usdt_balance is not None and usdt_balance >= 0 else 0.0
            coin_balance = coin_balance if coin_balance is not None and coin_balance >= 0 else 0.0
            if usdt_balance == 0.0 and coin_balance == 0.0:
                logging.error("Không lấy được số dư hoặc số dư bằng 0, bỏ qua đặt lệnh")
                return

            num_levels = len(self.grid_levels)
            if num_levels == 0:
                logging.warning("Không có mức grid nào được tính toán")
                return

            allocated_funds_per_order = (usdt_balance * (1 - config.capital_reserve_percent / 100)) / (2 * num_levels)
            
            try:
                atr = calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)
                position_size = dynamic_position_sizing(atr if atr is not None else 0.001)
                quantity = max(min_qty, position_size)
                if max_qty is not None:
                    quantity = min(quantity, max_qty)
            except Exception as e:
                logging.error(f"Lỗi tính quantity: {str(e)}")
                return

            # Kiểm tra số lượng hợp lệ
            if quantity is None or quantity <= 0:
                logging.error("Số lượng không hợp lệ sau khi tính toán")
                return

            # Làm tròn với tick_size
            try:
                quantity = round(quantity / tick_size) * tick_size
            except Exception as e:
                logging.error(f"Lỗi làm tròn quantity: {str(e)}")
                return

            if (quantity < min_qty) or (max_qty is not None and quantity > max_qty):
                logging.warning(f"Số lượng không hợp lệ: {quantity} (min: {min_qty}, max: {max_qty})")
                await DiscordNotifier().send_alert(f"Số lượng không hợp lệ: {quantity}", is_error=True)
                return

            open_count = len([o for o in open_orders if isinstance(o, dict)])
            current_time = time.time()

            for i, (buy_price, sell_price) in enumerate(self.grid_levels[self.static_levels:], start=self.static_levels):
                if buy_price == 0.0 or sell_price == 0.0:
                    logging.warning(f"Mức lưới {i} không hợp lệ: buy={buy_price}, sell={sell_price}")
                    continue

                profit_percent = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                if profit_percent < config.min_profit_percent:
                    continue

                if buy_price < current_price:
                    price_key = ("BUY", round(buy_price, self.price_precision))
                    if current_time < self.grid_cooldown.get(price_key, 0):
                        continue
                    if usdt_balance < buy_price * quantity:
                        logging.warning(f"USDT không đủ để đặt lệnh MUA (cần {buy_price * quantity:.2f}, có {usdt_balance:.2f})")
                        break
                    if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                        buy_price_rounded = round(buy_price / tick_size) * tick_size
                        if buy_price_rounded < min_price or buy_price_rounded > max_price:
                            continue
                        # Đặt lệnh và kiểm tra kết quả
                        buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price_rounded, alert_on_failure=True)
                        if buy_order and isinstance(buy_order, dict) and 'orderId' in buy_order:
                            self.order_ids.append(buy_order['orderId'])
                            open_count += 1
                            logging.info(f"(Dynamic) Đã đặt lệnh MUA tại {buy_price_rounded:.2f}, SL: {quantity:.6f}")
                        else:
                            logging.warning(f"Lệnh BUY không đặt được: {buy_order}")

                if sell_price > current_price:
                    price_key = ("SELL", round(sell_price, self.price_precision))
                    if current_time < self.grid_cooldown.get(price_key, 0):
                        continue
                    if open_count >= config.max_open_orders:
                        logging.warning(f"Đạt giới hạn lệnh tối đa ({open_count}/{config.max_open_orders})")
                        break
                    if coin_balance < quantity:
                        logging.warning(f"Số dư coin không đủ để đặt lệnh BÁN (cần {quantity:.6f}, có {coin_balance:.6f})")
                        continue
                    sell_price_rounded = round(sell_price / tick_size) * tick_size
                    if sell_price_rounded < min_price or sell_price_rounded > max_price:
                        continue
                    # Đặt lệnh và kiểm tra kết quả
                    sell_order = await safe_api_call(exchange.place_order, side='SELL', order_type='LIMIT', quantity=quantity, price=sell_price_rounded, alert_on_failure=True)
                    if sell_order and isinstance(sell_order, dict) and 'orderId' in sell_order:
                        self.order_ids.append(sell_order['orderId'])
                        open_count += 1
                        logging.info(f"(Dynamic) Đã đặt lệnh BÁN tại {sell_price_rounded:.2f}, SL: {quantity:.6f}")
                    else:
                        logging.warning(f"Lệnh SELL không đặt được: {sell_order}")

            has_buy_order = any(o['side'].upper() == 'BUY' for o in open_orders if isinstance(o, dict))
            if not has_buy_order and len([o for o in open_orders if isinstance(o, dict) and o['side'].upper() == 'BUY']) < 2 and current_time - self.last_fallback_time > self.FALLBACK_COOLDOWN:
                buy_price = current_price * (1 - config.base_grid_step_percent / 100)
                buy_price_rounded = round(buy_price / tick_size) * tick_size
                if (buy_price_rounded > 0 and 
                    not self.duplicate_order_exists("BUY", buy_price_rounded, open_orders) and 
                    buy_price_rounded >= min_price and buy_price_rounded <= max_price):
                    # Đặt lệnh và kiểm tra kết quả
                    buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price_rounded, alert_on_failure=True)
                    if buy_order and isinstance(buy_order, dict) and 'orderId' in buy_order:
                        self.order_ids.append(buy_order['orderId'])
                        self.last_fallback_time = current_time
                        logging.info(f"(Fallback) Đã đặt lệnh MUA tại {buy_price_rounded:.2f}, SL: {quantity:.6f}")
                    else:
                        logging.warning(f"Fallback BUY không đặt được: {buy_order}")

    async def cancel_all_orders(self, exchange: 'IExchange', preserve_static: bool = True) -> None:
        async with order_lock:
            orders_to_cancel = self.order_ids.copy()
            if preserve_static:
                orders_to_cancel = [oid for oid in orders_to_cancel if oid not in self.static_order_ids]
            for order_id in orders_to_cancel:
                try:
                    await safe_api_call(exchange.cancel_order, order_id, alert_on_failure=True)
                    if order_id in self.order_ids:
                        self.order_ids.remove(order_id)
                except Exception as e:
                    logging.warning(f"Lỗi khi hủy lệnh {order_id}: {str(e)}")
            if not preserve_static:
                self.order_ids.clear()
                self.static_order_ids.clear()
                self.static_orders_placed = False

    def set_cooldown(self, side: str, price: float):
        if price == 0.0:
            return
        key = (side.upper(), round(price, self.price_precision))
        self.grid_cooldown[key] = time.time() + config.order_cooldown_sec
        logging.info(f"Đặt cooldown cho {key} đến {self.grid_cooldown[key]:.2f}")

    def get_grid_range(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.grid_levels:
            return None, None
        lows = [l for l, _ in self.grid_levels if l is not None and l > 0]
        highs = [h for _, h in self.grid_levels if h is not None and h > 0]
        return min(lows) if lows else None, max(highs) if highs else None

    async def rebalance_if_needed(self, current_price: float, exchange: 'IExchange') -> None:
        pass

# ======== Strategy Interface and Implementations ======== 
class IGridStrategy(ABC):
    @abstractmethod
    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        pass

class AdaptiveGridStrategy(IGridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        price = data.price if data.price is not None and data.price > 0 else 0.0
        if price == 0.0:
            return []

        signals = []
        prices = list(self.grid_manager.price_history)
        if len(prices) < max(config.rsi_period, config.ma_short_period, config.ma_long_period, config.bb_period):
            return signals

        rsi = calculate_rsi(prices, config.rsi_period)
        rsi_low, rsi_high = auto_adjust_rsi_thresholds(prices)
        if rsi < rsi_low:
            signals.append(TradeSignal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))
        elif rsi > rsi_high:
            signals.append(TradeSignal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))

        lower_band, upper_band = calculate_bollinger_bands(prices, config.bb_period, config.bb_std_dev)
        if data.price <= lower_band:
            signals.append(TradeSignal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))
        elif data.price >= upper_band:
            signals.append(TradeSignal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))

        ma_short = calculate_moving_average(prices, config.ma_short_period)
        ma_long = calculate_moving_average(prices, config.ma_long_period)
        if ma_short > ma_long and data.price > ma_short:
            signals.append(TradeSignal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))
        elif ma_short < ma_long and data.price < ma_short:
            signals.append(TradeSignal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))

        macd = calculate_macd(prices)
        if macd > 0:
            signals.append(TradeSignal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))
        elif macd < 0:
            signals.append(TradeSignal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))

        return signals

class DenseGridStrategy(IGridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        price = data.price if data.price is not None and data.price > 0 else 0.0
        if price == 0.0:
            return []

        signals = []
        for buy_price, sell_price in self.grid_manager.grid_levels:
            if data.price > buy_price:
                signals.append(TradeSignal(side="BUY", price=buy_price, quantity=config.min_quantity))
            if data.price < sell_price:
                signals.append(TradeSignal(side="SELL", price=sell_price, quantity=config.min_quantity))
        return signals

class BreakoutStrategy(IGridStrategy):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        price = data.price if data.price is not None and data.price > 0 else 0.0
        if price == 0.0:
            return []

        signals = []
        df = await fetch_historical_data(None, '4h', 100)
        key_resistance = df['high'].rolling(50).max().iloc[-1] if not df.empty else price
        key_support = df['low'].rolling(50).min().iloc[-1] if not df.empty else price
        if price > key_resistance:
            signals.append(TradeSignal(side="BUY", price=price * 1.001, quantity=config.min_quantity))
        elif price < key_support:
            signals.append(TradeSignal(side="SELL", price=price * 0.999, quantity=config.min_quantity))
        return signals

# ======== Trend Detector ======== 
class TrendDetector:
    def __init__(self, state: 'BotState'):
        self.state = state

    async def detect_trend_strength(self) -> float:
        highs = list(self.state.protection.high_history)
        lows = list(self.state.protection.low_history)
        closes = list(self.state.grid_manager.price_history)
        if len(highs) < 14 or len(lows) < 14 or len(closes) < 14:
            return 0.0

        highs = [h for h in highs if h is not None and h > 0]
        lows = [l for l in lows if l is not None and l > 0]
        closes = [c for c in closes if c is not None and c > 0]

        if len(highs) != len(lows) or len(highs) != len(closes):
            min_len = min(len(highs), len(lows), len(closes))
            highs = highs[-min_len:]
            lows = lows[-min_len:]
            closes = closes[-min_len:]

        if not highs or not lows or not closes:
            return 0.0

        df = pd.DataFrame({'high': highs, 'low': lows, 'close': closes})
        adx_df = pd.DataFrame({'ADX_14': [0] * len(df)})
        try:
            import pandas_ta as ta
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
        except ImportError:
            logging.warning("pandas_ta không được cài đặt, giả lập ADX = 0")
        trend_strength = adx_df['ADX_14'].iloc[-1] if not adx_df.empty else 0.0
        if trend_strength > 25:
            await self._activate_anti_trend_mode()
        return trend_strength

    async def _activate_anti_trend_mode(self):
        config.adaptive_grid_enabled = False
        await self.state.grid_manager.cancel_all_orders(self.state.exchange)
        self.state.strategy = BreakoutStrategy(self.state.grid_manager)
        logging.info("Chuyển sang chiến lược Breakout do xu hướng mạnh")
        await DiscordNotifier().send_alert("Chuyển sang chiến lược Breakout do xu hướng mạnh")

# ======== News Analyzer ======== 
class NewsAnalyzer:
    def __init__(self):
        self.news_api_key = os.getenv("NEWS_API_KEY", "your_news_api_key")
        self.sentiment = 0.0

    async def update_sentiment(self) -> None:
        keywords = "Dogecoin cryptocurrency economy Trump tax policy"
        url = f"https://newsapi.org/v2/everything?q={keywords}&apiKey={self.news_api_key}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    articles = await response.json()
                    self.sentiment = self._analyze_sentiment(articles)
                    if abs(self.sentiment) > 0.5:
                        logging.info(f"Tin tức quan trọng: Tâm lý {self.sentiment}")
                        await DiscordNotifier().send_alert(f"Tin tức quan trọng: Tâm lý {self.sentiment}")
        except Exception as e:
            logging.error(f"Lỗi khi lấy tin tức: {str(e)}")

    def _analyze_sentiment(self, articles: Dict) -> float:
        if not isinstance(articles, dict) or 'articles' not in articles:
            return 0.0
        sentiments = [TextBlob(a.get('title', '')).sentiment.polarity for a in articles['articles'] if isinstance(a, dict)]
        return sum(sentiments) / len(sentiments) if sentiments else 0.0

# ======== Black Swan Protection ======== 
class BlackSwanProtection:
    def __init__(self, state: 'BotState'):
        self.state = state
        self.last_order_book = None
        self.last_black_swan_time = 0.0

    async def monitor(self) -> None:
        current_time = time.time()
        if current_time - self.state.last_price_update > 10:
            logging.info("Dữ liệu thị trường chưa cập nhật - bỏ qua kiểm tra Black Swan")
            return

        liquidity_ratio = await self._calculate_liquidity()
        if liquidity_ratio is not None and liquidity_ratio < 0.5:
            order_book = self.last_order_book or await safe_api_call(self.state.exchange.get_order_book, alert_on_failure=True)
            if not isinstance(order_book, dict):
                return

            best_bid = order_book['bids'][0][0] if order_book.get('bids') and len(order_book['bids']) > 0 else None
            best_ask = order_book['asks'][0][0] if order_book.get('asks') and len(order_book['asks']) > 0 else None
            best_bid = best_bid if best_bid is not None and best_bid > 0 else 0.0
            best_ask = best_ask if best_ask is not None and best_ask > 0 else 0.0
            spread = (best_ask - best_bid) / best_bid if best_bid and best_ask and best_bid > 0 else 0.0

            price_drop = 0.0
            if len(self.state.protection.price_history) >= 2:
                last_price = self.state.protection.price_history[-2]
                last_price = last_price if last_price is not None and last_price > 0 else 0.0
                current_price = self.state.current_price if self.state.current_price is not None and self.state.current_price > 0 else 0.0
                if last_price > 0 and current_price > 0:
                    price_drop = (last_price - current_price) / last_price

            abnormal_vol = self.state.protection.check_abnormal_activity(self.state.current_volume)
            if (price_drop is not None and config.price_drop_threshold is not None and price_drop > config.price_drop_threshold / 100) or (spread > 0.05 and abnormal_vol):
                if current_time - self.last_black_swan_time > 60:
                    config.min_profit_percent = 1.5
                    logging.info(f"Đã điều chỉnh trading: min_profit={config.min_profit_percent}%")
                    await self._emergency_liquidation()
                    self.last_black_swan_time = current_time
            else:
                logging.info("Phát hiện thanh khoản thấp nhưng chưa đủ điều kiện Black Swan")

    async def _calculate_liquidity(self) -> float:
        try:
            order_book = await safe_api_call(self.state.exchange.get_order_book, alert_on_failure=True)
            if not isinstance(order_book, dict):
                return 1.0
            self.last_order_book = order_book
            bid_liquidity = sum([bid[1] for bid in order_book['bids'][:10] if isinstance(bid, list) and len(bid) >= 2 and bid[1] is not None] if 'bids' in order_book else [])
            ask_liquidity = sum([ask[1] for ask in order_book['asks'][:10] if isinstance(ask, list) and len(ask) >= 2 and ask[1] is not None] if 'asks' in order_book else [])
            if bid_liquidity == 0 or ask_liquidity == 0:
                return 0.0
            return min(bid_liquidity, ask_liquidity) / max(bid_liquidity, ask_liquidity)
        except Exception as e:
            logging.error(f"Lỗi tính thanh khoản: {str(e)}")
            return 1.0

    async def _emergency_liquidation(self) -> None:
        await self.state.order_manager.cancel_all_orders(self.state.exchange)
        usdt_balance, coin_balance = await safe_api_call(self.state.exchange.get_balance, alert_on_failure=True)
        coin_balance = coin_balance if coin_balance is not None and coin_balance > 0 else 0.0
        if coin_balance > 0:
            try:
                await safe_api_call(self.state.exchange.place_order, "SELL", "MARKET", coin_balance, alert_on_failure=True)
                logging.warning("Thực hiện thanh lý khẩn cấp do sự kiện Black Swan")
                await DiscordNotifier().send_alert("Thanh lý khẩn cấp: Phát hiện sự kiện Black Swan", is_error=True)
            except Exception as e:
                logging.error(f"Lỗi khi thanh lý khẩn cấp: {str(e)}")
                await DiscordNotifier().send_alert(f"Lỗi nghiêm trọng: Thanh lý khẩn cấp thất bại - {str(e)}", is_error=True)

# ======== ML Predictor ======== 
class SimpleMLPredictor:
    def __init__(self):
        self.model = xgb.XGBClassifier()
        self.trained = False
        self.train_features = []
        self.train_labels = []

    def train(self, features: List[List[float]], labels: List[int]) -> None:
        if len(features) < 50 or len(labels) != len(features):
            logging.warning(f"Dữ liệu huấn luyện chưa đủ: features={len(features)}, labels={len(labels)}")
            return

        features = [[f if f is not None else 0.0 for f in feature] for feature in features]
        labels = [l if l is not None else 0 for l in labels]

        train_size = int(len(features) * 0.8)
        X_train, X_test = features[:train_size], features[train_size:]
        y_train, y_test = labels[:train_size], labels[train_size:]
        self.model.fit(np.array(X_train), np.array(y_train))
        self.trained = True
        accuracy = self.model.score(np.array(X_test), np.array(y_test))
        logging.info(f"Đã huấn luyện ML model, độ chính xác trên tập kiểm tra: {accuracy:.2f}")

    def predict(self, features: List[float]) -> int:
        if not self.trained:
            logging.warning("Model chưa được huấn luyện, trả về dự đoán mặc định")
            return 0

        features = [f if f is not None else 0.0 for f in features]
        pred = int(self.model.predict([features])[0])
        logging.info(f"ML dự đoán: {'MUA' if pred == 1 else 'BÁN' if pred == -1 else 'GIỮ'}")
        return pred

# ======== Bot State ======== 
class BotState:
    def __init__(self, exchange: 'IExchange', strategy: IGridStrategy, symbol: str):
        self.exchange = exchange
        self.exchange_name = exchange.exchange_name
        self.symbol = symbol
        self.tracker = EnhancedProfitTracker()
        self.protection = EnhancedProtectionSystem()
        self.current_price = 0.0
        self.current_volume = 0.0
        self.current_high = 0.0
        self.current_low = 0.0
        self.grid_manager = EnhancedSmartGridManager()
        self.order_manager = EnhancedOrderManager()
        self.strategy = strategy
        self.event_queue = asyncio.Queue()
        self.last_price_update = 0.0
        self.live_paper_trading = config.live_paper_trading and self.exchange_name == "paper_trading"
        self.last_report_time = time.time()
        self.trend_detector = TrendDetector(self)
        self.news_analyzer = NewsAnalyzer()
        self.black_swan_protection = BlackSwanProtection(self)
        self.ml_predictor = SimpleMLPredictor()
        self.ml_features = []
        self.ml_labels = []
        self.notifier = DiscordNotifier()

    async def process_events(self) -> None:
        try:
            start_time = time.time()
            current_time = time.time()

            if current_time - self.last_price_update >= 3:
                if not await check_internet_connection():
                    logging.error("Không có kết nối Internet, sử dụng giá trị gần nhất")
                    await self.notifier.send_alert("Không có kết nối Internet, bot tạm dừng lấy dữ liệu mới", is_error=True)
                else:
                    tasks = [
                        safe_api_call(self.exchange.get_price, alert_on_failure=True),
                        safe_api_call(self.exchange.get_volume, alert_on_failure=True),
                        safe_api_call(self.exchange.get_high_low, alert_on_failure=True)
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    price, volume, high_low = None, None, (None, None)
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            logging.error(f"Lỗi khi lấy dữ liệu {['price', 'volume', 'high_low'][i]}: {str(result)}")
                        elif i == 0:
                            price = result
                        elif i == 1:
                            volume = result
                        elif i == 2:
                            high_low = result

                    price = price if price is not None and isinstance(price, (int, float)) and price > 0 else self.current_price
                    volume = volume if volume is not None and volume >= 0 else self.current_volume
                    high, low = high_low if high_low and high_low[0] is not None and high_low[1] is not None else (self.current_high, self.current_low)
                    high = high if high is not None and high > 0 else price
                    low = low if low is not None and low > 0 else price

                    if price > 0:
                        self.current_price = price
                        self.current_volume = volume
                        self.current_high = high
                        self.current_low = low
                        self.last_price_update = current_time
                        logging.info(f"Lấy dữ liệu thị trường thành công: price={self.current_price}, volume={self.current_volume}")

            self.protection.update(self.current_price, self.current_high, self.current_low, self.current_volume)
            if await check_protections(self, self.exchange, self.current_price, self.current_volume, self.current_high, self.current_low):
                return

            health = await self.exchange.health_check()
            if not isinstance(health, dict) or health.get('status') != 'healthy':
                logging.warning(f"Sàn không ổn định: {health.get('status', 'Không xác định')}")
                await self.notifier.send_alert(f"Sàn không ổn định: {health.get('status', 'Không xác định')}", is_error=True)
                return

            trend_strength = await self.trend_detector.detect_trend_strength()
            await self.news_analyzer.update_sentiment()
            await self.black_swan_protection.monitor()
            await monitor_system_health()

            atr = calculate_atr(list(self.protection.high_history), list(self.protection.low_history), list(self.grid_manager.price_history), config.atr_period)
            volatility = atr / self.current_price if self.current_price > 0 else 0.001
            optimize_parameters_based_on_market(volatility, trend_strength)
            adjust_risk_by_volatility(self.tracker.equity_curve)

            features = [
                calculate_rsi(list(self.grid_manager.price_history), config.rsi_period),
                calculate_moving_average(list(self.grid_manager.price_history), config.ma_short_period),
                calculate_moving_average(list(self.grid_manager.price_history), config.ma_long_period),
                calculate_macd(list(self.grid_manager.price_history)),
                calculate_bollinger_bands(list(self.grid_manager.price_history), config.bb_period, config.bb_std_dev)[0],
                calculate_bollinger_bands(list(self.grid_manager.price_history), config.bb_period, config.bb_std_dev)[1],
            ]
            self.ml_features.append(features)
            if len(self.ml_features) > 50:
                profits = [self.tracker.equity_curve[i] - self.tracker.equity_curve[i-1] if i > 0 else 0 for i in range(len(self.tracker.equity_curve[-51:-1]))]
                self.ml_labels = [1 if p > 0 else -1 if p < 0 else 0 for p in profits]
                self.ml_predictor.train(self.ml_features[-50:], self.ml_labels)
            if self.ml_predictor.trained:
                prediction = self.ml_predictor.predict(features)

            usdt_balance, coin_balance = await safe_api_call(self.exchange.get_balance, alert_on_failure=True)
            usdt_balance = usdt_balance if usdt_balance is not None and usdt_balance >= 0 else 0.0
            coin_balance = coin_balance if coin_balance is not None and coin_balance >= 0 else 0.0
            if usdt_balance == 0.0 and coin_balance == 0.0:
                logging.error("Không lấy được số dư, tạm dừng xử lý")
                return

            capital = usdt_balance + coin_balance * self.current_price if self.current_price > 0 else usdt_balance
            initial_capital = config.initial_investment
            if capital < initial_capital * (1 - config.stop_loss_capital_percent / 100):
                message = f"Cảnh báo: Vốn giảm quá {config.stop_loss_capital_percent}%, dừng bot\nVốn hiện tại: {capital:.2f} USDT"
                logging.error(message)
                await self.notifier.send_alert(message, is_error=True)
                sys.exit()

            if config.strategy_mode != 2:
                await self.grid_manager.place_static_orders(self.current_price, self.exchange)
            if config.strategy_mode == 2:
                self.grid_manager.high_history.append(self.current_high)
                self.grid_manager.low_history.append(self.current_low)
            await self.grid_manager.calculate_adaptive_grid(self.current_price, self.exchange)
            await self.grid_manager.place_grid_orders(self.current_price, self.exchange)

            signals = await self.strategy.generate_signals(MarketData(
                price=self.current_price,
                high=self.current_high,
                low=self.current_low,
                volume=self.current_volume))
            async with order_lock:
                for signal in signals:
                    order = await safe_api_call(self.exchange.place_order, signal.side, "LIMIT", signal.quantity, signal.price, alert_on_failure=True)
                    if order and 'orderId' in order:
                        self.grid_manager.order_ids.append(order['orderId'])

            await self.order_manager.check_and_handle_orders(self.current_price, self.exchange, self.tracker, self.grid_manager)
            if config.trailing_stop_enabled:
                await self.handle_trailing_stops()
            await self.grid_manager.rebalance_if_needed(self.current_price, self.exchange)
            logging.info(f"process_events chạy mất {time.time() - start_time:.2f}s")

            if current_time - self.last_report_time >= 1200:  # 20 phút
                await self.notifier.send_report(self)
                self.last_report_time = current_time
        except Exception as e:
            error_msg = f"LỖI CHI TIẾT:\n{traceback.format_exc()}"
            await self.notifier.send_alert(error_msg, is_error=True)
            logging.error(error_msg)

    async def handle_trailing_stops(self):
        to_remove = []
        for (side, entry_price), data in self.grid_manager.trailing_stops.items():
            if side != "BUY" or not isinstance(data, list) or len(data) < 2:
                continue
            highest_price, quantity = data
            entry_price = entry_price if entry_price is not None and entry_price > 0 else 0.0
            highest_price = highest_price if highest_price is not None and highest_price > 0 else entry_price
            quantity = quantity if quantity is not None and quantity > 0 else 0.0
            if quantity == 0.0:
                to_remove.append((side, entry_price))
                continue

            if self.current_price > highest_price:
                self.grid_manager.trailing_stops[(side, entry_price)][0] = self.current_price
                highest_price = self.current_price

            atr = calculate_atr(list(self.grid_manager.high_history), list(self.grid_manager.low_history), list(self.grid_manager.price_history), config.atr_period)
            if atr is None or atr == 0.0:
                continue

            target_price = entry_price + atr * 3
            if highest_price >= target_price:
                stop_price = highest_price - atr * 2
                if self.current_price <= stop_price and self.current_price >= entry_price:
                    sell_price = self.current_price
                    base_asset = self.symbol.replace("USDT", "")
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
                        await self.notifier.send_alert(f"Chốt lời: Bán {quantity:.6f} {base_asset} @ {sell_price:.2f}, lợi nhuận {profit:.2f}")
                        self.ml_labels.append(1 if profit > 0 else -1)
                    to_remove.append((side, entry_price))
        for key in to_remove:
            self.grid_manager.trailing_stops.pop(key, None)

# ======== Protection Checks ======== 
async def check_protections(state: 'BotState', exchange: 'IExchange', price: float, volume: float, high: float, low: float) -> bool:
    if price is None or volume is None or high is None or low is None:
        logging.error("Dữ liệu thị trường không hợp lệ (None), bỏ qua kiểm tra bảo vệ")
        await state.notifier.send_alert("Lỗi nghiêm trọng: Dữ liệu thị trường không hợp lệ", is_error=True)
        return True
    if await state.protection.check_circuit_breaker(price):
        return True
    if state.protection.check_pump_protection(price):
        logging.warning("Bảo vệ pump được kích hoạt")
        await state.notifier.send_alert("Pump protection: Biến động giá quá lớn, tạm dừng giao dịch", is_error=True)
        return True
    if state.protection.check_abnormal_activity(volume):
        logging.warning("Phát hiện khối lượng bất thường")
        await state.notifier.send_alert("Khối lượng giao dịch bất thường, tạm dừng giao dịch", is_error=True)
        return True
    return False

# ======== Exchange Interface ======== 
class IExchange(ABC):
    @abstractmethod
    async def get_price(self) -> float:
        pass

    @abstractmethod
    async def get_volume(self) -> float:
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
    async def health_check(self) -> Dict[str, Any]:
        pass

class PaperTradingExchange(IExchange):
    def __init__(self, symbol: str):
        self.exchange_name = "paper_trading"
        self.symbol = symbol
        base_asset = symbol.replace("USDT", "")
        self.simulated_balance = {"USDT": config.initial_investment, base_asset: 0.0}
        logging.info(f"Khởi tạo số dư cho {symbol}: {self.simulated_balance}")
        self.paper_orders = []
        self.current_market_price = 0.1
        self.current_volume = 0.0
        self.current_high = 0.105
        self.current_low = 0.095
        self.order_book = {"bids": [], "asks": []}
        self.last_api_update = 0.0
        self.api_update_interval = 10
        self.websocket_connected = False

    async def get_price(self) -> float:
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            price = float(ticker.get("last", ticker.get("close", 0.0)))
            if price <= 0:
                raise ValueError("Không lấy được giá hợp lệ từ API")
            self.current_market_price = price
            self.current_volume = float(ticker.get("quoteVolume", ticker.get("baseVolume", 0.0)))
            self.current_high = float(ticker.get("high", ticker.get("highPrice", 0.0)))
            self.current_low = float(ticker.get("low", ticker.get("lowPrice", 0.0)))
            self.last_api_update = time.time()
            await self._update_order_book(price)
            return price
        except Exception as e:
            logging.error(f"Lỗi lấy giá: {str(e)}")
            return self.current_market_price
        finally:
            await mexc.close()

    async def get_volume(self) -> float:
        await self.get_price()
        return self.current_volume

    async def get_high_low(self) -> Tuple[float, float]:
        await self.get_price()
        return self.current_high, self.current_low

    async def _update_order_book(self, price: float):
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol.replace("USDT", "/USDT")
            order_book = await mexc.fetch_order_book(symbol_str, limit=10)
            self.order_book = {
                "bids": [[float(b[0]), float(b[1])] for b in order_book.get("bids", [])],
                "asks": [[float(a[0]), float(a[1])] for a in order_book.get("asks", [])],
            }
        except Exception as e:
            logging.error(f"Lỗi cập nhật order book: {str(e)}")
        finally:
            await mexc.close()

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        if order_type.upper() != "LIMIT" or price is None:
            raise ValueError("Chỉ hỗ trợ LIMIT order trong paper trading")
        filters = await self.get_symbol_filters()
        tick_size = filters.get('tickSize', 0.00001)
        price = round(price / tick_size) * tick_size
        if price < filters['minPrice'] or price > filters['maxPrice']:
            logging.warning(f"Giá {price} ngoài khoảng cho phép [{filters['minPrice']}, {filters['maxPrice']}]")
            return {}
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
        current_price = await self.get_price()
        await self._update_order_book(current_price)
        base_asset = self.symbol.replace("USDT", "")
        if base_asset not in self.simulated_balance:
            self.simulated_balance[base_asset] = 0.0
        for order in self.paper_orders:
            if order["status"] != "OPEN":
                continue
            order_price = float(order["price"])
            order_qty = float(order["quantity"])
            if order["side"] == "BUY" and current_price <= order_price:
                cost = order_qty * order_price
                if self.simulated_balance["USDT"] >= cost:
                    self.simulated_balance["USDT"] -= cost
                    self.simulated_balance[base_asset] += order_qty
                    order["status"] = "FILLED"
                    order["executedQty"] = order_qty
                    logging.info(f"Khớp lệnh MUA tại {order_price:.2f}, SL: {order_qty:.6f}, Balance: {self.simulated_balance}")
                    await DiscordNotifier().send_alert(f"Khớp lệnh MUA tại {order_price:.2f}, SL: {order_qty:.6f}")
                else:
                    logging.warning(f"Hủy lệnh MUA: USDT không đủ {self.simulated_balance['USDT']} < {cost}")
                    order["status"] = "CANCELED"
            elif order["side"] == "SELL" and current_price >= order_price:
                if self.simulated_balance[base_asset] >= order_qty:
                    proceeds = order_qty * order_price
                    self.simulated_balance[base_asset] -= order_qty
                    self.simulated_balance["USDT"] += proceeds
                    order["status"] = "FILLED"
                    order["executedQty"] = order_qty
                    logging.info(f"Khớp lệnh BÁN tại {order_price:.2f}, SL: {order_qty:.6f}, Balance: {self.simulated_balance}")
                    await DiscordNotifier().send_alert(f"Khớp lệnh BÁN tại {order_price:.2f}, SL: {order_qty:.6f}")
                else:
                    logging.warning(f"Hủy lệnh BÁN: {base_asset} không đủ {self.simulated_balance[base_asset]} < {order_qty}")
                    order["status"] = "CANCELED"

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
                logging.info(f"Đã hủy lệnh {order_id}")
                break

    async def get_balance(self) -> Tuple[float, float]:
        base_asset = self.symbol.replace("USDT", "")
        if base_asset not in self.simulated_balance:
            self.simulated_balance[base_asset] = 0.0
        usdt = self.simulated_balance["USDT"]
        coin = self.simulated_balance[base_asset]
        logging.info(f"Lấy số dư: {usdt} USDT, {coin} {base_asset}")
        return usdt, coin

    async def get_order_book(self) -> Dict:
        await self.get_price()
        return self.order_book

    async def get_symbol_filters(self) -> Dict:
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol.replace("USDT", "/USDT")
            market = mexc.market(symbol_str)
            limits = market.get('limits', {})
            amount_limits = limits.get('amount', {})
            price_limits = limits.get('price', {})

            filters = {
                "minQty": float(amount_limits.get('min', 1.0) or 1.0),
                "maxQty": float(amount_limits.get('max', 10000.0) or 10000.0),
                "stepSize": market.get('precision', {}).get('amount', 0.00001),
                "minPrice": float(price_limits.get('min', 0.00001) or 0.00001),
                "maxPrice": float(price_limits.get('max', 10000.0) or 10000.0),
                "tickSize": market.get('precision', {}).get('price', 0.00001)
            }
            # Đảm bảo không có giá trị None
            for key, value in filters.items():
                if value is None:
                    logging.warning(f"Giá trị {key} là None, gán giá trị mặc định")
                    if key == "minQty":
                        filters[key] = 1.0
                    elif key == "maxQty":
                        filters[key] = 10000.0
                    elif key == "stepSize":
                        filters[key] = 0.00001
                    elif key == "minPrice":
                        filters[key] = 0.00001
                    elif key == "maxPrice":
                        filters[key] = 10000.0
                    elif key == "tickSize":
                        filters[key] = 0.00001
            return filters
        except Exception as e:
            logging.error(f"Lỗi lấy symbol filters: {str(e)}")
            return {
                "minQty": 1.0,
                "maxQty": 10000.0,
                "stepSize": 0.00001,
                "minPrice": 0.00001,
                "maxPrice": 10000.0,
                "tickSize": 0.00001
            }
        finally:
            await mexc.close()

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

    async def run_paper_trading_live(self, state: 'BotState', interval: float = 1.0):
        last_ping_time = time.time()
        while True:
            try:
                await state.process_events()
                if time.time() - last_ping_time >= 600:
                    logging.info("Bot vẫn đang chạy")
                    last_ping_time = time.time()
            except Exception as e:
                logging.error(f"Lỗi trong vòng lặp chính: {str(e)}")
                await state.notifier.send_alert(f"Lỗi nghiêm trọng trong vòng lặp: {str(e)}", is_error=True)
            await asyncio.sleep(interval)

# ======== Safe API Call Utility ======== 
async def safe_api_call(func, *args, retries=3, delay=2, alert_on_failure=False, **kwargs):
    for attempt in range(retries):
        try:
            result = await func(*args, **kwargs)
            if result is None and func.__name__ not in ["cancel_order"]:
                logging.warning(f"{func.__name__} trả về None, thử lại")
                raise ValueError("Kết quả None")
            return result
        except Exception as e:
            logging.warning(f"Lỗi API lần {attempt+1}/{retries} trong {func.__name__}: {str(e)}")
            if attempt < retries - 1:
                await asyncio.sleep(delay * (2 ** attempt))
            else:
                logging.error(f"{func.__name__} thất bại sau {retries} lần")
                if alert_on_failure:
                    await DiscordNotifier().send_alert(f"Lỗi API nghiêm trọng: {func.__name__} thất bại sau {retries} lần - {str(e)}", is_error=True)
                if func.__name__ in ["get_open_orders", "get_all_orders"]:
                    return []
                elif func.__name__ == "get_balance":
                    return (0.0, 0.0)
                elif func.__name__ == "get_symbol_filters":
                    return {
                        "minQty": 1.0,
                        "maxQty": 10000.0,
                        "stepSize": 0.00001,
                        "minPrice": 0.00001,
                        "maxPrice": 10000.0,
                        "tickSize": 0.00001
                    }
                return None

# ======== Rebalance Loop ======== 
async def rebalance_loop(state: 'BotState'):
    while True:
        try:
            await state.grid_manager.rebalance_if_needed(state.current_price, state.exchange)
        except Exception as e:
            logging.error(f"Lỗi rebalance: {str(e)}")
        await asyncio.sleep(10)

# ======== Periodic Tasks ======== 
async def periodic_save_state(states):
    while True:
        save_state(states)
        await asyncio.sleep(300)

# ======== Main Execution ======== 
if __name__ == "__main__":
    async def main():
        config.paper_trading_enabled = True
        symbols = ["DOGEUSDT", "BTCUSDT"]
        states = {}
        for symbol in symbols:
            exchange = PaperTradingExchange(symbol)
            exchange.simulated_balance["USDT"] = config.initial_investment
            exchange.simulated_balance[symbol.replace("USDT", "")] = 0.0
            logging.info(f"Reset số dư khi khởi động cho {symbol}: {exchange.simulated_balance}")
            grid_manager = EnhancedSmartGridManager()
            strategy = AdaptiveGridStrategy(grid_manager) if config.strategy_mode == 1 else DenseGridStrategy(grid_manager)
            state = BotState(exchange, strategy, symbol)
            states[symbol] = state

        if config.backtest_enabled:
            logging.info("Bắt đầu chạy chế độ Backtest")
            for symbol, state in states.items():
                exchange = state.exchange
                df = await fetch_historical_data(exchange, config.backtest_timeframe, config.backtest_limit)
                for idx, data in df.iterrows():
                    price = float(data['close'])
                    high = float(data['high'])
                    low = float(data['low'])
                    volume = float(data['volume'])
                    state.current_price = price
                    state.current_high = high
                    state.current_low = low
                    state.current_volume = volume
                    state.protection.update(price, high, low, volume)
                    if await check_protections(state, exchange, price, volume, high, low):
                        continue
                    if config.strategy_mode != 2:
                        await state.grid_manager.place_static_orders(price, exchange)
                    if config.strategy_mode == 2:
                        state.grid_manager.high_history.append(high)
                        state.grid_manager.low_history.append(low)
                    await state.grid_manager.calculate_adaptive_grid(price, exchange)
                    await state.grid_manager.place_grid_orders(price, exchange)
                    signals = await state.strategy.generate_signals(MarketData(price=price, high=high, low=low, volume=volume))
                    for signal in signals:
                        order = await exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
                        if order and 'orderId' in order:
                            state.grid_manager.order_ids.append(order['orderId'])
                    await state.order_manager.check_and_handle_orders(price, exchange, state.tracker, state.grid_manager)
                    if config.trailing_stop_enabled:
                        await state.handle_trailing_stops()
                    await state.grid_manager.rebalance_if_needed(price, exchange)
                stats = state.tracker.get_stats()
                logging.info(f"Backtest hoàn tất cho {symbol}: {stats}")
                plt.plot(state.tracker.equity_curve)
                plt.title(f"Equity Curve - {symbol}")
                plt.xlabel("Trade")
                plt.ylabel("Profit (USDT)")
                plt.savefig(f'equity_curve_{symbol}.png')
            return

        for symbol, state in states.items():
            logging.info(f"Hủy tất cả lệnh mở trước khi khởi động bot cho {symbol}")
            await state.order_manager.cancel_all_orders(state.exchange)
            await state.grid_manager.cancel_all_orders(state.exchange, preserve_static=False)

        load_state(states)
        tasks = [state.exchange.run_paper_trading_live(state, interval=1) for state in states.values()]
        tasks.extend([rebalance_loop(state) for state in states.values()])
        tasks.append(periodic_save_state(states))
        await asyncio.gather(*tasks)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass