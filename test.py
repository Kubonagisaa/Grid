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
from sklearn.linear_model import LogisticRegression
import talib as ta

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

STATE_FILE = "bot_state.json"
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
        # Thay đổi ở đây: Chuẩn hóa symbol thành chữ in hoa
        self.symbol: str = os.getenv("TRADING_SYMBOL", "BTCUSDT").upper()
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
        self.websocket_enabled: bool = os.getenv("WEBSOCKET_ENABLED", "True").lower() == "true"
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
        self.min_profit_percent: float = float(os.getenv("MIN_PROFIT_PERCENT", 0.0))
        self.stop_loss_capital_percent: float = float(os.getenv("STOP_LOSS_CAPITAL_PERCENT", 30.0))
        self.backtest_enabled: bool = os.getenv("BACKTEST_ENABLED", "False").lower() == "true"
        self.backtest_timeframe: str = os.getenv("BACKTEST_TIMEFRAME", "1h")
        self.backtest_limit: int = int(os.getenv("BACKTEST_LIMIT", 1000))
        self.telegram_report_interval: int = int(os.getenv("TELEGRAM_REPORT_INTERVAL", 3600))
        self.telegram_volatile_interval: int = int(os.getenv("TELEGRAM_VOLATILE_INTERVAL", 600))
        self.telegram_volatility_threshold: float = float(os.getenv("TELEGRAM_VOLATILITY_THRESHOLD", 1.0))
        if self.strategy_mode == 1:
            self.base_grid_step_percent = self.wide_grid_step_percent_min
        elif self.strategy_mode == 2:
            self.base_grid_step_percent = self.dense_grid_step_percent_min
        if "DOGE" in self.symbol:
            self.trailing_up_activation = float(os.getenv("TRAILING_UP_ACTIVATION_DOGE", 0.0))
            self.trailing_down_activation = float(os.getenv("TRAILING_DOWN_ACTIVATION_DOGE", 0.0))

config = AdvancedConfig()

logging.info(f"Đã chọn chiến lược {config.strategy_mode}: {'Grid thưa (bước ~2-3%, có RSI)' if config.strategy_mode == 1 else 'Grid dày (bước ~0.5%, không dùng RSI)'}")
logging.info(f"Bước lưới cơ bản: {config.base_grid_step_percent:.2f}%, Mức grid tối thiểu: {config.min_grid_levels}, tối đa: {config.max_grid_levels}")

# ======== HÀM TIỆN ÍCH ========
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
    if len(prices) < period:
        return 0.0
    return np.mean(prices[-period:])

def calculate_bollinger_bands(prices: List[float], period: int, std_dev: float) -> Tuple[float, float]:
    if len(prices) < period:
        return 0.0, 0.0
    sma = np.mean(prices[-period:])
    std = np.std(prices[-period:])
    upper_band = sma + std_dev * std
    lower_band = sma - std_dev * std
    return lower_band, upper_band

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        dd = (peak - value) / peak * 100
        max_dd = max(max_dd, dd)
    return max_dd

def auto_adjust_rsi_thresholds(prices: List[float], default_low=30, default_high=70) -> Tuple[float, float]:
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
    risk_per_trade = 0.02  # 2% vốn/lệnh
    atr = current_volatility or 0.001
    position_size = (config.initial_investment * risk_per_trade) / (atr * 2)
    return round(position_size, 4)

def adaptive_take_profit(prices: List[float]) -> float:
    rsi = calculate_rsi(prices, config.rsi_period)
    if rsi > 70:
        return 1.5  # Chốt lời nhanh khi quá mua
    elif rsi < 30:
        return 3.0  # Để lợi nhuận chạy khi quá bán
    return 2.0

def adjust_risk_by_volatility(profit_curve: List[float]) -> None:
    if len(profit_curve) < 50:
        return
    std_dev = np.std(profit_curve[-50:])  # Độ biến động lợi nhuận 50 lệnh gần nhất
    if std_dev > 5:
        config.min_quantity *= 0.5
        logging.info(f"Giảm rủi ro: min_quantity = {config.min_quantity}")
    elif std_dev < 2:
        config.min_quantity *= 1.2
        logging.info(f"Tăng rủi ro: min_quantity = {config.min_quantity}")

def optimize_parameters_based_on_market(volatility: float, trend_strength: float) -> None:
    if volatility > 0.05:
        config.base_grid_step_percent = 2.5
    else:
        config.base_grid_step_percent = 1.5
    if trend_strength > 25:
        config.min_profit_percent = 3
    else:
        config.min_profit_percent = 1.5
    logging.info(f"Tối ưu tham số: grid_step={config.base_grid_step_percent}%, min_profit={config.min_profit_percent}%")

async def fetch_higher_timeframe_data(timeframe: str = '4h') -> pd.DataFrame:
    exchange = ccxt.binance()
    ohlcv = exchange.fetch_ohlcv(config.symbol, timeframe)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    return df

async def monitor_system_health() -> None:
    cpu = psutil.cpu_percent()
    memory = psutil.virtual_memory().percent
    if cpu > 85 or memory > 80:
        await send_telegram_alert(f"⚠️ Cảnh báo: CPU {cpu}% | RAM {memory}%")

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

class EnhancedProtectionSystem:
    def __init__(self):
        self.price_history: deque = deque(maxlen=config.volatility_window)
        self.high_history: deque = deque(maxlen=config.volatility_window)
        self.low_history: deque = deque(maxlen=config.volatility_window)
        self.volume_history: deque = deque(maxlen=config.volatility_window)
        self.trailing_stop_price: float = 0.0
        self.trailing_buy_price: float = 0.0
        self.circuit_breaker_triggered: bool = False
        self.circuit_breaker_start: Optional[float] = None

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
            await send_telegram_alert("Circuit breaker: biên độ giá quá lớn, tạm dừng giao dịch")
            return True
        return False

    async def check_circuit_breaker_status(self) -> bool:
        if self.circuit_breaker_triggered and self.circuit_breaker_start:
            elapsed = time.time() - self.circuit_breaker_start
            if elapsed > config.circuit_breaker_duration:
                self.circuit_breaker_triggered = False
                self.circuit_breaker_start = None
                logging.info("Ngắt mạch đã được đặt lại")
                await send_telegram_alert("Circuit breaker đã được đặt lại")
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
                                      profit_tracker: 'EnhancedProfitTracker', 
                                      grid_manager: 'EnhancedSmartGridManager') -> None:
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
                        f"Giá: {price:.2f}, SL: {qty:.6f}\n"
                        f"Lợi nhuận: {profit:.2f} USDT\n"
                        f"Phí: {fee:.4f} USDT"
                    )
                    logging.info(f"[PAPER] Lệnh {side.upper()} đã khớp: Giá {price:.2f}, SL {qty:.6f}, Lợi nhuận {profit:.2f} USDT")
                    await send_telegram_alert(msg)
                    self.completed_orders.append({"side": side.upper(), "price": price, "quantity": qty})
                elif order['status'] == 'CANCELED':
                    if order.get('side', '').upper() == 'BUY' and order.get('orderId') in grid_manager.order_ids:
                        grid_manager.order_ids.remove(order['orderId'])
        except Exception as e:
            logging.error(f"Lỗi khi xử lý lệnh: {str(e)}")

class EnhancedProfitTracker:
    def __init__(self):
        self.total_profit: float = 0.0
        self.trade_count: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0
        self.total_fees: float = 0.0
        self.equity_curve: List[float] = []

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
            logging.info("Không tìm thấy file trạng thái, thử tải bản sao lưu")
            for i in range(1, 4):
                backup_file = f"bot_state_backup{i}.json"
                if os.path.exists(backup_file):
                    with open(backup_file, 'r') as f:
                        state_data = json.load(f)
                    break
            else:
                logging.info("Không tìm thấy bản sao lưu, khởi tạo trạng thái mới")
                return
        else:
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
                state.grid_manager.trailing_stops = data.get("trailing_stops", {})
                state.order_manager.open_orders = data.get("open_orders", [])
    except Exception as e:
        logging.error(f"Lỗi khi load trạng thái bot: {str(e)}")

class EnhancedSmartGridManager:
    def __init__(self):
        self.grid_levels: List[Tuple[float, float]] = []
        self.static_levels: int = 3
        self.static_orders_placed: bool = False
        self.static_order_ids: List[str] = []
        self.order_ids: List[str] = []
        self.trailing_stops: Dict[Tuple[str, float], List[float]] = {}
        self.grid_cooldown: Dict[Tuple[str, float], float] = {}
        self.price_precision: int = 2
        self.FALLBACK_COOLDOWN: int = 300
        self.price_history: deque = deque(maxlen=config.volatility_window)
        self.high_history: deque = deque(maxlen=config.volatility_window)
        self.low_history: deque = deque(maxlen=config.volatility_window)
        self.last_static_order_time: float = 0.0
        self.last_fallback_time: float = 0.0

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
            usdt_balance, _ = await safe_api_call(exchange.get_balance)
            allocated_funds_per_order = (usdt_balance * (1 - config.capital_reserve_percent / 100)) / (2 * num_levels)
            quantity_per_level = allocated_funds_per_order / current_price if current_price > 0 else 0.0
            quantity = max(filters['minQty'], dynamic_position_sizing(calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)))
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
                    profit_percent = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                    if profit_percent < config.min_profit_percent:
                        continue
                    if usdt_balance < buy_price * quantity:
                        logging.warning("USDT không đủ để đặt lệnh MUA tĩnh, dừng đặt lệnh tiếp theo")
                        break
                    if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                        buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price)
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
            usdt_balance, coin_balance = await safe_api_call(exchange.get_balance)
            allocated_funds_per_order = (usdt_balance * (1 - config.capital_reserve_percent / 100)) / (2 * num_levels)
            quantity_per_level = allocated_funds_per_order / current_price if current_price > 0 else 0.0
            quantity = max(filters['minQty'], dynamic_position_sizing(calculate_atr(list(self.high_history), list(self.low_history), list(self.price_history), config.atr_period)))
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
                usdt_balance, coin_balance = await safe_api_call(exchange.get_balance)
                profit_percent = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                if profit_percent < config.min_profit_percent:
                    continue
                try:
                    if buy_price <= 0 or sell_price <= 0:
                        continue
                    if buy_price < current_price:
                        price_key = ("BUY", round(buy_price, self.price_precision))
                        if current_time < self.grid_cooldown.get(price_key, 0):
                            continue
                        if usdt_balance < buy_price * quantity:
                            logging.warning("USDT không đủ để đặt lệnh MUA, dừng vòng lặp đặt lệnh")
                            break
                        if not self.duplicate_order_exists("BUY", buy_price, open_orders):
                            buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price)
                            if 'orderId' not in buy_order:
                                continue
                            self.order_ids.append(buy_order['orderId'])
                            open_count += 1
                            logging.info(f"(Dynamic) Đã đặt lệnh MUA tại {buy_price:.2f}, SL: {quantity:.6f}")
                    if sell_price > current_price:
                        price_key = ("SELL", round(sell_price, self.price_precision))
                        if current_time < self.grid_cooldown.get(price_key, 0):
                            continue
                        if open_count >= config.max_open_orders:
                            logging.warning(f"Đạt giới hạn lệnh tối đa ({open_count}/{config.max_open_orders})")
                            break
                        if coin_balance < quantity:
                            logging.warning("Số dư coin không đủ để đặt lệnh BÁN")
                            continue
                        sell_order = await safe_api_call(exchange.place_order, side='SELL', order_type='LIMIT', quantity=quantity, price=sell_price)
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
                buy_price = current_price * (1 - config.base_grid_step_percent / 100)
                if buy_price > 0 and not self.duplicate_order_exists("BUY", buy_price, open_orders):
                    buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price)
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
        return min(lows), max(highs)

    async def rebalance_if_needed(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        pass  # Logic rebalance có thể được thêm sau nếu cần

class GridStrategy(ABC):
    def __init__(self, grid_manager: EnhancedSmartGridManager):
        self.grid_manager = grid_manager

    @abstractmethod
    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        pass

class AdaptiveGridStrategy(GridStrategy):
    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        signals = []
        prices = list(self.grid_manager.price_history)
        if len(prices) < max(config.rsi_period, config.ma_short_period, config.ma_long_period, config.bb_period):
            return signals

        # RSI
        rsi = calculate_rsi(prices, config.rsi_period)
        rsi_low, rsi_high = auto_adjust_rsi_thresholds(prices)
        if rsi < rsi_low:
            signals.append(TradeSignal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))
        elif rsi > rsi_high:
            signals.append(TradeSignal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))

        # Bollinger Bands
        lower_band, upper_band = calculate_bollinger_bands(prices, config.bb_period, config.bb_std_dev)
        if data.price <= lower_band:
            signals.append(TradeSignal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))
        elif data.price >= upper_band:
            signals.append(TradeSignal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))

        # Moving Average
        ma_short = calculate_moving_average(prices, config.ma_short_period)
        ma_long = calculate_moving_average(prices, config.ma_long_period)
        if ma_short > ma_long and data.price > ma_short:
            signals.append(TradeSignal(side="SELL", price=data.price * 1.001, quantity=config.min_quantity))
        elif ma_short < ma_long and data.price < ma_short:
            signals.append(TradeSignal(side="BUY", price=data.price * 0.999, quantity=config.min_quantity))

        return signals

class DenseGridStrategy(GridStrategy):
    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        return []

class BreakoutStrategy(GridStrategy):
    async def generate_signals(self, data: MarketData) -> List[TradeSignal]:
        signals = []
        df = await fetch_higher_timeframe_data()
        key_resistance = df['high'].rolling(50).max().iloc[-1]
        key_support = df['low'].rolling(50).min().iloc[-1]
        if data.price > key_resistance:
            signals.append(TradeSignal(side="BUY", price=data.price * 1.001, quantity=config.min_quantity))
        elif data.price < key_support:
            signals.append(TradeSignal(side="SELL", price=data.price * 0.999, quantity=config.min_quantity))
        return signals

class TrendDetector:
    def __init__(self, state: 'BotState'):
        self.state = state

    async def detect_trend_strength(self) -> float:
        highs = list(self.state.protection.high_history)
        lows = list(self.state.protection.low_history)
        closes = list(self.state.grid_manager.price_history)
        if len(highs) < 14 or len(lows) < 14 or len(closes) < 14:
            return 0.0
        adx = ta.ADX(np.array(highs), np.array(lows), np.array(closes), timeperiod=14)
        trend_strength = adx[-1] if len(adx) > 0 else 0.0
        if trend_strength > 25:
            await self._activate_anti_trend_mode()
        return trend_strength

    async def _activate_anti_trend_mode(self):
        config.adaptive_grid_enabled = False
        await self.state.grid_manager.cancel_all_orders(self.state.exchange)
        self.state.strategy = BreakoutStrategy(self.state.grid_manager)
        logging.info("Chuyển sang chiến lược Breakout do xu hướng mạnh")

class NewsAnalyzer:
    def __init__(self):
        self.news_api_key = os.getenv("NEWS_API_KEY", "your_news_api_key")
        self.sentiment = 0.0

    async def update_sentiment(self) -> None:
        url = f"https://newsapi.org/v2/everything?q=crypto&apiKey={self.news_api_key}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    articles = await response.json()
                    self.sentiment = self._analyze_sentiment(articles)
        except Exception as e:
            logging.error(f"Lỗi khi lấy tin tức: {str(e)}")

    def _analyze_sentiment(self, articles: Dict) -> float:
        # Giả lập phân tích cảm xúc đơn giản
        if 'articles' not in articles:
            return 0.0
        positive = sum(1 for a in articles['articles'] if "good" in a.get('title', '').lower())
        negative = sum(1 for a in articles['articles'] if "bad" in a.get('title', '').lower())
        total = positive + negative
        return (positive - negative) / total if total > 0 else 0.0

class BlackSwanProtection:
    def __init__(self, state: 'BotState'):
        self.state = state

    async def monitor(self) -> None:
        liquidity_ratio = await self._calculate_liquidity()
        if liquidity_ratio < 0.5:
            await self._emergency_liquidation()

    async def _calculate_liquidity(self) -> float:
        try:
            order_book = await self.state.exchange.get_order_book()
            bid_liquidity = sum([bid[1] for bid in order_book['bids'][:5]])
            ask_liquidity = sum([ask[1] for ask in order_book['asks'][:5]])
            return min(bid_liquidity, ask_liquidity) / max(bid_liquidity, ask_liquidity)
        except Exception as e:
            logging.error(f"Lỗi tính thanh khoản: {str(e)}")
            return 1.0

    async def _emergency_liquidation(self) -> None:
        await self.state.order_manager.cancel_all_orders(self.state.exchange)
        usdt_balance, coin_balance = await self.state.exchange.get_balance()
        if coin_balance > 0:
            await self.state.exchange.place_order("SELL", "MARKET", coin_balance)
        logging.warning("Thực hiện thanh lý khẩn cấp do sự kiện Black Swan")
        await send_telegram_alert("Thanh lý khẩn cấp: Phát hiện sự kiện Black Swan")

class SimpleMLPredictor:
    def __init__(self):
        self.model = LogisticRegression()
        self.trained = False

    def train(self, features: np.ndarray, labels: np.ndarray) -> None:
        if len(features) > 0 and len(labels) > 0:
            self.model.fit(features, labels)
            self.trained = True

    def predict(self, features: np.ndarray) -> int:
        if not self.trained:
            return 0
        return self.model.predict([features])[0]

class BotState:
    def __init__(self, exchange: 'ExchangeInterface', strategy: GridStrategy):
        self.exchange = exchange
        self.exchange_name = exchange.exchange_name
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

    async def process_events(self) -> None:
        start_time = time.time()
        current_time = time.time()

        # Cập nhật dữ liệu thị trường
        if current_time - self.last_price_update >= 5:
            if not await check_internet_connection():
                logging.error("Không có kết nối Internet, sử dụng giá trị gần nhất")
                await send_telegram_alert("Không có kết nối Internet, bot tạm dừng lấy dữ liệu mới")
            else:
                tasks = [
                    self.exchange.get_price(),
                    self.exchange.get_volume(),
                    self.exchange.get_high_low()
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logging.error(f"Lỗi khi lấy dữ liệu: {str(result)}")
                if all(not isinstance(r, Exception) for r in results):
                    new_price, new_volume, new_high_low = results
                    if new_price is not None and new_price > 0:
                        self.current_price = new_price
                        self.current_volume = new_volume
                        self.current_high, self.current_low = new_high_low
                        self.last_price_update = current_time
                        logging.info("Lấy dữ liệu thị trường thành công")

        self.protection.update(self.current_price, self.current_high, self.current_low, self.current_volume)
        if await check_protections(self, self.exchange, self.current_price, self.current_volume, self.current_high, self.current_low):
            return

        health = await self.exchange.health_check()
        if health['status'] != 'healthy':
            logging.warning(f"Sàn không ổn định: {health['status']}")
            await send_telegram_alert(f"Sàn không ổn định: {health['status']}")
            return

        # Thêm các bước mới
        trend_strength = await self.trend_detector.detect_trend_strength()
        await self.news_analyzer.update_sentiment()
        await self.black_swan_protection.monitor()
        await monitor_system_health()

        # Tối ưu tham số
        atr = calculate_atr(list(self.protection.high_history), list(self.protection.low_history), list(self.grid_manager.price_history), config.atr_period)
        volatility = atr / self.current_price if self.current_price > 0 else 0.001
        optimize_parameters_based_on_market(volatility, trend_strength)

        # Điều chỉnh rủi ro
        adjust_risk_by_volatility(self.tracker.equity_curve)

        # ML dự đoán
        features = [
            calculate_rsi(list(self.grid_manager.price_history), config.rsi_period),
            calculate_moving_average(list(self.grid_manager.price_history), config.ma_short_period),
            calculate_moving_average(list(self.grid_manager.price_history), config.ma_long_period),
        ]
        if len(self.ml_features) > 50:
            self.ml_predictor.train(np.array(self.ml_features[-50:]), np.array(self.ml_labels[-50:]))
        if self.ml_predictor.trained:
            prediction = self.ml_predictor.predict(features)
            if prediction == 1:
                logging.info("ML dự đoán: MUA")
            elif prediction == -1:
                logging.info("ML dự đoán: BÁN")

        # Kiểm tra vốn trước giao dịch
        usdt_balance, coin_balance = await safe_api_call(self.exchange.get_balance)
        capital = usdt_balance + coin_balance * self.current_price
        initial_capital = config.initial_investment
        if capital < initial_capital * 0.9:  # Lỗ 10%
            message = f"Cảnh báo: Vốn giảm 10%, tạm dừng giao dịch\nVốn hiện tại: {capital:.2f} USDT"
            logging.error(message)
            await send_telegram_alert(message)
            await send_email_alert("Bot Alert: Vốn giảm 10%", message)
            await send_discord_alert(message)
            return
        if capital < initial_capital * 0.7:  # Lỗ 30%
            message = f"Cảnh báo: Vốn giảm mạnh xuống còn {capital:.2f} USDT, dừng bot"
            logging.error(f"Vốn giảm dưới 30% ban đầu, dừng bot")
            await send_telegram_alert(message)
            await send_email_alert("Bot Alert: Vốn giảm mạnh", message)
            await send_discord_alert(message)
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
                try:
                    order = await self.exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
                    if 'orderId' not in order:
                        logging.error(f"Lệnh không trả về orderId: {order}")
                        continue
                    self.grid_manager.order_ids.append(order['orderId'])
                    # Ghi lại dữ liệu ML
                    self.ml_features.append(features)
                    self.ml_labels.append(1 if signal.side == "BUY" else -1)
                except Exception as e:
                    logging.error(f"Lỗi khi đặt lệnh: {signal.side} tại {signal.price}: {str(e)}")
        await self.order_manager.check_and_handle_orders(self.current_price, self.exchange, self.tracker, self.grid_manager)
        if config.trailing_stop_enabled:
            await self.handle_trailing_stops()
        await self.grid_manager.rebalance_if_needed(self.current_price, self.exchange)
        logging.info(f"process_events chạy mất {time.time() - start_time:.2f}s")

        # Kiểm tra vốn sau giao dịch
        usdt_balance, coin_balance = await safe_api_call(self.exchange.get_balance)
        capital = usdt_balance + coin_balance * self.current_price
        if capital < initial_capital * 0.9:
            message = f"Cảnh báo: Vốn giảm 10%, tạm dừng giao dịch\nVốn hiện tại: {capital:.2f} USDT"
            logging.error(message)
            await send_telegram_alert(message)
            await send_email_alert("Bot Alert: Vốn giảm 10%", message)
            await send_discord_alert(message)
            return
        if capital < initial_capital * 0.7:
            message = f"Cảnh báo: Vốn giảm mạnh xuống còn {capital:.2f} USDT, dừng bot"
            logging.error(f"Vốn giảm dưới 30% ban đầu, dừng bot")
            await send_telegram_alert(message)
            await send_email_alert("Bot Alert: Vốn giảm mạnh", message)
            await send_discord_alert(message)
            sys.exit()

        if config.telegram_enabled:
            interval = config.telegram_report_interval
            if len(self.protection.price_history) > 1:
                first_price = self.protection.price_history[0]
                price_change = abs(self.current_price - first_price) / first_price * 100
                if price_change >= config.telegram_volatility_threshold:
                    interval = config.telegram_volatile_interval
            if current_time - self.last_report_time >= interval:
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
            target_price = entry_price * (1 + adaptive_take_profit(list(self.grid_manager.price_history)) / 100)
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
            f"📊 Báo cáo sau {int((time.time()-self.last_report_time)/60)} phút:\n"
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
        logging.warning("Bảo vệ pump được kích hoạt")
        await send_telegram_alert("Pump protection: biến động giá quá lớn, tạm dừng giao dịch")
        return True
    if state.protection.check_abnormal_activity(volume):
        logging.warning("Phát hiện khối lượng bất thường")
        await send_telegram_alert("Khối lượng giao dịch bất thường, tạm dừng giao dịch")
        return True
    return False

last_telegram_message_time = 0
TELEGRAM_RATE_LIMIT_SECONDS = 2

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
            payload["reply_markup"] = json.dumps(reply_markup)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload) as response:
                if response.status == 200:
                    last_telegram_message_time = time.time()
                else:
                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status, message=await response.text())
    except Exception as e:
        logging.error(f"Lỗi gửi thông báo Telegram: {str(e)}")

async def send_email_alert(subject: str, message: str):
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = "your_email@example.com"
    msg['To'] = "recipient@example.com"
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login("your_email@example.com", "your_password")
            server.sendmail("your_email@example.com", "recipient@example.com", msg.as_string())
    except Exception as e:
        logging.error(f"Lỗi gửi email: {str(e)}")

async def send_discord_alert(message: str):
    webhook_url = "your_discord_webhook_url"
    payload = {"content": message}
    try:
        response = requests.post(webhook_url, json=payload)
        if response.status_code != 204:
            logging.error(f"Lỗi gửi Discord: {response.text}")
    except Exception as e:
        logging.error(f"Lỗi gửi Discord: {str(e)}")

async def periodic_save_state(states):
    while True:
        save_state(states)
        await asyncio.sleep(300)  # Lưu mỗi 5 phút

class ExchangeInterface(ABC):
    def __init__(self, name: str, api_key: str, api_secret: str):
        self.exchange_name = name
        self.api_key = api_key
        self.api_secret = api_secret
        self.websocket_connected = False

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

    async def start_websocket(self) -> None:
        logging.info(f"WebSocket không được hỗ trợ trong {self.exchange_name}")

class PaperTradingExchange(ExchangeInterface):
    def __init__(self, symbol: str):
        super().__init__("paper_trading", "", "")
        self.symbol = symbol
        base_asset = symbol.replace("USDT", "")
        self.simulated_balance = {"USDT": config.initial_investment, base_asset: 0.0}
        self.paper_orders = []
        self.current_market_price = 100.0
        self.current_volume = 0.0
        self.current_high = 101.0
        self.current_low = 99.0
        self.order_book = {"bids": [], "asks": []}
        self.last_api_update = 0.0
        self.api_update_interval = 10

    async def get_price(self) -> float:
        if config.websocket_enabled:
            return self.current_market_price  # Được cập nhật bởi WebSocket
        current_time = time.time()
        if current_time - self.last_api_update < self.api_update_interval:
            return self.current_market_price
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            ticker = await mexc.fetch_ticker(symbol_str)
            price = float(ticker.get("last", ticker.get("close", 0.0)))
            if price <= 0:
                raise ValueError("Không lấy được giá từ API")
            self.current_market_price = price
            self.current_volume = float(ticker.get("quoteVolume", ticker.get("baseVolume", 0.0)))
            self.current_high = float(ticker.get("high", ticker.get("highPrice", 0.0)))
            self.current_low = float(ticker.get("low", ticker.get("lowPrice", 0.0)))
            self.last_api_update = current_time
            await self._update_order_book(price)
            return price
        finally:
            await mexc.close()

    async def get_volume(self) -> float:
        if config.websocket_enabled:
            return self.current_volume
        await self.get_price()
        return self.current_volume

    async def get_high_low(self) -> Tuple[float, float]:
        if config.websocket_enabled:
            return self.current_high, self.current_low
        await self.get_price()
        return self.current_high, self.current_low

    async def _update_order_book(self, price: float):
        mexc = ccxt_async.mexc()
        try:
            await mexc.load_markets()
            symbol_str = self.symbol if "/" in self.symbol else self.symbol.replace("USDT", "/USDT")
            order_book = await mexc.fetch_order_book(symbol_str, limit=10)
            self.order_book = {
                "bids": [[float(b[0]), float(b[1])] for b in order_book.get("bids", [])],
                "asks": [[float(a[0]), float(a[1])] for a in order_book.get("asks", [])],
            }
        finally:
            await mexc.close()

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

    async def start_websocket(self):
        if not config.websocket_enabled:
            return
        while True:
            try:
                async with websockets.connect("wss://mexc.com/websocket") as websocket:
                    subscribe_msg = {"method": "SUBSCRIPTION", "params": [f"spot@public.ticker@{self.symbol}"]}
                    await websocket.send(json.dumps(subscribe_msg))
                    self.websocket_connected = True
                    while True:
                        message = await websocket.recv()
                        data = json.loads(message)
                        if "ticker" in data:
                            self.current_market_price = float(data["ticker"]["last"])
                            self.current_volume = float(data["ticker"]["volume"])
                            self.current_high = float(data["ticker"]["high"])
                            self.current_low = float(data["ticker"]["low"])
            except Exception as e:
                logging.error(f"Lỗi WebSocket: {str(e)}")
                self.websocket_connected = False
                await asyncio.sleep(5)

    async def run_paper_trading_live(self, state: BotState, interval: float = 1.0):
        last_ping_time = time.time()
        if config.websocket_enabled:
            asyncio.create_task(self.start_websocket())
        while True:
            try:
                await state.process_events()
                if time.time() - last_ping_time >= 600:
                    logging.info("Bot vẫn đang chạy")
                    last_ping_time = time.time()
            except Exception as e:
                logging.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            await asyncio.sleep(interval)

async def safe_api_call(func, *args, **kwargs):
    try:
        result = func(*args, **kwargs)
        if hasattr(result, "__await__"):
            return await result
        return result
    except Exception as e:
        logging.error(f"API call error: {str(e)}")
        raise

async def rebalance_loop(state: BotState):
    while True:
        try:
            await state.grid_manager.rebalance_if_needed(state.current_price, state.exchange)
        except Exception as e:
            logging.error(f"Lỗi rebalance: {str(e)}")
        await asyncio.sleep(10)

if __name__ == "__main__":
    async def main():
        config.paper_trading_enabled = True
        exchange = PaperTradingExchange(config.symbol)
        grid_manager = EnhancedSmartGridManager()
        strategy = AdaptiveGridStrategy(grid_manager) if config.strategy_mode == 1 else DenseGridStrategy(grid_manager)
        state = BotState(exchange, strategy)
        states = {exchange.exchange_name: state}

        if config.backtest_enabled:
            logging.info("Bắt đầu chạy chế độ Backtest")
            df = await exchange.fetch_historical_data(config.backtest_timeframe, config.backtest_limit)
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
                    if 'orderId' in order:
                        state.grid_manager.order_ids.append(order['orderId'])
                await state.order_manager.check_and_handle_orders(price, exchange, state.tracker, state.grid_manager)
                if config.trailing_stop_enabled:
                    await state.handle_trailing_stops()
                await state.grid_manager.rebalance_if_needed(price, exchange)
            stats = state.tracker.get_stats()
            logging.info(f"Backtest hoàn tất: {stats}")
            return

        logging.info("Hủy tất cả lệnh mở trước khi khởi động bot")
        await state.order_manager.cancel_all_orders(exchange)
        await grid_manager.cancel_all_orders(exchange, preserve_static=False)

        load_state(states)
        await asyncio.gather(
            exchange.run_paper_trading_live(state, interval=1),
            rebalance_loop(state),
            periodic_save_state(states)
        )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot đã dừng lại bởi người dùng")