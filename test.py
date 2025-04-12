import time
import math
from datetime import datetime, timedelta
import os
from typing import List, Tuple, Dict, Optional, Any
from collections import deque
from abc import ABC, abstractmethod
from dataclasses import dataclass
import asyncio
import logging
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
import ccxt.async_support as ccxt_async
import aiohttp  # dùng để gọi API bất đồng bộ
import cachetools

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

api_cache = cachetools.TTLCache(maxsize=100, ttl=300)

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
        self.min_grid_levels: int = int(os.getenv("MIN_GRID_LEVELS", 3))
        self.max_grid_levels: int = int(os.getenv("MAX_GRID_LEVELS", 10))
        self.base_grid_step_percent: float = float(os.getenv("BASE_GRID_STEP_PERCENT", 1.0))
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
        self.websocket_enabled: bool = os.getenv("WEBSOCKET_ENABLED", "False").lower() == "true"
        self.websocket_reconnect_interval: int = int(os.getenv("WEBSOCKET_RECONNECT_INTERVAL", 60))
        self.order_cooldown_sec: int = int(os.getenv("ORDER_COOLDOWN_SECONDS", 180))
        # Nếu bật paper trading, ta sẽ dùng sàn mô phỏng
        self.paper_trading_enabled: bool = os.getenv("PAPER_TRADING", "False").lower() == "true"

config = AdvancedConfig()

# ======== GỎI CẢNH BÁO TELEGRAM ========
def send_telegram_alert(message: str, parse_mode: str = "Markdown") -> None:
    if not config.telegram_enabled:
        return
    try:
        url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": config.telegram_chat_id,
            "text": message,
            "parse_mode": parse_mode
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logging.info(f"Đã gửi cảnh báo Telegram: {message}")
    except Exception as e:
        logging.error(f"Lỗi khi gửi cảnh báo Telegram: {str(e)}")

# ======== DATA CLASSES ========
@dataclass
class MarketData:
    price: float
    high: float
    low: float
    volume: float

@dataclass
class Signal:
    side: str  # "BUY" hoặc "SELL"
    price: float
    quantity: float

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
        if self.circuit_breaker_triggered:
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

    async def calculate_adaptive_grid(self, current_price: float, exchange: Optional['ExchangeInterface'] = None) -> None:
        if not isinstance(current_price, (int, float)) or current_price <= 0:
            logging.warning(f"Giá hiện tại không hợp lệ: {current_price}. Bỏ qua tính toán lưới.")
            return

        self.price_history.append(current_price)
        if not config.adaptive_grid_enabled or len(self.price_history) < config.volatility_window:
            self.grid_levels = self._create_static_grid(current_price)
            return

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
        precision = self._get_price_precision()
        target = round(price, precision)
        for order in open_orders:
            order_price = round(float(order['price']), precision)
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
            key_buy = ("BUY", round(buy_price, self._get_price_precision()))
            key_sell = ("SELL", round(sell_price, self._get_price_precision()))
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
                        self.order_ids.append(buy_order['id'])
                        has_buy_order = True
                        logging.info(f"Đã đặt lệnh MUA tại {buy_price:.2f}, số lượng: {quantity:.6f}")
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
                        self.order_ids.append(sell_order['id'])
                        logging.info(f"Đã đặt lệnh BÁN tại {sell_price:.2f}, số lượng: {quantity:.6f}")
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
                        self.order_ids.append(buy_order['id'])
                        logging.info(f"Đã đặt lệnh MUA động tại {buy_price:.2f}, số lượng: {quantity:.6f}")
                        send_telegram_alert(f"📥 Đã đặt lệnh MUA động tại {buy_price:.2f}, số lượng: {quantity:.6f}")
                    else:
                        logging.info(f"Bỏ qua lệnh MUA động tại {buy_price:.2f} do đã tồn tại lệnh trùng.")
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh MUA động: {str(e)}")
                send_telegram_alert(f"⚠️ Lỗi khi đặt lệnh MUA động: {str(e)}")

    def _get_price_precision(self) -> int:
        return 2

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        for order_id in self.order_ids:
            try:
                await safe_api_call(exchange.cancel_order, order_id)
            except Exception as e:
                logging.warning(f"Lỗi khi hủy lệnh {order_id}: {str(e)}")
        self.order_ids.clear()

    def set_cooldown(self, side: str, price: float):
        key = (side.upper(), round(price, self._get_price_precision()))
        self.grid_cooldown[key] = time.time() + config.order_cooldown_sec
        logging.info(f"Đặt cooldown cho {key} đến {self.grid_cooldown[key]:.2f}")

    def get_grid_range(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.grid_levels:
            return None, None
        lows = [l for l, _ in self.grid_levels]
        highs = [h for _, h in self.grid_levels]
        return min(lows), max(highs)

    def update_trailing_stop_for_level(self, grid_level: Tuple[float, float], current_price: float) -> float:
        _, sell_price = grid_level
        key = ("SELL", round(sell_price, self._get_price_precision()))
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
        if data.price is None or data.high is None or data.low is None:
            logging.warning(f"Dữ liệu không đầy đủ: price={data.price}, high={data.high}, low={data.low}. Bỏ qua lần cập nhật này.")
            return []
        self.grid_manager.price_history.append(data.price)
        self.grid_manager.high_history.append(data.high)
        self.grid_manager.low_history.append(data.low)
        await self.grid_manager.calculate_adaptive_grid(data.price)
        signals = []
        for buy_price, sell_price in self.grid_manager.grid_levels:
            if data.price <= buy_price:
                signals.append(Signal(side="BUY", price=buy_price, quantity=config.min_quantity))
            elif data.price >= sell_price:
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

    async def process_events(self) -> None:
        try:
            self.current_price = await safe_api_call(self.exchange.get_price)
            self.current_volume = await safe_api_call(self.exchange.get_volume)
            self.current_high, self.current_low = await safe_api_call(self.exchange.get_high_low)
        except Exception as e:
            logging.error(f"Lỗi khi lấy dữ liệu thị trường: {str(e)}")
            await asyncio.sleep(60)
            return
        market_data = MarketData(
            price=self.current_price,
            volume=self.current_volume,
            high=self.current_high,
            low=self.current_low
        )
        self.protection.update(market_data.price, market_data.high, market_data.low, market_data.volume)
        await self.grid_manager.calculate_adaptive_grid(self.current_price, self.exchange)
        # Kiểm tra các biện pháp bảo vệ (nếu có)
        if await check_protections(self, self.exchange, market_data.price, market_data.volume, market_data.high, market_data.low):
            return
        signals = await self.strategy.generate_signals(market_data)
        for signal in signals:
            try:
                await self.exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
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

# ======== PAPER TRADING EXCHANGE (MÔ PHỎNG) ========
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
            "id": len(self.paper_orders) + 1,
            "side": side.upper(),
            "price": price,
            "quantity": quantity,
            "type": order_type.upper(),
            "status": "OPEN",  # Đặt trạng thái ban đầu là OPEN
            "timestamp": time.time(),
            "executedQty": 0.0
        }
        self.paper_orders.append(order)
        # Không cập nhật số dư ngay lúc đặt lệnh; số dư sẽ được cập nhật trong _match_orders khi khớp đơn
        return order

    async def _match_orders(self):
        current_price = await self.get_price()
        base = self.symbol.replace("USDT", "")
        for o in self.paper_orders:
            if o["status"] == "OPEN":
                # Đối với BUY: khớp nếu giá thị trường <= giá đặt; đối với SELL: khớp nếu giá thị trường >= giá đặt
                if (o["side"] == "BUY" and current_price <= o["price"]) or (o["side"] == "SELL" and current_price >= o["price"]):
                    cost = o["price"] * o["quantity"]
                    if o["side"] == "BUY":
                        if self.simulated_balance["USDT"] < cost:
                            logging.error("Số dư USDT không đủ cho lệnh mua mô phỏng khi khớp.")
                            continue
                        self.simulated_balance["USDT"] -= cost
                        self.simulated_balance[base] += o["quantity"]
                    else:  # SELL
                        if self.simulated_balance[base] < o["quantity"]:
                            logging.error("Số dư không đủ để bán trong mô phỏng khi khớp.")
                            continue
                        self.simulated_balance[base] -= o["quantity"]
                        self.simulated_balance["USDT"] += cost
                    o["status"] = "FILLED"
                    o["executedQty"] = o["quantity"]

    async def get_all_orders(self) -> List[Dict]:
        await self._match_orders()
        return self.paper_orders

    async def get_open_orders(self) -> List[Dict]:
        await self._match_orders()
        return [o for o in self.paper_orders if o["status"] == "OPEN"]

    async def cancel_order(self, order_id: str) -> None:
        for order in self.paper_orders:
            if str(order["id"]) == str(order_id) and order["status"] == "OPEN":
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
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    async def start_websocket(self, state: BotState) -> None:
        return

    async def close(self) -> None:
        logging.info("Đóng kết nối paper trading (không có client thật)")

# ======== PHẦN CODE CHO CÁC SÀN GIAO DỊCH THỰC (Ví dụ: BinanceExchange và MEXCExchange)
# Các lớp dưới đây được giữ nguyên cấu trúc như code gốc của bạn

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
            return await self.client.create_order(
                symbol=self.symbol,
                side=side.lower(),
                type=order_type,
                amount=round(quantity, self.quantity_precision),
                price=price
            )
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
        finally:
            await self.close()

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
            return await self.client.create_order(
                symbol=self.symbol,
                side=side.lower(),
                type=order_type,
                amount=round(quantity, self.quantity_precision),
                price=price
            )
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
        finally:
            await self.close()

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

# ======== HÀM HỖ TRỢ VÀ TÍNH TOÁN CHỈ SỐ KỸ THUẬT ========
async def safe_api_call(func, *args, max_retries: int = 3, **kwargs) -> Any:
    for attempt in range(max_retries):
        try:
            cache_key = f"{func.__name__}_{str(args)}_{str(kwargs)}"
            if cache_key in api_cache:
                return api_cache[cache_key]
            result = await func(*args, **kwargs)
            api_cache[cache_key] = result
            return result
        except (ccxt_async.DDoSProtection, ccxt_async.RateLimitExceeded) as e:
            logging.warning(f"API bị giới hạn tốc độ: {str(e)}. Chờ {config.api_retry_delay} giây")
            await asyncio.sleep(config.api_retry_delay * (2 ** attempt))
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

def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
    if len(highs) != len(lows) or len(lows) != len(closes):
        logging.warning(f"Độ dài danh sách không khớp: highs={len(highs)}, lows={len(lows)}, closes={len(closes)}. Trả về ATR = 0.0")
        return 0.0
    if len(closes) < period + 1:
        return 0.0
    tr_list = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
    if not tr_list:
        return 0.0
    atr = sum(tr_list[-period:]) / period
    return atr

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        dd = (peak - value) / peak if peak != 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd * 100

def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
    if len(returns) < 2:
        return 0.0
    mean_return = float(np.mean(returns))
    std_return = float(np.std(returns))
    if std_return < 1e-6:
        return 0.0
    return (mean_return - risk_free_rate) / std_return * np.sqrt(252)

# ======== PHẦN CODE CHÍNH (VÒNG LẶP, KIỂM TRA, CẬP NHẬT TRẠNG THÁI BOT, v.v.) ========
# Các hàm kiểm tra số dư, kết nối API, cập nhật trạng thái, bảo vệ, xử lý đơn lệnh, rebalance grid,…
# Bạn có thể tham khảo lại code gốc của bạn để tích hợp vào vòng lặp chính.
# Ví dụ một hàm cập nhật trạng thái đơn giản:

async def check_protections(state: BotState, exchange: ExchangeInterface, current_price: float, current_volume: float, high: float, low: float) -> bool:
    if state.protection.check_pump_protection(current_price):
        logging.warning(f"Bảo vệ tăng giá đột biến được kích hoạt trên {state.exchange_name}")
        send_telegram_alert(f"⚠️ Bảo vệ tăng giá đột biến được kích hoạt trên {state.exchange_name}")
        state.order_manager.is_paused = True
        return False
    if state.protection.check_circuit_breaker(current_price) or state.protection.check_circuit_breaker_status():
        await state.order_manager.cancel_all_orders(exchange)
        return True
    if state.protection.check_abnormal_activity(current_volume):
        logging.warning(f"Phát hiện hoạt động bất thường trên {state.exchange_name}: Khối lượng {current_volume}")
        send_telegram_alert(f"⚠️ Phát hiện hoạt động bất thường trên {state.exchange_name}: Khối lượng {current_volume}")
        state.order_manager.is_paused = True
        return False
    should_stop, trailing_stop_price = state.protection.update_trailing_stop(current_price)
    if should_stop:
        logging.info(f"Trailing stop được kích hoạt trên {state.exchange_name} tại {trailing_stop_price:.2f}")
        send_telegram_alert(f"✅ Trailing stop được kích hoạt trên {state.exchange_name} tại {trailing_stop_price:.2f}")
        await state.order_manager.cancel_all_orders(exchange)
        return True
    should_buy, trailing_buy_price = state.protection.update_trailing_buy(current_price)
    if should_buy:
        logging.info(f"Trailing buy được kích hoạt trên {state.exchange_name} tại {trailing_buy_price:.2f}")
        send_telegram_alert(f"✅ Trailing buy được kích hoạt trên {state.exchange_name} tại {trailing_buy_price:.2f}")
        await state.grid_manager.place_grid_orders(current_price, exchange)
    return False

# Hàm main chạy bot (bạn có thể tùy chỉnh thêm)
async def run_bot():
    states: Dict[str, BotState] = {}
    # Khởi tạo sàn giao dịch
    if config.paper_trading_enabled:
        exchange = PaperTradingExchange(config.symbol)
    else:
        # Ví dụ: sử dụng BinanceExchange nếu không bật paper trading
        api_key, api_secret = config.exchange_credentials.get("binance", ("your_api_key", "your_api_secret"))
        exchange = BinanceExchange(api_key, api_secret)
    strategy = AdaptiveGridStrategy(EnhancedSmartGridManager())
    state = BotState(exchange, strategy)
    states[exchange.exchange_name] = state

    logging.info(f"🤖 Bot đang khởi động trên {exchange.exchange_name.upper()}")
    # Hủy tất cả đơn lệnh mở khi khởi động
    await state.order_manager.cancel_all_orders(exchange)

    # Kiểm tra kết nối API và số dư
    if not await safe_api_call(exchange.get_price):
        logging.error(f"Không thể kết nối với {exchange.exchange_name}, dừng bot")
        return
    usdt_balance, btc_balance = await safe_api_call(exchange.get_balance)
    if usdt_balance < config.initial_investment:
        logging.error(f"Số dư không đủ trên {exchange.exchange_name}, dừng bot")
        return

    # Vòng lặp chính của bot
    while True:
        await state.process_events()
        # Để đảm bảo các lệnh được khớp, gọi _match_orders ở sàn paper trading (nếu sử dụng)
        if exchange.exchange_name == "paper_trading":
            await exchange._match_orders()
        await asyncio.sleep(1)  # Điều chỉnh thời gian sleep tùy thuộc vào tần suất cập nhật thị trường

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logging.info("Bot dừng lại bởi người dùng.")