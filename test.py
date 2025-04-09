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
import ccxt.async_support as ccxt_async
import dash
from dash import dcc, html, Output, Input
import plotly.graph_objs as go
from concurrent.futures import ThreadPoolExecutor
import cachetools
import psutil

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
        self.grid_rebalance_interval: int = int(os.getenv("GRID_REBALANCE_INTERVAL", 60))  # Giảm từ 300 xuống 60 giây
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

config = AdvancedConfig()

# ======== GỬI CẢNH BÁO TELEGRAM ========
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

    async def calculate_adaptive_grid(self, current_price: float, exchange: Optional['ExchangeInterface'] = None) -> None:
        # Kiểm tra giá hiện tại hợp lệ
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

        atr = calculate_atr(
            list(self.high_history),
            list(self.low_history),
            list(self.price_history),
            config.atr_period
        )
        volatility = atr / current_price if current_price != 0 else 0.0
        volatility = min(volatility, 0.1)  # Giới hạn volatility tối đa 10%
        num_levels = max(config.min_grid_levels, min(config.max_grid_levels, int(volatility / (config.base_grid_step_percent / 100))))
        step_percent = volatility / num_levels if num_levels > 0 else config.base_grid_step_percent / 100
        step_percent = min(step_percent, 0.02)  # Giới hạn step_percent tối đa 2%

        self.grid_levels = []
        for i in range(num_levels):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            if buy_price < current_price * 0.5 or sell_price > current_price * 1.5:
                logging.warning(f"Giá vượt quá ngưỡng cho phép: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            self.grid_levels.append((buy_price, sell_price))

        logging.info(f"Đã tính toán lưới thích ứng: {num_levels} mức, bước {step_percent:.2%}")

    def _create_static_grid(self, current_price: float) -> List[Tuple[float, float]]:
        grid = []
        step_percent = config.base_grid_step_percent / 100
        for i in range(config.min_grid_levels):
            buy_price = current_price * (1 - step_percent * (i + 1))
            sell_price = current_price * (1 + step_percent * (i + 1))
            if buy_price <= 0 or sell_price <= 0:
                logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            if buy_price < current_price * 0.5 or sell_price > current_price * 1.5:
                logging.warning(f"Giá vượt quá ngưỡng cho phép: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                continue
            grid.append((buy_price, sell_price))
        return grid

    async def place_grid_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        await self.cancel_all_orders(exchange)
        filters = await exchange.get_symbol_filters()
        
        quantity_per_level = config.initial_investment / current_price / max(1, len(self.grid_levels))
        quantity = max(filters['minQty'], quantity_per_level)
        quantity = min(quantity, filters['maxQty'])
        step_size = filters.get('stepSize', 0.00001)
        quantity = round(quantity - (quantity % step_size), 6)
        
        if quantity < filters['minQty']:
            logging.warning(f"Số lượng {quantity} nhỏ hơn mức tối thiểu {filters['minQty']}")
            send_telegram_alert(f"⚠️ Số lượng {quantity} nhỏ hơn mức tối thiểu {filters['minQty']}")
            return

        has_buy_order = False
        for buy_price, sell_price in self.grid_levels:
            try:
                if buy_price <= 0 or sell_price <= 0:
                    logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                    continue

                if buy_price < current_price:
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
                
                if sell_price > current_price:
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
                
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh: {str(e)}")
                send_telegram_alert(f"⚠️ Lỗi khi đặt lệnh: {str(e)}")

        if not has_buy_order:
            try:
                buy_price = current_price * 0.99
                if buy_price > 0:
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
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh MUA động: {str(e)}")
                send_telegram_alert(f"⚠️ Lỗi khi đặt lệnh MUA động: {str(e)}")

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        for order_id in self.order_ids:
            try:
                await safe_api_call(exchange.cancel_order, order_id)
            except Exception as e:
                logging.warning(f"Lỗi khi hủy lệnh {order_id}: {str(e)}")
        self.order_ids.clear()

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

    async def check_and_handle_orders(self, current_price: float, exchange: 'ExchangeInterface', profit_tracker: 'EnhancedProfitTracker') -> None:
        try:
            orders = await safe_api_call(exchange.get_all_orders)
            for order in orders:
                if order['status'] == 'FILLED':
                    side = order['side']
                    price = float(order['price'])
                    qty = float(order['executedQty'])
                    
                    fee = qty * price * (config.maker_fee if order['type'] == 'LIMIT' else config.taker_fee)
                    profit = (current_price - price) * qty if side == 'BUY' else (price - current_price) * qty
                    
                    profit_tracker.record_trade(profit, fee)
                    
                    msg = (f"✅ Lệnh {side} đã khớp\n"
                           f"▪ Giá: {price:.2f}\n"
                           f"▪ Số lượng: {qty:.6f}\n"
                           f"▪ Lợi nhuận: {profit:.2f} USDT\n"
                           f"▪ Phí: {fee:.4f} USDT")
                    send_telegram_alert(msg)
                    
                    self.open_orders = [o for o in self.open_orders if o['orderId'] != order['orderId']]
                else:
                    if not any(o['orderId'] == order['orderId'] for o in self.open_orders):
                        self.open_orders.append(order)
        except Exception as e:
            logging.error(f"Lỗi khi kiểm tra lệnh: {str(e)}")
            send_telegram_alert(f"⚠️ Lỗi khi kiểm tra lệnh: {str(e)}")

# ======== GIAO DIỆN CHIẾN LƯỢc ========
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

    # Chỉnh sửa: hàm process_events thực hiện 1 vòng lặp cập nhật duy nhất
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
        if await check_protections(self, self.exchange, market_data.price, market_data.volume, market_data.high, market_data.low):
            return

        signals = await self.strategy.generate_signals(market_data)
        for signal in signals:
            try:
                await self.exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh: {signal.side} tại {signal.price}: {str(e)}")
        await handle_orders(self, self.exchange, market_data.price)
        await rebalance_grid_if_needed(self, self.exchange, market_data.price)

# ======== GIAO DIỆN SÀN GIAO DỊCH (ABSTRACTION) ========
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
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'type': o['type'].upper(), 'status': 'OPEN'} for o in orders]
        except Exception as e:
            logging.error(f"Lỗi khi lấy danh sách lệnh mở trên {self.exchange_name}: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'type': o['type'].upper(), 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]
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
                logging.error(f"Lỗi khi lấy thông tin LOT_SIZE từ {self.exchange_name} (lần thử {attempt + 1}/{config.api_retry_count}): {str(e)}")
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
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'type': o['type'].upper(), 'status': 'OPEN'} for o in orders]
        except Exception as e:
            logging.error(f"Lỗi khi lấy danh sách lệnh mở trên {self.exchange_name}: {str(e)}")
            raise

    async def get_all_orders(self) -> List[Dict]:
        try:
            orders = await self.client.fetch_orders(self.symbol, limit=50)
            return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'type': o['type'].upper(), 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]
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
                logging.error(f"Lỗi khi lấy thông tin LOT_SIZE từ {self.exchange_name} (lần thử {attempt + 1}/{config.api_retry_count}): {str(e)}")
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

# ======== QUẢN LÝ ĐA SÀN GIAO DỊCH ========
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
                logging.error(f"Sàn giao dịch {exchange_name} không được hỗ trợ!")
                raise ValueError(f"Sàn giao dịch không được hỗ trợ: {exchange_name}")
            logging.info(f"Đã khởi tạo sàn giao dịch {exchange_name}")

    async def check_price_disparity(self) -> None:
        prices = {}
        for name, ex in self.exchanges.items():
            prices[name] = await safe_api_call(ex.get_price)
        max_diff = max(prices.values()) - min(prices.values())
        if max_diff / min(prices.values()) > 0.01:
            logging.warning(f"Chênh lệch giá lớn: {max_diff:.2f} giữa {prices}")
            send_telegram_alert(f"⚠️ Chênh lệch giá lớn: {max_diff:.2f} giữa {prices}")

    async def close_all(self) -> None:
        for name, ex in self.exchanges.items():
            await ex.close()

# ======== XỬ LÝ LỖI ========
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
    rsi_calculator = RSICalculator(period)
    rsi = 50.0
    for i in range(1, len(prices)):
        rsi = rsi_calculator.update(prices[i], prices[i-1])
    return rsi

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
    historical_data = await safe_api_call(exchange.fetch_historical_data, timeframe='1h', limit=100)
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
    logging.info(f"Kết quả kiểm tra mô phỏng trên {exchange.exchange_name}: {stats}")
    send_telegram_alert(
        f"📉 Kết quả kiểm tra mô phỏng trên {exchange.exchange_name}:\n"
        f"PnL={stats['Total Profit']:.2f}, Giao Dịch={stats['Trade Count']}, "
        f"Sharpe={stats['Sharpe Ratio']:.2f}, MaxDD={stats['Max Drawdown']:.2f}%, Phí={stats['Total Fees']:.2f}"
    )
    return stats

# ======== KIỂM TRA THỰC TẾ ========
async def backtest_strategy(exchange: ExchangeInterface, state: 'BotState') -> Dict[str, float]:
    logging.info(f"Chạy kiểm tra thực tế trên {exchange.exchange_name}")
    stats = state.tracker.get_stats()
    logging.info(f"Kết quả kiểm tra thực tế trên {exchange.exchange_name}: {stats}")
    send_telegram_alert(
        f"📈 Kết quả kiểm tra thực tế trên {exchange.exchange_name}:\n"
        f"PnL={stats['Total Profit']:.2f}, Giao Dịch={stats['Trade Count']}, "
        f"Sharpe={stats['Sharpe Ratio']:.2f}, MaxDD={stats['Max Drawdown']:.2f}%, Phí={stats['Total Fees']:.2f}"
    )
    return stats

# ======== HÀM HỖ TRỢ THỰC THI BOT ========
async def check_account_balance(exchange: ExchangeInterface) -> Tuple[float, float]:
    try:
        usdt_balance, btc_balance = await safe_api_call(exchange.get_balance)
        logging.info(f"Số dư tài khoản trên {exchange.exchange_name}: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC")
        return usdt_balance, btc_balance
    except Exception as e:
        logging.error(f"Lỗi khi kiểm tra số dư: {str(e)}")
        return 0.0, 0.0

async def check_balance_before_trading(exchange: ExchangeInterface) -> bool:
    try:
        usdt_balance, _ = await safe_api_call(exchange.get_balance)
        if usdt_balance < config.initial_investment:
            msg = (f"⚠️ Số dư không đủ\n"
                   f"▪ Yêu cầu: {config.initial_investment} USDT\n"
                   f"▪ Hiện có: {usdt_balance:.2f} USDT")
            send_telegram_alert(msg)
            logging.warning(f"Số dư USDT không đủ: {usdt_balance:.2f} < {config.initial_investment}")
            return False
        return True
    except Exception as e:
        logging.error(f"Lỗi kiểm tra số dư: {str(e)}")
        send_telegram_alert(f"⚠️ Lỗi kiểm tra số dư: {str(e)}")
        return False

async def check_api_connectivity(exchange: ExchangeInterface) -> bool:
    try:
        await exchange.get_price()
        return True
    except Exception as e:
        logging.error(f"Lỗi kết nối API: {str(e)}")
        send_telegram_alert(f"🔴 Mất kết nối với {exchange.exchange_name}")
        return False

async def handle_exchange_downtime(state: BotState, downtime_start: Optional[float]) -> Tuple[bool, Optional[float]]:
    if downtime_start is None:
        downtime_start = time.time()
        logging.warning(f"Sàn {state.exchange_name} không khả dụng, chuyển sang chế độ an toàn")
        send_telegram_alert(f"⚠️ Sàn {state.exchange_name} không khả dụng, chuyển sang chế độ an toàn")
    elapsed = time.time() - downtime_start
    if elapsed > 300:
        logging.error(f"Sàn {state.exchange_name} không khả dụng quá lâu, dừng bot")
        send_telegram_alert(f"❌ Sàn {state.exchange_name} không khả dụng quá lâu, dừng bot")
        return True, downtime_start
    return True, downtime_start

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

async def handle_orders(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    await state.order_manager.check_and_handle_orders(current_price, exchange, state.tracker)

async def rebalance_grid_if_needed(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    if time.time() - state.grid_manager.last_rebalance > config.grid_rebalance_interval:
        await state.grid_manager.calculate_adaptive_grid(current_price, exchange)
        await state.grid_manager.place_grid_orders(current_price, exchange)
        state.grid_manager.last_rebalance = time.time()
        logging.info(f"Đã cân bằng lại lưới trên {state.exchange_name}")

async def update_status(state: BotState, exchange: ExchangeInterface, last_status_update: float) -> float:
    logging.info(f"Kiểm tra cập nhật trạng thái: time.time()={time.time():.2f}, last_status_update={last_status_update:.2f}, interval={config.status_update_interval}")
    if time.time() - last_status_update > config.status_update_interval:
        try:
            stats = state.tracker.get_stats()
            usdt_balance, btc_balance = await safe_api_call(exchange.get_balance)
            health = await exchange.health_check()
            atr = calculate_atr(
                list(state.grid_manager.high_history),
                list(state.grid_manager.low_history),
                list(state.grid_manager.price_history),
                config.atr_period
            )
            rsi = calculate_rsi(list(state.grid_manager.price_history), config.rsi_period)
            bb_middle, bb_upper, bb_lower = calculate_bollinger_bands(
                list(state.grid_manager.price_history), config.bb_period, config.bb_std_dev
            )
            open_orders = await safe_api_call(exchange.get_open_orders)
            open_orders_info = "\n".join(
                [f"- {order['side']} tại {order['price']:.2f}, số lượng: {order['origQty']}" for order in open_orders]
            ) if open_orders else "Không có lệnh mở"
            latency = health.get("api_latency_ms")
            latency_str = f"{latency:.2f}ms" if latency is not None else "Không xác định"

            message = (
                f"📊 Cập nhật trạng thái trên {state.exchange_name}:\n"
                f"**Giá Hiện Tại**: {state.current_price:.2f}\n"
                f"**Tổng Lợi Nhuận**: {stats['Total Profit']:.2f}\n"
                f"**Số Giao Dịch**: {stats['Trade Count']}\n"
                f"**Tỷ Lệ Thắng**: {stats['Win Rate']:.2f}%\n"
                f"**Sụt Giảm Tối Đa**: {stats['Max Drawdown']:.2f}%\n"
                f"**Tỷ Lệ Sharpe**: {stats['Sharpe Ratio']:.2f}\n"
                f"**Tổng Phí**: {stats['Total Fees']:.2f}\n"
                f"**Số Dư**: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC\n"
                f"**Chỉ Số Kỹ Thuật**:\n"
                f"- ATR: {atr:.2f}\n"
                f"- RSI: {rsi:.2f}\n"
                f"- Bollinger Bands: (Lower: {bb_lower:.2f}, Middle: {bb_middle:.2f}, Upper: {bb_upper:.2f})\n"
                f"**Lệnh Đang Mở**:\n{open_orders_info}\n"
                f"**Sức Khỏe**: {health['status']} (Độ trễ: {latency_str})"
            )
            logging.info(f"Cập nhật trạng thái trên {state.exchange_name}: {stats}, Số dư: {usdt_balance:.2f} USDT, {btc_balance:.6f} BTC, Sức khỏe: {health}")
            send_telegram_alert(message)
            last_status_update = time.time()
        except Exception as e:
            logging.error(f"Lỗi khi cập nhật trạng thái: {str(e)}")
            send_telegram_alert(f"⚠️ Lỗi khi cập nhật trạng thái trên {state.exchange_name}: {str(e)}")
    return last_status_update

# ======== CHẠY BOT CHO TỪNG SÀN GIAO DỊCH ========
async def run_bot_for_exchange(exchange: ExchangeInterface, states: Dict[str, BotState]) -> None:
    try:
        strategy = AdaptiveGridStrategy(EnhancedSmartGridManager())
        state = BotState(exchange, strategy)
        states[exchange.exchange_name] = state

        logging.info(f"🤖 Bot đang khởi động trên {exchange.exchange_name.upper()}")
        await state.order_manager.cancel_all_orders(exchange)
        
        if not await check_api_connectivity(exchange):
            logging.error(f"Không thể kết nối với {exchange.exchange_name}, dừng bot")
            return

        usdt_balance, btc_balance = await check_account_balance(exchange)
        if not await check_balance_before_trading(exchange):
            logging.error(f"Số dư không đủ trên {exchange.exchange_name}, dừng bot")
            return

        try:
            state.current_price = await safe_api_call(exchange.get_price)
            state.current_volume = await safe_api_call(exchange.get_volume)
            state.current_high, state.current_low = await safe_api_call(exchange.get_high_low)
        except Exception as e:
            logging.error(f"Lỗi khi lấy giá ban đầu: {str(e)}")
            send_telegram_alert(f"❌ Lỗi khi lấy giá ban đầu trên {exchange.exchange_name}: {str(e)}")
            return

        health = await exchange.health_check()
        logging.info(f"Kiểm tra sức khỏe ban đầu: {health}")
        if health['status'] != 'healthy':
            send_telegram_alert(f"⚠️ Kiểm tra sức khỏe thất bại trên {exchange.exchange_name}: {health['status']}")
            return

        await state.grid_manager.calculate_adaptive_grid(state.current_price, exchange)
        await state.grid_manager.place_grid_orders(state.current_price, exchange)

        last_status_update = time.time()
        safe_mode = False
        downtime_start = None

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

            logging.info(f"Thời gian kể từ lần cập nhật trạng thái cuối: {time.time() - last_status_update:.2f} giây")
            last_status_update = await update_status(state, exchange, last_status_update)
            await backtest_strategy(exchange, state)
            await asyncio.sleep(60)  # Khoảng thời gian giữa các vòng lặp chính

    except KeyboardInterrupt:
        logging.info(f"🤖 Bot trên {exchange.exchange_name} đã bị dừng bởi người dùng")
        send_telegram_alert(f"🤖 Bot trên {exchange.exchange_name} đã bị dừng bởi người dùng")
        await state.order_manager.cancel_all_orders(exchange)
    except Exception as e:
        logging.critical(f"🚨 Lỗi nghiêm trọng trên {exchange.exchange_name}: {str(e)}", exc_info=True)
        send_telegram_alert(f"🚨 Bot dừng hoạt động do lỗi trên {exchange.exchange_name}: {str(e)}")
        await state.order_manager.cancel_all_orders(exchange)
    finally:
        await exchange.close()
        logging.info(f"Bot trên {exchange.exchange_name} đã kết thúc")

# ======== CÀI ĐẶT DASH APP ========
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Bảng Điều Khiển Bot Giao Dịch Lưới"),
    dcc.Graph(id='price-chart'),
    dcc.Graph(id='profit-chart'),
    html.Div(id='open-orders'),
    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0)
])

@app.callback(
    [Output('price-chart', 'figure'), 
     Output('profit-chart', 'figure'), 
     Output('open-orders', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    price_fig = go.Figure()
    profit_fig = go.Figure()
    orders_list = []
    
    for ex_name, state in bot_states.items():
        price_fig.add_trace(go.Scatter(
            x=list(range(len(state.grid_manager.price_history))),
            y=list(state.grid_manager.price_history),
            name=f"{ex_name} Price"
        ))
        profit_fig.add_trace(go.Scatter(
            x=list(range(len(state.tracker.equity_curve))),
            y=state.tracker.equity_curve,
            name=f"{ex_name} Profit"
        ))
        orders_list.append(html.H4(f"{ex_name} Open Orders:"))
        for order in state.order_manager.open_orders:
            orders_list.append(html.P(
                f"{order['side']} {order['origQty']} @ {order['price']}"
            ))
    
    return (
        {'data': price_fig.data, 'layout': {'title': 'Price History'}},
        {'data': profit_fig.data, 'layout': {'title': 'Profit Curve'}},
        orders_list if orders_list else [html.P("Không có lệnh mở")]
    )

# ======== BIẾN TOÀN CỤC ĐỂ LƯU TRẠNG THÁI BOT ========
bot_states: Dict[str, BotState] = {}

# ======== HÀM CHẠY DASH APP ========
def run_dash():
    app.run(debug=False, host='0.0.0.0', port=8050)

# ======== HÀM CHÍNH ========
async def main():
    exchange_manager = ExchangeManager()
    tasks = []

    for exchange_name, exchange in exchange_manager.exchanges.items():
        if not await check_balance_before_trading(exchange):
            continue
        await run_simulated_backtest(exchange)
        bot_task = asyncio.create_task(run_bot_for_exchange(exchange, bot_states))
        tasks.append(bot_task)

    dash_thread = Thread(target=run_dash, daemon=True)
    dash_thread.start()
    logging.info("Đã khởi động Dash app trên http://0.0.0.0:8050")

    try:
        while True:
            await exchange_manager.check_price_disparity()
            await asyncio.sleep(300)
    except KeyboardInterrupt:
        logging.info("Dừng bot bởi người dùng")
        for task in tasks:
            task.cancel()
        await exchange_manager.close_all()
    except Exception as e:
        logging.error(f"Lỗi trong main loop: {str(e)}")
        for task in tasks:
            task.cancel()
        await exchange_manager.close_all()

if __name__ == "__main__":
    asyncio.run(main())