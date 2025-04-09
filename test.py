import time
import math
import os
from typing import List, Tuple, Dict, Optional, Any
from collections import deque
import asyncio
import logging
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
import ccxt.async_support as ccxt_async
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
        self.initial_investment: float = float(os.getenv("INITIAL_INVESTMENT", 500))
        self.min_quantity: float = float(os.getenv("MIN_QUANTITY", 0.001))
        self.base_stop_loss_percent: float = float(os.getenv("BASE_STOP_LOSS_PERCENT", 2.0))
        self.base_take_profit_percent: float = float(os.getenv("BASE_TAKE_PROFIT_PERCENT", 2.0))
        self.adaptive_grid_enabled: bool = os.getenv("ADAPTIVE_GRID_ENABLED", "True").lower() == "true"
        self.min_grid_levels: int = int(os.getenv("MIN_GRID_LEVELS", 3))
        self.max_grid_levels: int = int(os.getenv("MAX_GRID_LEVELS", 10))
        self.base_grid_step_percent: float = float(os.getenv("BASE_GRID_STEP_PERCENT", 2.0))
        self.grid_rebalance_interval: int = int(os.getenv("GRID_REBALANCE_INTERVAL", 30))
        self.trailing_stop_enabled: bool = os.getenv("TRAILING_STOP_ENABLED", "True").lower() == "true"
        self.trailing_up_activation: float = float(os.getenv("TRAILING_UP_ACTIVATION", 85000))
        self.trailing_down_activation: float = float(os.getenv("TRAILING_DOWN_ACTIVATION", 82000))
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
        self.atr_period: int = int(os.getenv("ATR_PERIOD", 14))
        self.rsi_period: int = int(os.getenv("RSI_PERIOD", 14))
        self.bb_period: int = int(os.getenv("BB_PERIOD", 20))
        self.bb_std_dev: float = float(os.getenv("BB_STD_DEV", 1.8))

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
class MarketData:
    def __init__(self, price: float, high: float, low: float, volume: float):
        self.price = price
        self.high = high
        self.low = low
        self.volume = volume

class Signal:
    def __init__(self, side: str, price: float, quantity: float):
        self.side = side
        self.price = price
        self.quantity = quantity

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
        max_drawdown = self.calculate_max_drawdown()
        returns = [self.equity_curve[i] - self.equity_curve[i-1] for i in range(1, len(self.equity_curve))] if len(self.equity_curve) > 1 else []
        sharpe_ratio = self.calculate_sharpe_ratio(returns)
        return {
            "Total Profit": self.total_profit,
            "Trade Count": self.trade_count,
            "Win Rate": win_rate,
            "Max Drawdown": max_drawdown,
            "Sharpe Ratio": sharpe_ratio,
            "Total Fees": self.total_fees
        }

    def calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        if len(returns) < 2:
            return 0.0
        mean_return = float(np.mean(returns))
        std_return = float(np.std(returns))
        if std_return < 1e-6:
            return 0.0
        return (mean_return - risk_free_rate) / std_return * np.sqrt(252)

    def calculate_max_drawdown(self) -> float:
        if not self.equity_curve or len(self.equity_curve) < 2:
            return 0.0
        peak = self.equity_curve[0]
        max_dd = 0.0
        for value in self.equity_curve:
            if peak == 0:
                continue
            peak = max(peak, value)
            dd = (peak - value) / peak if peak != 0 else 0.0
            max_dd = max(max_dd, dd)
        return max_dd * 100

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

    async def calculate_adaptive_grid(self, current_price: float, exchange: Any = None) -> None:
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
        volatility = min(volatility, 0.05)
        num_levels = max(config.min_grid_levels, min(config.max_grid_levels, int(volatility / (config.base_grid_step_percent / 100))))
        step_percent = config.base_grid_step_percent / 100

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

        logging.info(f"Đã tính toán lưới thích ứng: {num_levels} mức, bước {step_percent:.2%}, giá hiện tại: {current_price:.2f}")
        send_telegram_alert(f"🔄 Đã tính toán lưới thích ứng: {num_levels} mức, bước {step_percent:.2%}, giá hiện tại: {current_price:.2f}")

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
        usdt_balance, btc_balance = await exchange.get_balance()
        required_balance = config.initial_investment
        if usdt_balance < required_balance:
            logging.warning(f"Số dư USDT không đủ để đặt lệnh: {usdt_balance:.2f} < {required_balance:.2f}")
            send_telegram_alert(f"⚠️ Số dư USDT không đủ để đặt lệnh: {usdt_balance:.2f} < {required_balance:.2f}")
            return

        # Hủy tất cả lệnh cũ trước khi đặt lệnh mới
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

        if len(self.order_ids) >= config.max_open_orders:
            logging.warning(f"Đã đạt giới hạn số lệnh mở tối đa: {config.max_open_orders}")
            send_telegram_alert(f"⚠️ Đã đạt giới hạn số lệnh mở tối đa: {config.max_open_orders}")
            return

        has_buy_order = False
        has_sell_order = False
        for buy_price, sell_price in self.grid_levels:
            try:
                if buy_price <= 0 or sell_price <= 0:
                    logging.warning(f"Giá không hợp lệ: buy_price={buy_price}, sell_price={sell_price}. Bỏ qua mức lưới này.")
                    continue

                if buy_price < current_price and not has_buy_order:
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
                
                if sell_price > current_price and not has_sell_order:
                    sell_order = await safe_api_call(
                        exchange.place_order,
                        side='SELL',
                        order_type='LIMIT',
                        quantity=quantity,
                        price=sell_price
                    )
                    self.order_ids.append(sell_order['id'])
                    has_sell_order = True
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

        if not has_sell_order:
            try:
                sell_price = current_price * 1.01
                if sell_price > 0:
                    sell_order = await safe_api_call(
                        exchange.place_order,
                        side='SELL',
                        order_type='LIMIT',
                        quantity=quantity,
                        price=sell_price
                    )
                    self.order_ids.append(sell_order['id'])
                    logging.info(f"Đã đặt lệnh BÁN động tại {sell_price:.2f}, số lượng: {quantity:.6f}")
                    send_telegram_alert(f"📤 Đã đặt lệnh BÁN động tại {sell_price:.2f}, số lượng: {quantity:.6f}")
            except Exception as e:
                logging.error(f"Lỗi khi đặt lệnh BÁN động: {str(e)}")
                send_telegram_alert(f"⚠️ Lỗi khi đặt lệnh BÁN động: {str(e)}")

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        try:
            open_orders = await safe_api_call(exchange.get_open_orders)
            for order in open_orders:
                try:
                    await safe_api_call(exchange.cancel_order, order['orderId'])
                    logging.info(f"Đã hủy lệnh {order['orderId']} tại giá {order['price']:.2f}")
                except Exception as e:
                    logging.warning(f"Lỗi khi hủy lệnh {order['orderId']}: {str(e)}")
            self.order_ids.clear()
            logging.info("Đã hủy tất cả lệnh mở trong grid manager")
        except Exception as e:
            logging.error(f"Lỗi khi hủy tất cả lệnh trong grid manager: {str(e)}")

class EnhancedOrderManager:
    def __init__(self):
        self.open_orders: List[Dict] = []
        self.is_paused: bool = False
        self.last_trade_time: Dict[float, float] = {}
        self.last_trade_price: Dict[float, str] = {}
        self.cooldown_period: int = 120
        self.price_change_threshold: float = 0.005
        self.max_orders_per_price: int = 1
        self.processed_orders: set = set()

    async def cancel_all_orders(self, exchange: 'ExchangeInterface') -> None:
        try:
            open_orders = await safe_api_call(exchange.get_open_orders)
            for order in open_orders:
                await safe_api_call(exchange.cancel_order, order['orderId'])
                logging.info(f"Đã hủy lệnh {order['orderId']} tại giá {order['price']:.2f}")
            self.open_orders.clear()
            logging.info("Đã hủy tất cả lệnh mở trong order manager")
        except Exception as e:
            logging.error(f"Lỗi khi hủy tất cả lệnh trong order manager: {str(e)}")

    async def check_and_handle_orders(self, current_price: float, exchange: 'ExchangeInterface', profit_tracker: 'EnhancedProfitTracker', grid_levels: List[Tuple[float, float]]) -> None:
        try:
            orders = await safe_api_call(exchange.get_all_orders)
            for order in orders:
                order_id = order['orderId']
                if order['status'] == 'FILLED' and order_id not in self.processed_orders:
                    side = order['side']
                    price = float(order['price'])
                    qty = float(order['executedQty'])

                    # Kiểm tra xem giá khớp có thuộc grid_levels không
                    is_valid_price = False
                    for buy_price, sell_price in grid_levels:
                        if side == 'BUY' and abs(price - buy_price) < 0.01:
                            is_valid_price = True
                            break
                        if side == 'SELL' and abs(price - sell_price) < 0.01:
                            is_valid_price = True
                            break
                    if not is_valid_price:
                        logging.warning(f"Lệnh {side} khớp tại {price:.2f} không thuộc grid_levels. Bỏ qua.")
                        send_telegram_alert(f"⚠️ Lệnh {side} khớp tại {price:.2f} không thuộc grid_levels. Bỏ qua.")
                        self.processed_orders.add(order_id)
                        continue

                    # Tính phí và lợi nhuận
                    fee = qty * price * (config.maker_fee if order['type'] == 'LIMIT' else config.taker_fee)
                    if fee > qty * price * 0.01:
                        logging.warning(f"Phí giao dịch bất thường: {fee:.4f} cho lệnh {side} tại {price:.2f}, số lượng: {qty:.6f}")
                        send_telegram_alert(f"⚠️ Phí giao dịch bất thường: {fee:.4f} cho lệnh {side} tại {price:.2f}, số lượng: {qty:.6f}")
                        fee = qty * price * config.maker_fee
                    profit = (current_price - price) * qty if side == 'BUY' else (price - current_price) * qty
                    
                    profit_tracker.record_trade(profit, fee)
                    
                    msg = (f"✅ Lệnh {side} đã khớp\n"
                           f"▪ Giá: {price:.2f}\n"
                           f"▪ Số lượng: {qty:.6f}\n"
                           f"▪ Lợi nhuận: {profit:.2f} USDT\n"
                           f"▪ Phí: {fee:.4f} USDT")
                    send_telegram_alert(msg)
                    logging.info(f"Lệnh {side} đã khớp tại {price:.2f}, số lượng: {qty:.6f}, lợi nhuận: {profit:.2f}, phí: {fee:.4f}")

                    # Đánh dấu lệnh đã xử lý
                    self.processed_orders.add(order_id)

                    # Kiểm tra cooldown và giá thay đổi
                    current_time = time.time()
                    price_key = round(price, 2)
                    if price_key in self.last_trade_time:
                        time_since_last_trade = current_time - self.last_trade_time[price_key]
                        price_change = abs(current_price - price) / price if price != 0 else 0
                        if time_since_last_trade < self.cooldown_period:
                            logging.info(f"Đang trong thời gian cooldown tại giá {price:.2f} ({time_since_last_trade:.2f}s). Bỏ qua đặt lệnh đối ứng.")
                            send_telegram_alert(f"⏳ Đang trong thời gian cooldown tại giá {price:.2f} ({time_since_last_trade:.2f}s). Bỏ qua đặt lệnh đối ứng.")
                            continue
                        if price_change < self.price_change_threshold:
                            logging.info(f"Giá thay đổi không đủ tại {price:.2f} ({price_change:.2%} < {self.price_change_threshold:.2%}). Bỏ qua đặt lệnh đối ứng.")
                            send_telegram_alert(f"⏳ Giá thay đổi không đủ tại {price:.2f} ({price_change:.2%} < {self.price_change_threshold:.2%}). Bỏ qua đặt lệnh đối ứng.")
                            continue

                    # Kiểm tra số lượng lệnh mở tại mức giá này
                    open_orders_at_price = sum(1 for o in self.open_orders if abs(float(o['price']) - price) < 0.01)
                    if open_orders_at_price >= self.max_orders_per_price:
                        logging.info(f"Đã đạt giới hạn số lệnh tại giá {price:.2f} ({open_orders_at_price}/{self.max_orders_per_price}). Bỏ qua đặt lệnh đối ứng.")
                        send_telegram_alert(f"⏳ Đã đạt giới hạn số lệnh tại giá {price:.2f} ({open_orders_at_price}/{self.max_orders_per_price}). Bỏ qua đặt lệnh đối ứng.")
                        continue

                    # Cập nhật thời gian và loại lệnh cuối
                    self.last_trade_time[price_key] = current_time
                    self.last_trade_price[price_key] = side

                    # Đặt lại lệnh đối ứng
                    if side == 'SELL':
                        buy_price = price * 0.995
                        potential_profit = (price - buy_price) * qty - 2 * fee
                        if potential_profit <= 0:
                            logging.warning(f"Lợi nhuận tiềm năng không đủ: {potential_profit:.2f} USDT. Bỏ qua đặt lệnh BUY.")
                            send_telegram_alert(f"⚠️ Lợi nhuận tiềm năng không đủ: {potential_profit:.2f} USDT. Bỏ qua đặt lệnh BUY.")
                            continue
                        try:
                            buy_order = await safe_api_call(
                                exchange.place_order,
                                side='BUY',
                                order_type='LIMIT',
                                quantity=qty,
                                price=buy_price
                            )
                            logging.info(f"Đặt lại lệnh MUA tại {buy_price:.2f}, số lượng: {qty:.6f} sau khi SELL khớp")
                            send_telegram_alert(f"📥 Đặt lại lệnh MUA tại {buy_price:.2f}, số lượng: {qty:.6f} sau khi SELL khớp")
                            self.open_orders.append({'orderId': buy_order['id'], 'side': 'BUY', 'price': buy_price, 'origQty': qty})
                        except Exception as e:
                            logging.error(f"Lỗi khi đặt lại lệnh MUA: {str(e)}")
                            send_telegram_alert(f"⚠️ Lỗi khi đặt lại lệnh MUA: {str(e)}")
                    elif side == 'BUY':
                        sell_price = price * 1.005
                        potential_profit = (sell_price - price) * qty - 2 * fee
                        if potential_profit <= 0:
                            logging.warning(f"Lợi nhuận tiềm năng không đủ: {potential_profit:.2f} USDT. Bỏ qua đặt lệnh SELL.")
                            send_telegram_alert(f"⚠️ Lợi nhuận tiềm năng không đủ: {potential_profit:.2f} USDT. Bỏ qua đặt lệnh SELL.")
                            continue
                        try:
                            sell_order = await safe_api_call(
                                exchange.place_order,
                                side='SELL',
                                order_type='LIMIT',
                                quantity=qty,
                                price=sell_price
                            )
                            logging.info(f"Đặt lại lệnh BÁN tại {sell_price:.2f}, số lượng: {qty:.6f} sau khi BUY khớp")
                            send_telegram_alert(f"📤 Đặt lại lệnh BÁN tại {sell_price:.2f}, số lượng: {qty:.6f} sau khi BUY khớp")
                            self.open_orders.append({'orderId': sell_order['id'], 'side': 'SELL', 'price': sell_price, 'origQty': qty})
                        except Exception as e:
                            logging.error(f"Lỗi khi đặt lại lệnh BÁN: {str(e)}")
                            send_telegram_alert(f"⚠️ Lỗi khi đặt lại lệnh BÁN: {str(e)}")
                    
                    self.open_orders = [o for o in self.open_orders if o['orderId'] != order_id]
                else:
                    if not any(o['orderId'] == order_id for o in self.open_orders):
                        self.open_orders.append(order)
        except Exception as e:
            logging.error(f"Lỗi khi kiểm tra lệnh: {str(e)}")
            send_telegram_alert(f"⚠️ Lỗi khi kiểm tra lệnh: {str(e)}")

# ======== GIAO DIỆN CHIẾN LƯỢC ========
class GridStrategy:
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

    async def process_events(self) -> None:
        while True:
            try:
                self.current_price = await safe_api_call(self.exchange.get_price)
                self.current_volume = await safe_api_call(self.exchange.get_volume)
                self.current_high, self.current_low = await safe_api_call(self.exchange.get_high_low)
            except Exception as e:
                logging.error(f"Lỗi khi lấy dữ liệu thị trường: {str(e)}")
                await asyncio.sleep(60)
                continue

            market_data = MarketData(
                price=self.current_price,
                volume=self.current_volume,
                high=self.current_high,
                low=self.current_low
            )
            self.protection.update(market_data.price, market_data.high, market_data.low, market_data.volume)
            if await check_protections(self, self.exchange, market_data.price, market_data.volume, market_data.high, market_data.low):
                break
            signals = await self.strategy.generate_signals(market_data)
            for signal in signals:
                try:
                    await self.exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
                except Exception as e:
                    logging.error(f"Lỗi khi đặt lệnh: {signal.side} tại {signal.price}: {str(e)}")
            await handle_orders(self, self.exchange, market_data.price)
            await self.grid_manager.calculate_adaptive_grid(self.current_price, self.exchange)
            await self.grid_manager.place_grid_orders(self.current_price, self.exchange)
            await rebalance_grid_if_needed(self, self.exchange, market_data.price)
            await asyncio.sleep(60)

# ======== GIAO DIỆN SÀN GIAO DỊCH (ABSTRACTION) ========
class ExchangeInterface:
    def __init__(self, exchange_name: str, api_key: str, api_secret: str):
        self.exchange_name: str = exchange_name
        self.symbol: str = config.symbol
        self.price_precision: int = 2
        self.quantity_precision: int = 6
        self.client: Any = None

    async def get_price(self) -> float:
        pass

    async def get_volume(self) -> float:
        pass

    async def get_high_low(self) -> Tuple[float, float]:
        pass

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        pass

    async def cancel_order(self, order_id: str) -> None:
        pass

    async def get_open_orders(self) -> List[Dict]:
        pass

    async def get_all_orders(self) -> List[Dict]:
        pass

    async def get_balance(self) -> Tuple[float, float]:
        pass

    async def get_symbol_filters(self) -> Dict:
        pass

    async def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        pass

    async def close(self) -> None:
        pass

    async def health_check(self) -> Dict[str, Any]:
        try:
            start_time = time.time()
            await self.get_price()
            latency = (time.time() - start_time) * 1000
            return {
                'exchange': self.exchange_name,
                'api_latency_ms': latency,
                'status': 'healthy'
            }
        except Exception as e:
            return {
                'exchange': self.exchange_name,
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
            else:
                logging.error(f"Sàn giao dịch {exchange_name} không được hỗ trợ!")
                raise ValueError(f"Sàn giao dịch không được hỗ trợ: {exchange_name}")
            logging.info(f"Đã khởi tạo sàn giao dịch {exchange_name}")

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
def calculate_rsi(prices: List[float], period: int) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        gains.append(change if change > 0 else 0)
        losses.append(abs(change) if change < 0 else 0)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

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
    await state.order_manager.check_and_handle_orders(current_price, exchange, state.tracker, state.grid_manager.grid_levels)

async def rebalance_grid_if_needed(state: BotState, exchange: ExchangeInterface, current_price: float) -> None:
    if time.time() - state.grid_manager.last_rebalance > config.grid_rebalance_interval:
        await state.grid_manager.calculate_adaptive_grid(current_price, exchange)
        await state.grid_manager.place_grid_orders(current_price, exchange)
        state.grid_manager.last_rebalance = time.time()
        logging.info(f"Đã cân bằng lại lưới trên {state.exchange_name}")

async def update_status(state: BotState, exchange: ExchangeInterface, last_status_update: float) -> float:
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
                f"**Sức Khỏe**: {health['status']} (Độ trễ: {health['api_latency_ms']:.2f}ms)"
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
        # Hủy tất cả lệnh cũ khi khởi động bot
        await state.order_manager.cancel_all_orders(exchange)
        await state.grid_manager.cancel_all_orders(exchange)
        
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

            last_status_update = await update_status(state, exchange, last_status_update)
            
            cpu_usage = psutil.cpu_percent(interval=1)
            memory_usage = psutil.virtual_memory().percent
            if cpu_usage > 90 or memory_usage > 90:
                logging.warning(f"Tài nguyên hệ thống cao: CPU {cpu_usage}%, RAM {memory_usage}%")
                send_telegram_alert(f"⚠️ Tài nguyên hệ thống cao: CPU {cpu_usage}%, RAM {memory_usage}%")
                await asyncio.sleep(300)

    except Exception as e:
        logging.error(f"Lỗi nghiêm trọng trên {exchange.exchange_name}: {str(e)}")
        send_telegram_alert(f"❌ Lỗi nghiêm trọng trên {exchange.exchange_name}: {str(e)}")
    finally:
        await exchange.close()
        logging.info(f"Bot đã dừng trên {exchange.exchange_name}")
        send_telegram_alert(f"🛑 Bot đã dừng trên {exchange.exchange_name}")

# ======== KIỂM TRA YÊU CẦU TRƯỚC KHI CHẠY BOT ========
def check_requirements():
    if not os.path.exists(".env"):
        logging.error("File .env không tồn tại. Vui lòng tạo file .env và cấu hình các biến cần thiết.")
        return False
    
    required_vars = ["ENABLED_EXCHANGES", "TRADING_SYMBOL", "INITIAL_INVESTMENT"]
    for var in required_vars:
        if not os.getenv(var):
            logging.error(f"Biến môi trường {var} không được cấu hình trong file .env")
            return False
    
    for exchange in config.enabled_exchanges:
        api_key_var = f"{exchange.upper()}_API_KEY"
        api_secret_var = f"{exchange.upper()}_API_SECRET"
        if not os.getenv(api_key_var) or not os.getenv(api_secret_var):
            logging.error(f"API key hoặc secret cho {exchange} không được cấu hình")
            return False
    
    return True

# ======== HÀM MAIN ĐỂ CHẠY BOT ========
async def main():
    if not check_requirements():
        logging.error("Kiểm tra yêu cầu thất bại. Dừng bot.")
        return
    
    try:
        exchange_manager = ExchangeManager()
        states = {}
        tasks = []
        for exchange_name, exchange in exchange_manager.exchanges.items():
            task = asyncio.create_task(run_bot_for_exchange(exchange, states))
            tasks.append(task)
        await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"Lỗi khi chạy bot: {str(e)}")
        send_telegram_alert(f"❌ Lỗi khi chạy bot: {str(e)}")
    finally:
        await exchange_manager.close_all()

if __name__ == "__main__":
    asyncio.run(main())