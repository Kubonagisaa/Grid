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
import cachetools
import psutil

# ======== CÀI ĐẶT BAN ĐẦU ========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('grid_trading_bot.log'), logging.StreamHandler()]
)

api_cache = cachetools.TTLCache(maxsize=100, ttl=300)

# ======== QUẢN LÝ CẤU HÌNH ========
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges = os.getenv("ENABLED_EXCHANGES", "binance").split(",")
        self.exchange_credentials = {
            ex: (os.getenv(f"{ex.upper()}_API_KEY", "your_api_key"), os.getenv(f"{ex.upper()}_API_SECRET", "your_api_secret"))
            for ex in self.enabled_exchanges
        }
        self.symbol = os.getenv("TRADING_SYMBOL", "BTCUSDT")
        self.initial_investment = float(os.getenv("INITIAL_INVESTMENT", 200))
        self.min_quantity = float(os.getenv("MIN_QUANTITY", 0.0005))
        self.max_position = float(os.getenv("MAX_POSITION", 0.01))
        self.base_stop_loss_percent = float(os.getenv("BASE_STOP_LOSS_PERCENT", 2.0))
        self.base_take_profit_percent = float(os.getenv("BASE_TAKE_PROFIT_PERCENT", 2.0))
        self.grid_adjust_threshold = float(os.getenv("GRID_ADJUST_THRESHOLD", 0.01))
        self.adaptive_grid_enabled = os.getenv("ADAPTIVE_GRID_ENABLED", "True").lower() == "true"
        self.trailing_grid_enabled = os.getenv("TRAILING_GRID_ENABLED", "True").lower() == "true"
        self.trailing_buy_enabled = os.getenv("TRAILING_BUY_ENABLED", "False").lower() == "true"
        self.trailing_buy_activation_percent = float(os.getenv("TRAILING_BUY_ACTIVATION_PERCENT", 1.0))
        self.trailing_buy_distance_percent = float(os.getenv("TRAILING_BUY_DISTANCE_PERCENT", 0.5))
        self.circuit_breaker_threshold = float(os.getenv("CIRCUIT_BREAKER_THRESHOLD", 0.07))
        self.circuit_breaker_duration = int(os.getenv("CIRCUIT_BREAKER_DURATION", 360))
        self.max_circuit_triggers = int(os.getenv("MAX_CIRCUIT_TRIGGERS", 3))
        self.maker_fee = float(os.getenv("MAKER_FEE", 0.0002))
        self.taker_fee = float(os.getenv("TAKER_FEE", 0.0004))
        self.telegram_enabled = bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.max_open_orders = int(os.getenv("MAX_OPEN_ORDERS", 100))
        self.volatility_window = int(os.getenv("VOLATILITY_WINDOW", 200))
        self.atr_period = int(os.getenv("ATR_PERIOD", 14))
        self.rsi_period = int(os.getenv("RSI_PERIOD", 14))
        self.ema_period = int(os.getenv("EMA_PERIOD", 20))
        self.bb_period = int(os.getenv("BB_PERIOD", 20))
        self.bb_std_dev = float(os.getenv("BB_STD_DEV", 2.0))
        self.drawdown_threshold = float(os.getenv("DRAWDOWN_THRESHOLD", 10.0))
        self.max_drawdown_alert = float(os.getenv("MAX_DRAWDOWN_ALERT", 20.0))
        self.grid_rebalance_interval = int(os.getenv("GRID_REBALANCE_INTERVAL", 300))
        self.min_grid_levels = int(os.getenv("MIN_GRID_LEVELS", 5))
        self.max_grid_levels = int(os.getenv("MAX_GRID_LEVELS", 8))
        self.base_grid_step_percent = float(os.getenv("BASE_GRID_STEP_PERCENT", 0.2))

config = AdvancedConfig()

# ======== GỬI CẢNH BÁO TELEGRAM ========
class TelegramRateLimiter:
    def __init__(self):
        self.last_sent = 0
        self.message_count = 0
        self.rate_limit = 20  # Số tin nhắn tối đa mỗi phút
        self.time_window = 60  # 60 giây
        self.min_interval = 1  # Khoảng cách tối thiểu giữa các tin nhắn (1 giây)

    def can_send(self) -> bool:
        current_time = time.time()
        if current_time - self.last_sent < self.min_interval:
            return False
        if current_time - self.last_sent > self.time_window:
            self.message_count = 0
            self.last_sent = current_time
        if self.message_count < self.rate_limit:
            self.message_count += 1
            self.last_sent = current_time
            return True
        return False

telegram_limiter = TelegramRateLimiter()

def send_telegram_alert(message: str) -> None:
    if not config.telegram_enabled:
        return
    if not telegram_limiter.can_send():
        logging.warning("Telegram rate limit reached, skipping alert")
        return
    try:
        url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
        payload = {"chat_id": config.telegram_chat_id, "text": message}
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logging.info(f"Telegram alert sent: {message}")
    except Exception as e:
        logging.error(f"Failed to send Telegram alert: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Telegram API response: {e.response.text}")

# ======== DATA CLASSES ========
@dataclass
class MarketData:
    price: float
    high: float
    low: float
    volume: float

@dataclass
class TradeSignal:
    side: str
    quantity: float
    price: float

# ======== TÍNH TOÁN CHỈ SỐ KỸ THUẬT ========
def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
    if len(highs) < period + 1:
        return 0.0
    tr_list = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
    return sum(tr_list[-period:]) / period if tr_list else 0.0

def calculate_rsi(prices: List[float], period: int) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(diff))
    avg_gain = sum(gains[-period:]) / period if gains else 0.0
    avg_loss = sum(losses[-period:]) / period if losses else 0.0
    rs = avg_gain / avg_loss if avg_loss > 0 else float('inf')
    return 100 - (100 / (1 + rs))

def calculate_ema(prices: List[float], period: int) -> List[float]:
    if not prices or len(prices) < period:
        return []
    multiplier = 2 / (period + 1)
    ema_values = [prices[0]]
    for price in prices[1:]:
        ema = (price - ema_values[-1]) * multiplier + ema_values[-1]
        ema_values.append(ema)
    return ema_values

def calculate_bollinger_bands(prices: List[float], period: int, std_dev: float) -> Tuple[float, float, float]:
    if len(prices) < period:
        return prices[-1], prices[-1], prices[-1] if prices else (0.0, 0.0, 0.0)
    sma = sum(prices[-period:]) / period
    variance = sum((p - sma) ** 2 for p in prices[-period:]) / period
    std = math.sqrt(variance)
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    return sma, upper, lower

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    if len(equity_curve) < 2:
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
    return (mean_return - risk_free_rate) / std_return * np.sqrt(252) if std_return > 0 else 0.0

# ======== CHIẾN LƯỢC GIAO DỊCH ========
class GridStrategy(ABC):
    @abstractmethod
    async def generate_signals(self, market_data: MarketData) -> List[TradeSignal]:
        pass

class AdaptiveGridStrategy(GridStrategy):
    def __init__(self):
        self.price_history = deque(maxlen=config.volatility_window)

    async def generate_signals(self, market_data: MarketData) -> List[TradeSignal]:
        self.price_history.append(market_data.price)
        if len(self.price_history) < max(config.rsi_period, config.ema_period, config.bb_period) + 1:
            return []

        rsi = calculate_rsi(list(self.price_history), config.rsi_period)
        sma, bb_upper, bb_lower = calculate_bollinger_bands(list(self.price_history), config.bb_period, config.bb_std_dev)
        signals = []

        quantity = config.min_quantity
        if market_data.price <= bb_lower:
            quantity *= 1.5
            if rsi < 35:
                signals.append(TradeSignal(side="BUY", quantity=quantity, price=market_data.price * 0.995))
                if rsi < 20:
                    send_telegram_alert(f"RSI Oversold: {rsi:.2f}, Price near BB Lower: {market_data.price:.2f}")
        elif market_data.price >= bb_upper:
            quantity *= 1.5
            if rsi > 65:
                signals.append(TradeSignal(side="SELL", quantity=quantity, price=market_data.price * 1.005))
                if rsi > 80:
                    send_telegram_alert(f"RSI Overbought: {rsi:.2f}, Price near BB Upper: {market_data.price:.2f}")

        return signals

# ======== CÁC LỚP NÂNG CAO ========
class EnhancedProfitTracker:
    def __init__(self):
        self.total_profit = 0.0
        self.trade_count = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.equity_curve = []
        self.total_fees = 0.0
        self.price_history = deque(maxlen=config.volatility_window)
        self.last_summary_time = time.time()
        self.consecutive_buys = 0
        self.consecutive_sells = 0
        self.max_consecutive_trades = 5

    def record_trade(self, profit: float, fee: float, side: str) -> None:
        self.total_profit += profit
        self.trade_count += 1
        self.total_fees += fee
        if profit > 0:
            self.winning_trades += 1
        elif profit < 0:
            self.losing_trades += 1
        self.equity_curve.append(self.total_profit)

        if side == 'BUY':
            self.consecutive_buys += 1
            self.consecutive_sells = 0
        elif side == 'SELL':
            self.consecutive_sells += 1
            self.consecutive_buys = 0

        if self.consecutive_buys >= self.max_consecutive_trades:
            logging.warning(f"Too many consecutive BUY trades: {self.consecutive_buys}")
            send_telegram_alert(f"Too many consecutive BUY trades: {self.consecutive_buys}")
        if self.consecutive_sells >= self.max_consecutive_trades:
            logging.warning(f"Too many consecutive SELL trades: {self.consecutive_sells}")
            send_telegram_alert(f"Too many consecutive SELL trades: {self.consecutive_sells}")

    def get_stats(self) -> Dict[str, float]:
        win_rate = (self.winning_trades / self.trade_count * 100) if self.trade_count > 0 else 0.0
        max_drawdown = calculate_max_drawdown(self.equity_curve)
        returns = [self.equity_curve[i] - self.equity_curve[i-1] for i in range(1, len(self.equity_curve))] if len(self.equity_curve) > 1 else []
        sharpe_ratio = calculate_sharpe_ratio(returns)
        rsi = calculate_rsi(list(self.price_history), config.rsi_period) if len(self.price_history) >= config.rsi_period else 50.0
        return {
            "Total Profit": self.total_profit,
            "Trade Count": self.trade_count,
            "Win Rate": win_rate,
            "Max Drawdown": max_drawdown,
            "Sharpe Ratio": sharpe_ratio,
            "Total Fees": self.total_fees,
            "RSI": rsi,
            "Consecutive Buys": self.consecutive_buys,
            "Consecutive Sells": self.consecutive_sells
        }

    def send_daily_summary(self) -> None:
        current_time = time.time()
        if current_time - self.last_summary_time >= 86400:
            stats = self.get_stats()
            message = (
                f"Daily Summary:\n"
                f"Total Profit: {stats['Total Profit']:.2f}\n"
                f"Trade Count: {stats['Trade Count']}\n"
                f"Win Rate: {stats['Win Rate']:.2f}%\n"
                f"Max Drawdown: {stats['Max Drawdown']:.2f}%\n"
                f"Total Fees: {stats['Total Fees']:.4f}"
            )
            send_telegram_alert(message)
            self.last_summary_time = current_time

class EnhancedTrailingManager:
    def __init__(self):
        self.best_price = 0.0
        self.trailing_stop = 0.0
        self.trailing_tp = 0.0
        self.trailing_buy_price = 0.0
        self.stop_loss_pct = config.base_stop_loss_percent
        self.take_profit_pct = config.base_take_profit_percent
        self.trailing_enabled = config.trailing_grid_enabled
        self.trailing_buy_enabled = config.trailing_buy_enabled

    def update(self, current_price: float) -> Tuple[bool, bool, float, float]:
        if not self.trailing_enabled:
            return False, False, self.trailing_stop, self.trailing_tp

        if self.best_price == 0.0 or current_price > self.best_price:
            self.best_price = current_price
            self.trailing_stop = self.best_price * (1 - self.stop_loss_pct / 100)
            self.trailing_tp = self.best_price * (1 + self.take_profit_pct / 100)

        stop_triggered = current_price <= self.trailing_stop
        tp_triggered = current_price >= self.trailing_tp
        if stop_triggered:
            send_telegram_alert(f"Trailing Stop triggered at {self.trailing_stop:.2f}")
        if tp_triggered:
            send_telegram_alert(f"Trailing Take-Profit triggered at {self.trailing_tp:.2f}")
        return stop_triggered, tp_triggered, self.trailing_stop, self.trailing_tp

    def update_trailing_buy(self, price: float) -> Tuple[bool, float]:
        if not self.trailing_buy_enabled:
            return False, self.trailing_buy_price
        
        if self.trailing_buy_price == 0.0:
            self.trailing_buy_price = price * (1 + config.trailing_buy_activation_percent / 100)
        if price < self.trailing_buy_price * (1 - config.trailing_buy_distance_percent / 100):
            self.trailing_buy_price = price * (1 + config.trailing_buy_activation_percent / 100)
        
        should_buy = price >= self.trailing_buy_price
        return should_buy, self.trailing_buy_price

class CircuitBreaker:
    def __init__(self):
        self.trigger_count = 0
        self.last_trigger = 0
        self.max_triggers = config.max_circuit_triggers

    def check(self, price_change: float) -> bool:
        if abs(price_change) > config.circuit_breaker_threshold:
            current_time = time.time()
            if current_time - self.last_trigger > config.circuit_breaker_duration:
                self.trigger_count = 0
            self.trigger_count += 1
            self.last_trigger = current_time
            logging.warning(f"Circuit breaker triggered: {price_change:.2%}, count: {self.trigger_count}")
            return self.trigger_count > self.max_triggers
        return False

class EnhancedProtectionSystem:
    def __init__(self):
        self.price_history = deque(maxlen=config.volatility_window)
        self.high_history = deque(maxlen=config.volatility_window)
        self.low_history = deque(maxlen=config.volatility_window)
        self.volume_history = deque(maxlen=config.volatility_window)
        self.circuit_breaker = CircuitBreaker()
        self.trailing_manager = EnhancedTrailingManager()

    def update(self, price: float, high: float, low: float, volume: float) -> None:
        self.price_history.append(price)
        self.high_history.append(high)
        self.low_history.append(low)
        self.volume_history.append(volume)

    def check_circuit_breaker(self, price: float) -> bool:
        if len(self.price_history) < 2:
            return False
        price_change = (price - self.price_history[-2]) / self.price_history[-2]
        return self.circuit_breaker.check(price_change)

class SmartGridManager:
    def __init__(self):
        self.grid_levels = []
        self.last_price = 0.0
        self.order_ids = []
        self.price_history = deque(maxlen=config.volatility_window)
        self.high_history = deque(maxlen=config.volatility_window)
        self.low_history = deque(maxlen=config.volatility_window)
        self.adaptive_enabled = config.adaptive_grid_enabled
        self.trailing_enabled = config.trailing_grid_enabled
        self.optimal_levels = config.min_grid_levels
        self.grid_spacing = config.base_grid_step_percent / 100
        self.last_adjust_time = time.time()
        self.is_initializing = True
        self.initial_wait_time = 60

    def calculate_market_conditions(self) -> Tuple[float, float]:
        if len(self.price_history) < config.atr_period:
            return 0.01, 0
            
        closes = list(self.price_history)
        highs = list(self.high_history)
        lows = list(self.low_history)
        
        atr = calculate_atr(highs, lows, closes, config.atr_period)
        volatility = atr / closes[-1] if closes[-1] > 0 else 0.01
        
        ema_values = calculate_ema(closes, config.ema_period)
        trend_strength = 0
        if len(ema_values) > 1:
            trend_strength = (ema_values[-1] - ema_values[-2]) / ema_values[-2] * 100
            
        return volatility, trend_strength

    def auto_adjust_grid_params(self, current_price: float, balance: float, drawdown: float) -> None:
        volatility, trend_strength = self.calculate_market_conditions()
        
        base_levels = min(
            int(balance / (current_price * config.min_quantity)),
            config.max_grid_levels
        )
        
        volatility_factor = max(0.5, min(2.0, volatility / 0.01))
        trend_factor = 1.0 + abs(trend_strength) / 50
        
        self.optimal_levels = max(
            config.min_grid_levels,
            min(
                config.max_grid_levels,
                int(base_levels / (volatility_factor * 0.5) / (trend_factor * 0.5))
            )
        )
        
        if drawdown > config.drawdown_threshold:
            self.optimal_levels = max(config.min_grid_levels, int(self.optimal_levels * 0.75))
            logging.info(f"Reduced grid levels to {self.optimal_levels} due to drawdown: {drawdown:.2f}%")
            if drawdown > config.max_drawdown_alert:
                send_telegram_alert(f"High Drawdown Alert: {drawdown:.2f}%")
        
        self.grid_spacing = max(
            config.base_grid_step_percent / 100,
            min(
                volatility * 1.5,
                0.02
            )
        )
        
        logging.info(
            f"Grid auto-adjusted: levels={self.optimal_levels}, "
            f"spacing={self.grid_spacing:.2%}, "
            f"volatility={volatility:.2%}, "
            f"trend={trend_strength:.2f}%"
        )

    async def update_grid(self, current_price: float, exchange: 'ExchangeInterface', drawdown: float, profit_tracker: 'EnhancedProfitTracker') -> None:
        try:
            if not isinstance(current_price, (int, float)) or current_price <= 0:
                logging.warning(f"Invalid price for grid update: {current_price}")
                return
                
            self.price_history.append(current_price)
            self.high_history.append(current_price)
            self.low_history.append(current_price)
            
            if self.is_initializing:
                if len(self.price_history) < 5:
                    logging.info("Bot is initializing, collecting price data...")
                    return
                avg_price = sum(list(self.price_history)[-5:]) / 5
                current_price = avg_price
                self.is_initializing = False
                logging.info(f"Bot initialization complete, using average price: {current_price:.2f}")
            
            should_update = (
                not self.trailing_enabled or
                self.last_price == 0.0 or
                abs(current_price - self.last_price) >= (self.grid_spacing * current_price * 1.0) or
                time.time() - self.last_adjust_time > config.grid_rebalance_interval
            )
            
            stats = profit_tracker.get_stats()
            force_update = False
            if self.adaptive_enabled:
                if stats["Consecutive Sells"] >= profit_tracker.max_consecutive_trades:
                    self.grid_spacing = max(self.grid_spacing * 0.8, config.base_grid_step_percent / 100)
                    logging.info(f"Forced grid adjustment: Reduced grid spacing to {self.grid_spacing:.2%} due to too many consecutive SELL trades ({stats['Consecutive Sells']})")
                    force_update = True
                    profit_tracker.consecutive_sells = 0
                elif stats["Consecutive Buys"] >= profit_tracker.max_consecutive_trades:
                    self.grid_spacing = max(self.grid_spacing * 0.8, config.base_grid_step_percent / 100)
                    logging.info(f"Forced grid adjustment: Reduced grid spacing to {self.grid_spacing:.2%} due to too many consecutive BUY trades ({stats['Consecutive Buys']})")
                    force_update = True
                    profit_tracker.consecutive_buys = 0
            
            if not (should_update or force_update):
                logging.debug(f"Grid update skipped: should_update={should_update}, force_update={force_update}, "
                              f"price_change={abs(current_price - self.last_price):.2f}, "
                              f"threshold={self.grid_spacing * current_price * 1.0:.2f}, "
                              f"time_since_last_adjust={time.time() - self.last_adjust_time:.2f}s")
                return
                
            balance = (await exchange.get_balance())[0]
            
            if self.adaptive_enabled:
                self.auto_adjust_grid_params(current_price, balance, drawdown)
            
            new_levels = []
            for i in range(self.optimal_levels):
                buy_price = current_price * (1 - (i + 1) * self.grid_spacing)
                sell_price = current_price * (1 + (i + 1) * self.grid_spacing)
                if buy_price > 0 and sell_price > 0:
                    new_levels.append((buy_price, sell_price))
            
            self.grid_levels = new_levels
            self.last_price = current_price
            self.last_adjust_time = time.time()
            
            await self.place_grid_orders(current_price, exchange)
            
            if config.telegram_enabled:
                message = (
                    f"Grid updated at {current_price:.2f}\n"
                    f"Levels: {len(new_levels)}\n"
                    f"Spacing: {self.grid_spacing:.2%}\n"
                    f"Next update: {config.grid_rebalance_interval}s"
                )
                send_telegram_alert(message)
                
        except Exception as e:
            logging.error(f"Grid update failed: {str(e)}")
            send_telegram_alert(f"Grid update failed: {str(e)}")

    async def place_grid_orders(self, current_price: float, exchange: 'ExchangeInterface') -> None:
        open_orders = await exchange.get_open_orders()
        current_open_orders = len(open_orders)
        if current_open_orders >= config.max_open_orders:
            logging.warning(f"Max open orders reached: {current_open_orders}/{config.max_open_orders}")
            return

        for order_id in self.order_ids[:]:
            try:
                await exchange.cancel_order(order_id)
                self.order_ids.remove(order_id)
                logging.info(f"Canceled old order {order_id} before placing new grid orders")
            except Exception as e:
                logging.error(f"Failed to cancel order {order_id}: {str(e)}")
                self.order_ids.remove(order_id)

        filters = await exchange.get_symbol_filters()
        quantity_per_level = config.initial_investment / current_price / max(1, len(self.grid_levels))
        quantity = max(filters['minQty'], min(quantity_per_level, filters['maxQty']))
        step_size = filters.get('stepSize', 0.00001)
        quantity = round(quantity - (quantity % step_size), 6)

        for buy_price, sell_price in self.grid_levels:
            current_open_orders = len(self.order_ids)
            if current_open_orders >= config.max_open_orders:
                logging.warning(f"Max open orders reached: {current_open_orders}/{config.max_open_orders}")
                break
            try:
                if buy_price < current_price:
                    buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price)
                    self.order_ids.append(buy_order['id'])
                    logging.info(f"Placed BUY order at {buy_price:.2f}, qty: {quantity:.6f}")
                if sell_price > current_price:
                    sell_order = await safe_api_call(exchange.place_order, side='SELL', order_type='LIMIT', quantity=quantity, price=sell_price)
                    self.order_ids.append(sell_order['id'])
                    logging.info(f"Placed SELL order at {sell_price:.2f}, qty: {quantity:.6f}")
            except Exception as e:
                logging.error(f"Order placement failed: {str(e)}")
                send_telegram_alert(f"Order placement failed: {str(e)}")

        if not any(buy_price < current_price for buy_price, _ in self.grid_levels):
            buy_price = current_price * 0.995
            if len(self.order_ids) < config.max_open_orders:
                buy_order = await safe_api_call(exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price)
                self.order_ids.append(buy_order['id'])
                logging.info(f"Placed emergency BUY order at {buy_price:.2f}, qty: {quantity:.6f}")

class OrderManager:
    def __init__(self, exchange: 'ExchangeInterface'):
        self.open_orders = []
        self.is_paused = False
        self.exchange = exchange

    async def get_usable_balance(self) -> float:
        try:
            usdt_balance, _ = await safe_api_call(self.exchange.get_balance)
            return usdt_balance
        except Exception as e:
            logging.error(f"Failed to get balance: {str(e)}")
            return 0.0

    async def manage_orders(self, current_price: float, profit_tracker: 'EnhancedProfitTracker', grid_manager: 'SmartGridManager') -> None:
        open_orders = await safe_api_call(self.exchange.get_open_orders, allow_empty=True)
        if len(open_orders) >= config.max_open_orders:
            logging.warning(f"Max open orders reached: {len(open_orders)}/{config.max_open_orders}")
            return

        balance = await self.get_usable_balance()
        if balance < config.initial_investment * 0.1:
            logging.warning(f"Balance too low: {balance:.2f} USDT")
            send_telegram_alert(f"Balance too low: {balance:.2f} USDT")
            self.is_paused = True
            return

        await self.check_and_handle_orders(current_price, profit_tracker, grid_manager)
        profit_tracker.send_daily_summary()

    async def check_and_handle_orders(self, current_price: float, profit_tracker: 'EnhancedProfitTracker', grid_manager: 'SmartGridManager') -> None:
        try:
            orders = await safe_api_call(self.exchange.get_all_orders, allow_empty=True)
            for order in orders:
                if order['status'] == 'FILLED':
                    side = order['side']
                    price = float(order['price'])
                    qty = float(order['executedQty'])
                    fee = qty * price * (config.maker_fee if order['type'] == 'LIMIT' else config.taker_fee)
                    profit = (current_price - price) * qty if side == 'BUY' else (price - current_price) * qty
                    profit_tracker.record_trade(profit, fee, side)
                    self.open_orders = [o for o in self.open_orders if o['orderId'] != order['orderId']]
                    logging.info(f"Filled {side} order at {price:.2f}, profit: {profit:.2f}, fee: {fee:.4f}")
                    send_telegram_alert(f"Filled {side} at {price:.2f}, profit: {profit:.2f}")

                    filters = await self.exchange.get_symbol_filters()
                    quantity = max(filters['minQty'], min(qty, filters['maxQty']))
                    step_size = filters.get('stepSize', 0.00001)
                    quantity = round(quantity - (quantity % step_size), 6)

                    if side == 'SELL':
                        buy_price = price * (1 - grid_manager.grid_spacing)
                        buy_order = await safe_api_call(self.exchange.place_order, side='BUY', order_type='LIMIT', quantity=quantity, price=buy_price)
                        grid_manager.order_ids.append(buy_order['id'])
                        logging.info(f"Placed counter BUY order at {buy_price:.2f}, qty: {quantity:.6f}")
                    elif side == 'BUY':
                        sell_price = price * (1 + grid_manager.grid_spacing)
                        sell_order = await safe_api_call(self.exchange.place_order, side='SELL', order_type='LIMIT', quantity=quantity, price=sell_price)
                        grid_manager.order_ids.append(sell_order['id'])
                        logging.info(f"Placed counter SELL order at {sell_price:.2f}, qty: {quantity:.6f}")
        except Exception as e:
            logging.error(f"Error checking orders: {str(e)}")
            send_telegram_alert(f"Error checking orders: {str(e)}")

# ======== GIAO DIỆN SÀN GIAO DỊCH ========
class ExchangeInterface(ABC):
    def __init__(self, exchange_name: str, api_key: str, api_secret: str):
        self.exchange_name = exchange_name
        self.symbol = config.symbol
        self.price_precision = 2
        self.quantity_precision = 6
        self.client = None

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
    async def get_symbol_filters(self) -> Dict:
        pass

    @abstractmethod
    async def fetch_historical_data(self, timeframe: str, limit: int) -> List[Dict]:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    @abstractmethod
    async def cancel_all_orders(self) -> None:
        pass

    async def health_check(self) -> Dict[str, Any]:
        checks = {}
        start_time = time.time()
        try:
            checks['api'] = await self.get_price() is not None
            checks['balance'] = (await self.get_balance())[0] > 0
            checks['orders'] = await self.get_open_orders() is not None
            checks['performance'] = True
            latency = (time.time() - start_time) * 1000
            status = "healthy" if all(checks.values()) else "unhealthy"
            return {'status': status, 'latency_ms': latency, 'details': checks}
        except Exception as e:
            return {'status': f"unhealthy: {str(e)}", 'latency_ms': None, 'details': checks}

class BinanceExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("binance", api_key, api_secret)
        self.client = ccxt_async.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'testnet': False
        })

    async def get_price(self) -> float:
        ticker = await self.client.fetch_ticker(self.symbol)
        return float(ticker['last'])

    async def get_volume(self) -> float:
        ticker = await self.client.fetch_ticker(self.symbol)
        return float(ticker['quoteVolume'])

    async def get_high_low(self) -> Tuple[float, float]:
        ticker = await self.client.fetch_ticker(self.symbol)
        return float(ticker['high']), float(ticker['low'])

    async def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
        if order_type == 'limit' and price is None:
            raise ValueError("Limit order requires price")
        return await self.client.create_order(
            symbol=self.symbol,
            side=side.lower(),
            type=order_type,
            amount=round(quantity, self.quantity_precision),
            price=round(price, self.price_precision) if price else None
        )

    async def cancel_order(self, order_id: str) -> None:
        await self.client.cancel_order(order_id, self.symbol)

    async def get_open_orders(self) -> List[Dict]:
        orders = await self.client.fetch_open_orders(self.symbol)
        return [{'id': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'type': o['type'].upper(), 'status': 'OPEN'} for o in orders]

    async def get_all_orders(self) -> List[Dict]:
        orders = await self.client.fetch_orders(self.symbol, limit=50)
        return [{'id': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'type': o['type'].upper(), 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN'} for o in orders]

    async def get_balance(self) -> Tuple[float, float]:
        balance = await self.client.fetch_balance()
        usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
        base_asset = self.symbol.split('USDT')[0]
        btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
        return usdt_balance, btc_balance

    async def get_symbol_filters(self) -> Dict:
        markets = await self.client.load_markets()
        market = markets[self.symbol]
        return {
            'minQty': float(market['limits']['amount']['min']),
            'maxQty': float(market['limits']['amount']['max']),
            'stepSize': float(market['precision']['amount'])
        }

    async def fetch_historical_data(self, timeframe: str, limit: int) -> List[Dict]:
        ohlcv = await self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
        return [{'timestamp': o[0], 'open': o[1], 'high': o[2], 'low': o[3], 'close': o[4], 'volume': o[5]} for o in ohlcv]

    async def close(self) -> None:
        if self.client:
            await self.client.close()

    async def cancel_all_orders(self) -> None:
        orders = await self.get_open_orders()
        for order in orders:
            try:
                await self.cancel_order(order['id'])
                logging.info(f"Canceled order {order['id']} at startup/shutdown")
            except Exception as e:
                logging.warning(f"Failed to cancel order {order['id']}: {str(e)}")

# ======== TRẠNG THÁI BOT ========
class BotState:
    def __init__(self, exchange: 'ExchangeInterface', strategy: GridStrategy):
        self.exchange = exchange
        self.tracker = EnhancedProfitTracker()
        self.protection = EnhancedProtectionSystem()
        self.grid_manager = SmartGridManager()
        self.order_manager = OrderManager(exchange)
        self.strategy = strategy
        self.current_price = 0.0
        self.current_volume = 0.0
        self.current_high = 0.0
        self.current_low = 0.0

    async def process_events(self) -> None:
        while True:
            start_time = time.time()
            try:
                self.current_price = await safe_api_call(self.exchange.get_price)
                self.current_volume = await safe_api_call(self.exchange.get_volume)
                self.current_high, self.current_low = await safe_api_call(self.exchange.get_high_low)
                logging.info(f"Current market price: {self.current_price:.2f}, high: {self.current_high:.2f}, low: {self.current_low:.2f}")
                market_data = MarketData(self.current_price, self.current_high, self.current_low, self.current_volume)
                
                self.tracker.price_history.append(market_data.price)
                self.protection.update(market_data.price, market_data.high, market_data.low, market_data.volume)
                if await check_protections(self, self.exchange, market_data.price, market_data.volume):
                    break

                stats = self.tracker.get_stats()
                await self.grid_manager.update_grid(market_data.price, self.exchange, stats["Max Drawdown"], self.tracker)
                signals = await self.strategy.generate_signals(market_data)
                for signal in signals:
                    await self.exchange.place_order(signal.side, "LIMIT", signal.quantity, signal.price)
                await self.order_manager.manage_orders(market_data.price, self.tracker, self.grid_manager)
            except Exception as e:
                logging.error(f"Main loop error: {str(e)}")
                send_telegram_alert(f"Main loop error: {str(e)}")

            elapsed = time.time() - start_time
            sleep_time = max(1, 5 - elapsed)
            await asyncio.sleep(sleep_time)

    async def run_simulated_backtest(self, timeframe: str = '1h', limit: int = 1000) -> Dict[str, float]:
        historical_data = await self.exchange.fetch_historical_data(timeframe, limit)
        initial_balance = config.initial_investment
        balance = initial_balance
        position = 0.0
        trades = []

        for candle in historical_data:
            price = candle['close']
            market_data = MarketData(price, candle['high'], candle['low'], candle['volume'])
            self.tracker.price_history.append(price)
            self.protection.update(price, candle['high'], candle['low'], candle['volume'])

            signals = await self.strategy.generate_signals(market_data)
            for signal in signals:
                if signal.side == "BUY" and balance >= signal.quantity * signal.price:
                    position += signal.quantity
                    balance -= signal.quantity * signal.price
                    fee = signal.quantity * signal.price * config.taker_fee
                    self.tracker.record_trade(0, fee, signal.side)
                    trades.append(("BUY", signal.quantity, signal.price))
                elif signal.side == "SELL" and position >= signal.quantity:
                    position -= signal.quantity
                    balance += signal.quantity * signal.price
                    profit = (signal.price - trades[-1][2]) * signal.quantity if trades else 0
                    fee = signal.quantity * signal.price * config.taker_fee
                    self.tracker.record_trade(profit, fee, signal.side)
                    trades.append(("SELL", signal.quantity, signal.price))

        final_value = balance + position * historical_data[-1]['close']
        stats = self.tracker.get_stats()
        stats["Return"] = (final_value - initial_balance) / initial_balance * 100
        return stats

# ======== HÀM HỖ TRỢ ========
async def safe_api_call(func, allow_empty: bool = False, **kwargs):
    try:
        result = await func(**kwargs)
        if not allow_empty and (result is None or (isinstance(result, (list, dict)) and not result)):
            raise ValueError(f"Empty response from {func.__name__}")
        return result
    except Exception as e:
        logging.error(f"API call failed: {func.__name__}, error: {str(e)}")
        send_telegram_alert(f"API call failed: {func.__name__}, error: {str(e)}")
        return [] if allow_empty else None

async def check_protections(bot: BotState, exchange: ExchangeInterface, price: float, volume: float) -> bool:
    if bot.protection.check_circuit_breaker(price):
        logging.warning("Circuit breaker triggered, pausing bot")
        send_telegram_alert("Circuit breaker triggered, pausing bot")
        await exchange.cancel_all_orders()
        return True

    stop_triggered, tp_triggered, _, _ = bot.protection.trailing_manager.update(price)
    if stop_triggered or tp_triggered:
        logging.info("Trailing stop or take-profit triggered, closing positions")
        send_telegram_alert("Trailing stop or take-profit triggered, closing positions")
        await exchange.cancel_all_orders()
        return True

    return False

# ======== DASHBOARD ========
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Grid Trading Bot Dashboard"),
    dcc.Graph(id='price-chart'),
    dcc.Graph(id='equity-curve'),
    html.Div(id='stats'),
    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0)
])

@app.callback(
    [Output('price-chart', 'figure'),
     Output('equity-curve', 'figure'),
     Output('stats', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n_intervals):
    bot = BotState(exchange, strategy)
    price_data = list(bot.tracker.price_history)
    equity_data = bot.tracker.equity_curve
    stats = bot.tracker.get_stats()

    price_fig = go.Figure(
        data=[go.Scatter(y=price_data, mode='lines', name='Price')],
        layout=go.Layout(title='Price Chart', xaxis_title='Time', yaxis_title='Price')
    )

    equity_fig = go.Figure(
        data=[go.Scatter(y=equity_data, mode='lines', name='Equity')],
        layout=go.Layout(title='Equity Curve', xaxis_title='Trade', yaxis_title='Equity')
    )

    stats_div = html.Div([
        html.P(f"Total Profit: {stats['Total Profit']:.2f}"),
        html.P(f"Trade Count: {stats['Trade Count']}"),
        html.P(f"Win Rate: {stats['Win Rate']:.2f}%"),
        html.P(f"Max Drawdown: {stats['Max Drawdown']:.2f}%"),
        html.P(f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}"),
        html.P(f"Total Fees: {stats['Total Fees']:.4f}"),
        html.P(f"RSI: {stats['RSI']:.2f}")
    ])

    return price_fig, equity_fig, stats_div

# ======== CHẠY BOT ========
async def main():
    global exchange, strategy
    api_key, api_secret = config.exchange_credentials['binance']
    exchange = BinanceExchange(api_key, api_secret)
    strategy = AdaptiveGridStrategy()
    bot = BotState(exchange, strategy)

    logging.info("Canceling all existing orders at startup...")
    await exchange.cancel_all_orders()
    send_telegram_alert("Canceled all existing orders at startup")
    await asyncio.sleep(1)

    health = await exchange.health_check()
    if health['status'] != 'healthy':
        logging.error(f"Exchange health check failed: {health}")
        send_telegram_alert(f"Exchange health check failed: {health}")
        return

    send_telegram_alert("Grid Trading Bot Started")
    await asyncio.sleep(1)
    await bot.process_events()

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
        send_telegram_alert("Bot stopped by user")
        logging.info("Canceling all existing orders at shutdown...")
        loop.run_until_complete(exchange.cancel_all_orders())
        send_telegram_alert("Canceled all existing orders at shutdown")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        send_telegram_alert(f"Unexpected error: {str(e)}")
        logging.info("Canceling all existing orders due to error...")
        loop.run_until_complete(exchange.cancel_all_orders())
        send_telegram_alert("Canceled all existing orders due to error")
    finally:
        loop.run_until_complete(exchange.close())
        loop.close()