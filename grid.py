import time
import math
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import requests
import logging
from typing import List, Dict, Optional, Tuple
from cryptography.fernet import Fernet
import ccxt
import ccxt.async_support as ccxt_async
import dash
from dash import dcc, html, Output, Input
import plotly.graph_objs as go
from concurrent.futures import ThreadPoolExecutor
import cachetools
import psutil
import asyncio
from threading import Thread

# ======== INITIAL SETUP ========
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grid_trading_bot.log'),
        logging.StreamHandler()
    ]
)

api_cache = cachetools.TTLCache(maxsize=100, ttl=300)

# ======== SECURITY (Removed encryption to avoid InvalidToken) ========
def get_encrypted_credentials(exchange: str) -> Tuple[str, str]:
    api_key = os.getenv(f"{exchange.upper()}_API_KEY", "your_api_key")
    api_secret = os.getenv(f"{exchange.upper()}_API_SECRET", "your_api_secret")
    return (api_key, api_secret)

# ======== ADVANCED CONFIG (Fixed min_quantity) ========
class AdvancedConfig:
    def __init__(self):
        self.enabled_exchanges = os.getenv("ENABLED_EXCHANGES", "binance").split(",")
        self.exchange_credentials = {ex: get_encrypted_credentials(ex) for ex in self.enabled_exchanges}
        self.symbol = os.getenv("TRADING_SYMBOL", "BTCUSDT")
        self.initial_investment = float(os.getenv("INITIAL_INVESTMENT", 100))
        self.min_quantity = 0.0005  # Fixed to match BTCUSDT LOT_SIZE on Testnet
        self.max_position = 0.01
        self.base_stop_loss_percent = 2.0
        self.base_take_profit_percent = 2.0
        self.price_drop_threshold = 1.0
        self.adaptive_grid_enabled = True
        self.min_grid_levels = 5
        self.max_grid_levels = 10
        self.base_grid_step_percent = 0.01
        self.grid_rebalance_interval = 300
        self.trailing_stop_enabled = True
        self.trailing_up_activation = 85000
        self.trailing_down_activation = 82000
        self.trailing_tp_enabled = True
        self.trailing_buy_stop_enabled = True
        self.trailing_buy_activation_percent = 1.5
        self.trailing_buy_distance_percent = 1.0
        self.pump_protection_threshold = 0.03
        self.circuit_breaker_threshold = 0.07
        self.circuit_breaker_duration = 360
        self.abnormal_activity_threshold = 3.0
        self.maker_fee = 0.0002
        self.taker_fee = 0.0004
        self.telegram_enabled = bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.api_retry_count = 3
        self.api_retry_delay = 5
        self.max_open_orders = 50
        self.status_update_interval = 600
        self.volatility_window = 100
        self.ma_short_period = 20
        self.ma_long_period = 50
        self.atr_period = 14
        self.rsi_period = 14
        self.bb_period = 20
        self.bb_std_dev = 1.8
        self.min_acceptable_volume = 100
        self.fee_threshold = 0.1
        self.websocket_enabled = True
        self.websocket_reconnect_interval = 60

config = AdvancedConfig()

# ======== BOT STATE ========
class BotState:
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.tracker = EnhancedProfitTracker()
        self.protection = EnhancedProtectionSystem()
        self.current_price = 0.0
        self.grid_manager = EnhancedSmartGridManager()
        self.order_manager = EnhancedOrderManager()

# ======== EXCHANGE INTERFACE ABSTRACTION ========
class ExchangeInterface:
    def __init__(self, exchange_name: str, api_key: str, api_secret: str):
        self.exchange_name = exchange_name
        self.symbol = config.symbol
        self.price_precision = 2
        self.quantity_precision = 6
        self.websocket_connected = False
        self.latest_price = 0.0
        self.latest_volume = 0.0
        self.latest_high = 0.0
        self.latest_low = 0.0

    def get_price(self) -> float:
        raise NotImplementedError

    def get_volume(self) -> float:
        raise NotImplementedError

    def get_high_low(self) -> Tuple[float, float]:
        raise NotImplementedError

    def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    def get_open_orders(self) -> List[Dict]:
        raise NotImplementedError

    def get_all_orders(self) -> List[Dict]:
        raise NotImplementedError

    def get_balance(self) -> Tuple[float, float]:
        raise NotImplementedError

    def get_order_book(self) -> Dict:
        raise NotImplementedError

    def get_symbol_filters(self) -> Dict:
        raise NotImplementedError

    def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        raise NotImplementedError

    async def start_websocket(self, state: BotState):
        raise NotImplementedError

class BinanceExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("binance", api_key, api_secret)
        self.client = ccxt_async.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'testnet': True
        })
        self.ws_client = None

    def get_price(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['last'])

    def get_volume(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['quoteVolume'])

    def get_high_low(self) -> Tuple[float, float]:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['high']), float(ticker['low'])

    def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
        return self.client.create_order(
            symbol=self.symbol,
            side=side.lower(),
            type=order_type,
            amount=round(quantity, self.quantity_precision),
            price=round(price, self.price_precision) if price else None
        )

    def cancel_order(self, order_id: str) -> None:
        self.client.cancel_order(order_id, self.symbol)

    def get_open_orders(self) -> List[Dict]:
        orders = self.client.fetch_open_orders(self.symbol)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]

    def get_all_orders(self) -> List[Dict]:
        orders = self.client.fetch_orders(self.symbol, limit=50)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]

    def get_balance(self) -> Tuple[float, float]:
        balance = self.client.fetch_balance()
        usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
        base_asset = self.symbol.split('USDT')[0]
        btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
        return usdt_balance, btc_balance

    def get_order_book(self) -> Dict:
        return self.client.fetch_order_book(self.symbol, limit=50)

    def get_symbol_filters(self) -> Dict:
        try:
            # Sử dụng asyncio để chạy bất đồng bộ
            loop = asyncio.get_event_loop()
            markets = loop.run_until_complete(self.client.load_markets())
            if self.symbol not in markets:
                logging.error(f"Symbol {self.symbol} not found in markets")
                return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}
                
            market = markets[self.symbol]
            logging.info(f"Successfully fetched LOT_SIZE for {self.symbol}: {market['limits']}")
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Error getting LOT_SIZE info from Binance: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
        return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)

    async def start_websocket(self, state: BotState):
        if not config.websocket_enabled:
            return
            
        try:
            await self.client.load_markets()
        except Exception as e:
            logging.error(f"Error loading markets in WebSocket: {str(e)}")
            self.websocket_connected = False
            return
            
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                state.current_price = float(ticker['last'])
                self.latest_price = state.current_price
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Price update - {state.current_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} error: {str(e)}, retrying in {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

class MEXCExchange(ExchangeInterface):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__("mexc", api_key, api_secret)
        self.client = ccxt_async.mexc({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        self.symbol = self.symbol.replace("USDT", "/USDT")

    def get_price(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['last'])

    def get_volume(self) -> float:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['quoteVolume'])

    def get_high_low(self) -> Tuple[float, float]:
        ticker = self.client.fetch_ticker(self.symbol)
        return float(ticker['high']), float(ticker['low'])

    def place_order(self, side: str, order_type: str, quantity: float, price: float = None) -> Dict:
        order_type = 'limit' if order_type in ['LIMIT', 'LIMIT_MAKER'] else 'market'
        return self.client.create_order(
            symbol=self.symbol,
            side=side.lower(),
            type=order_type,
            amount=round(quantity, self.quantity_precision),
            price=round(price, self.price_precision) if price else None
        )

    def cancel_order(self, order_id: str) -> None:
        self.client.cancel_order(order_id, self.symbol)

    def get_open_orders(self) -> List[Dict]:
        orders = self.client.fetch_open_orders(self.symbol)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled']} for o in orders]

    def get_all_orders(self) -> List[Dict]:
        orders = self.client.fetch_orders(self.symbol, limit=50)
        return [{'orderId': o['id'], 'side': o['side'].upper(), 'price': o['price'], 'origQty': o['amount'], 'executedQty': o['filled'], 'status': 'FILLED' if o['status'] == 'closed' else 'OPEN', 'updateTime': o['timestamp']} for o in orders]

    def get_balance(self) -> Tuple[float, float]:
        balance = self.client.fetch_balance()
        usdt_balance = float(balance['USDT']['free']) if 'USDT' in balance else 0.0
        base_asset = self.symbol.split('/')[0]
        btc_balance = float(balance[base_asset]['free']) if base_asset in balance else 0.0
        return usdt_balance, btc_balance

    def get_order_book(self) -> Dict:
        return self.client.fetch_order_book(self.symbol, limit=50)

    def get_symbol_filters(self) -> Dict:
        try:
            markets = self.client.load_markets()
            market = markets[self.symbol]
            return {
                'minQty': float(market['limits']['amount']['min']),
                'maxQty': float(market['limits']['amount']['max']),
                'stepSize': float(market['precision']['amount'])
            }
        except Exception as e:
            logging.error(f"Error getting LOT_SIZE info from MEXC: {str(e)}")
            return {'minQty': 0.0001, 'maxQty': 1000, 'stepSize': 0.0001}

    def fetch_historical_data(self, timeframe: str, limit: int) -> pd.DataFrame:
        ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
        return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).astype(float)

    async def start_websocket(self, state: BotState):
        if not config.websocket_enabled:
            return
            
        try:
            await self.client.load_markets()
        except Exception as e:
            logging.error(f"Error loading markets: {str(e)}")
            return
            
        while True:
            try:
                ticker = await self.client.watch_ticker(self.symbol)
                state.current_price = float(ticker['last'])
                self.latest_price = state.current_price
                self.websocket_connected = True
                logging.debug(f"WebSocket {self.exchange_name}: Price update - {state.current_price}")
                await asyncio.sleep(1)
            except Exception as e:
                self.websocket_connected = False
                logging.warning(f"WebSocket {self.exchange_name} error: {str(e)}, retrying in {config.websocket_reconnect_interval}s")
                await asyncio.sleep(config.websocket_reconnect_interval)

# ======== MULTI-EXCHANGE MANAGEMENT ========
class ExchangeManager:
    def __init__(self):
        self.exchanges = {}
        for exchange_name in config.enabled_exchanges:
            api_key, api_secret = config.exchange_credentials[exchange_name]
            if exchange_name == "binance":
                self.exchanges[exchange_name] = BinanceExchange(api_key, api_secret)
            elif exchange_name == "mexc":
                self.exchanges[exchange_name] = MEXCExchange(api_key, api_secret)
            else:
                logging.error(f"Exchange {exchange_name} not supported!")
                raise ValueError(f"Unsupported exchange: {exchange_name}")
            logging.info(f"Initialized exchange {exchange_name}")

    def check_price_disparity(self) -> None:
        prices = {name: ex.latest_price if ex.websocket_connected else safe_api_call(ex.get_price) for name, ex in self.exchanges.items()}
        max_diff = max(prices.values()) - min(prices.values())
        if max_diff / min(prices.values()) > 0.01:
            logging.warning(f"Large price disparity: {max_diff:.2f} between {prices}")
            send_telegram_alert(f"⚠️ Large price disparity: {max_diff:.2f} between {prices}")

# ======== IMPROVED ERROR HANDLING ========
def safe_api_call(func, *args, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            cache_key = f"{func.__name__}_{str(args)}_{str(kwargs)}"
            if cache_key in api_cache:
                return api_cache[cache_key]
            result = func(*args, **kwargs)
            api_cache[cache_key] = result
            return result
        except (ccxt.DDoSProtection, ccxt.RateLimitExceeded) as e:
            logging.warning(f"API rate limited: {str(e)}. Waiting {config.api_retry_delay} seconds")
            time.sleep(config.api_retry_delay * (2 ** attempt))
        except ccxt.ExchangeNotAvailable as e:
            logging.error(f"Exchange unavailable: {str(e)}. Pausing for 5 minutes")
            send_telegram_alert(f"⚠️ Exchange unavailable: {str(e)}")
            time.sleep(300)
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            if attempt == max_retries - 1:
                logging.error(f"API call failed after {max_retries} attempts: {str(e)}")
                raise
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Unknown error: {str(e)}")
            raise

# ======== TECHNICAL INDICATOR CALCULATIONS ========
def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
    if len(closes) < period + 1:
        return 0.0
    tr_list = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
    return float(np.mean(tr_list[-period:]))

def calculate_rsi(prices: List[float], period: int) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        gains.append(change if change > 0 else 0)
        losses.append(abs(change) if change < 0 else 0)
    avg_gain = np.mean(gains[-period:]) if gains else 0
    avg_loss = np.mean(losses[-period:]) if losses else 0
    rs = avg_gain / avg_loss if avg_loss != 0 else 0
    return 100 - (100 / (1 + rs))

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
    return (mean_return - risk_free_rate) / std_return * np.sqrt(252) if std_return > 0 else 0.0

def calculate_max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if peak == 0:  # Đã sửa lỗi ZeroDivisionError
            continue
        peak = max(peak, value)
        dd = (peak - value) / peak if peak != 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd * 100

# ======== MAIN BOT EXECUTION ========
def run_bot_for_exchange(exchange: ExchangeInterface, states: Dict[str, BotState]) -> None:
    state = BotState(exchange.exchange_name)
    states[exchange.exchange_name] = state
    
    logging.info(f"🤖 Bot starting on {exchange.exchange_name.upper()}")
    state.order_manager.cancel_all_orders(exchange)
    usdt_balance, btc_balance = check_account_balance(exchange)
    state.current_price = safe_api_call(exchange.get_price)
    state.grid_manager.calculate_adaptive_grid(state.current_price, state.protection.price_history, state.protection.high_history, state.protection.low_history, exchange)
    state.grid_manager.place_grid_orders(state.current_price, exchange)
    
    if config.websocket_enabled:
        async def run_websocket():
            await exchange.start_websocket(state)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        websocket_thread = Thread(target=lambda: loop.run_until_complete(run_websocket()), daemon=True)
        websocket_thread.start()
        logging.info(f"WebSocket thread started for {exchange.exchange_name}")
    
    last_status_update = time.time()
    safe_mode = False
    downtime_start = None
    
    try:
        while True:
            if state.order_manager.is_paused and not safe_mode:
                time.sleep(60)
                state.order_manager.is_paused = False
                continue
            
            try:
                current_price, current_volume, high, low = update_market_data(state, exchange)
                safe_mode, downtime_start = False, None
            except ccxt.ExchangeNotAvailable:
                safe_mode, downtime_start = handle_exchange_downtime(state, downtime_start)
                continue
            
            if safe_mode:
                continue
            
            if check_protections(state, exchange, current_price, current_volume, high, low):
                break
            
            handle_orders(state, exchange, current_price)
            rebalance_grid_if_needed(state, exchange, current_price)
            last_status_update = update_status(state, exchange, last_status_update)
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info(f"⏹ Bot on {exchange.exchange_name} stopped by user")
        send_telegram_alert(f"⏹ Bot on {exchange.exchange_name} stopped by user")
        state.order_manager.cancel_all_orders(exchange)
    except Exception as e:
        logging.error(f"🚨 Critical error on {exchange.exchange_name}: {str(e)}")
        send_telegram_alert(f"🚨 Critical error on {exchange.exchange_name}: {str(e)}")
        state.order_manager.cancel_all_orders(exchange)
    finally:
        if config.websocket_enabled:
            asyncio.run_coroutine_threadsafe(exchange.client.close(), loop).result()
        logging.info(f"Bot on {exchange.exchange_name} has ended")

# ======== MAIN EXECUTION ========
bot_states: Dict[str, BotState] = {}

def main():
    exchange_manager = ExchangeManager()
    
    for exchange_name, exchange in exchange_manager.exchanges.items():
        run_simulated_backtest(exchange)
    
    for exchange_name, exchange in exchange_manager.exchanges.items():
        historical_data = safe_api_call(exchange.fetch_historical_data, timeframe='1h', limit=100)
        backtest_result = backtest_strategy(exchange, historical_data)
        logging.info(f"Real backtest on {exchange_name}: {backtest_result}")
        send_telegram_alert(f"📈 Real backtest on {exchange_name}: PnL={backtest_result['PnL']:.2f}, Trades={backtest_result['Trades']}, Sharpe={backtest_result['Sharpe Ratio']:.2f}, MaxDD={backtest_result['Max Drawdown']:.2f}%, Fees={backtest_result['Total Fees']:.2f}")
    
    cpu_usage = psutil.cpu_percent(interval=1)
    max_workers = min(len(exchange_manager.exchanges), max(1, int(psutil.cpu_count() * (1 - cpu_usage / 100))))
    logging.info(f"Starting bot with {max_workers} workers based on CPU usage: {cpu_usage}%")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda ex: run_bot_for_exchange(ex, bot_states), exchange_manager.exchanges.values())
    
    app.run_server(debug=False, host='0.0.0.0', port=8050)

if __name__ == "__main__":
    main()