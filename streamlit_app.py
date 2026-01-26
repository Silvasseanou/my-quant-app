import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import akshare as ak
import datetime
import time
import json
import os
import re
import requests
import pytz
import smtplib
import datetime
from email.mime.text import MIMEText
from email.header import Header
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from st_supabase_connection import SupabaseConnection

# 修改位置：脚本顶部
def get_bj_time():
    """无论服务器在哪，永远返回北京时间"""
    tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(tz)

# === 全局配置 ===
st.set_page_config(layout="wide", page_title="Elliott Wave Mobile Full (v37.0)", page_icon="🌊", initial_sidebar_state="expanded")

# === 0. 移动端 CSS 适配 (新增) ===
# 让按钮在手机上变宽，更易点击；调整字体大小适配
st.markdown("""
    <style>
    /* 手机端按钮全宽，增加点击区域 */
    .stButton>button {
        width: 100%;
        border-radius: 8px;
        height: 3em;
    }
    /* 调整指标卡片在手机上的显示 */
    div[data-testid="stMetricValue"] {
        font-size: 1.2rem;
    }
    /* 侧边栏调整 */
    section[data-testid="stSidebar"] {
        width: 300px !important;
    }
    </style>
""", unsafe_allow_html=True)

# === 核心常量 & 路径锚定 ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PAPER_TRADING_FILE = os.path.join(SCRIPT_DIR, "ew_otf_portfolio.json")

DEFAULT_CAPITAL = 20000.0
MAX_POSITIONS_DEFAULT = 10 
RISK_PER_TRADE = 0.01 
TRAILING_STOP_PCT = 0.08 
TRAILING_STOP_ACTIVATE = 1.05 
FUND_STOP_LOSS = 0.08 
MAX_SINGLE_POS_WEIGHT = 0.20 
DEAD_MONEY_DAYS = 40 
DEAD_MONEY_THRESHOLD = 0.03 

# 费率设置 (模拟C类)
FEE_C_CLASS = {'buy': 0.0, 'sell_punish': 0.015, 'sell_normal': 0.0}

class NotificationService:
    # 您的专用 Webhook
    FEISHU_HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/31bb5f01-1e8b-4b08-8824-d634b95329e8"

    @staticmethod
    def send_feishu(title, content):
        headers = {'Content-Type': 'application/json'}
        bj_now = get_bj_time().strftime('%Y-%m-%d %H:%M:%S')
        
        # 预警类消息自动显示为红色
        template = "red" if any(x in title+content for x in ["止损", "卖出", "预警", "信号"]) else "blue"
        
        data = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "template": template,
                    "title": {"content": title, "tag": "plain_text"}
                },
                "elements": [
                    {"tag": "div", "text": {"content": content, "tag": "lark_md"}},
                    {"tag": "note", "elements": [{"content": f"时间 (北京): {bj_now}", "tag": "plain_text"}]}
                ]
            }
        }
        try:
            r = requests.post(NotificationService.FEISHU_HOOK, headers=headers, json=data, timeout=5)
            return r.status_code == 200, "发送成功"
        except Exception as e:
            return False, str(e)

# === 行业代表性 ETF 池 (用于轮动雷达) ===
SECTOR_ETF_POOL = [
    {"code": "012885", "name": "💻 科技/AI"}, 
    {"code": "001595", "name": "📈 券商/金融"}, 
    {"code": "003095", "name": "💊 医药/健康"}, 
    {"code": "012414", "name": "🍷 消费/白酒"}, 
    {"code": "002190", "name": "🔋 新能源"}, 
    {"code": "009051", "name": "🛡️ 红利/防御"}, 
    {"code": "011630", "name": "⛏️ 资源/有色"}
]


# === 用户持仓数据 (实盘展示用 - 示例) ===
USER_PORTFOLIO_CONFIG = [
    {"code":"025942","name":"广发新动力混合C","cost":2.2767,"hold":826.23, "hold_7d": 0.0},
    {"code":"004260","name":"德邦稳盈增长灵活配置混合A","cost":1.2839,"hold":3884.19, "hold_7d": 3841.4},
    {"code":"011630","name":"东财有色增强A","cost":2.4796,"hold":2772.07, "hold_7d": 2405.4},
    {"code":"002207","name":"前海开源金银珠宝混合C","cost":2.8347,"hold":1648.5, "hold_7d": 525.39},
    {"code":"012620","name":"嘉实中证软件服务ETF联接C","cost":0.9037,"hold":4454.87, "hold_7d": 2745.44},
    {"code":"018301","name":"华夏消费电子ETF联接C","cost":1.7396,"hold":1000.0, "hold_7d": 1000.0},
    {"code":"025857","name":"华夏中证电网设备主题ETF发起式联接C","cost":1.2605,"hold":3000.0, "hold_7d": 3000.0},
    {"code":"019924","name":"华泰柏瑞中证2000指数增强C","cost":1.8418,"hold":218.96, "hold_7d":218.96},
    {"code":"002861","name":"工银瑞信智能制造股票A","cost":2.9104,"hold":836.38, "hold_7d": 0.0},
    {"code":"005776","name":"中加转型动力灵活配置混合C","cost":4.9843,"hold":1421.59, "hold_7d": 0.0},
    {"code":"010956","name":"天弘中证智能汽车主题指数C","cost":1.1932,"hold":2037.04, "hold_7d": 0.0},
    {"code":"014497","name":"诺安研究优选混合C","cost":1.5973,"hold":1592.89, "hold_7d": 0.0},
    {"code":"001184","name":"易方达新常态灵活配置混合","cost":1.2389,"hold":1275.08, "hold_7d": 0.0}
]

# === 静态优选池 (小池子 - 机器人每日自动扫描) ===
STATIC_OTF_POOL = [
    {"code": "005827", "name": "易方达蓝筹精选"},
    {"code": "003095", "name": "中欧医疗健康A"},
    {"code": "012414", "name": "招商中证白酒C"},
    {"code": "001618", "name": "天弘中证电子C"},
    {"code": "001630", "name": "天弘中证计算机C"},
    {"code": "012620", "name": "嘉实中证软件服务C"},
    {"code": "001071", "name": "华安媒体互联网混合A"},
    {"code": "014855", "name": "嘉实中证半导体C"},
    {"code": "005669", "name": "前海开源公用事业"},
    {"code": "004854", "name": "广发中证全指汽车C"},
    {"code": "010956", "name": "天弘中证智能汽车C"},
    {"code": "002190", "name": "农银新能源主题"},
    {"code": "011630", "name": "东财有色增强A"},
    {"code": "002207", "name": "前海开源金银珠宝C"},
    {"code": "000248", "name": "汇添富中证主要消费"},
    {"code": "001594", "name": "天弘中证银行C"},
    {"code": "001595", "name": "天弘中证证券C"},
    {"code": "007872", "name": "金信稳健策略"},
    {"code": "019924", "name": "华泰柏瑞中证2000增强C"},
    {"code": "000961", "name": "天弘沪深300ETF联接A"}
]

# === 静态宽基无偏池 ===
STATIC_UNBIASED_POOL = [
    # 1. 核心宽基 (大/中/小/微)
    {"code": "000300", "name": "沪深300联接A"},      # 大盘蓝筹
    {"code": "000905", "name": "中证500联接A"},      # 中盘成长
    {"code": "011860", "name": "中证1000联接A"},     # 小盘活跃
    {"code": "019924", "name": "中证2000指数增强C"}, # 微盘 (2023-24神话)
    {"code": "002987", "name": "广发创业板联接A"},   # 创业板 (成长)
    {"code": "012618", "name": "易方达科创50联接A"}, # 科创板 (硬科技)
    {"code": "014350", "name": "华夏北证50成份联接A"}, # 北交所 (高波)

    # 2. 策略/风格 (红利/价值) -> 熊市避风港
    {"code": "009051", "name": "嘉实中证红利低波动C"},
    {"code": "016814", "name": "央企红利ETF联接A"},
    {"code": "501029", "name": "华宝红利基金LOF"},

    # 3. 必选赛道：大科技 (TMT)
    {"code": "012885", "name": "华夏人工智能AI"},          # AI 算力/应用
    {"code": "001630", "name": "天弘中证计算机C"},        # 计算机/软件
    {"code": "001158", "name": "金信智能中国2025"},       # 芯片/半导体
    {"code": "004877", "name": "汇添富全球移动互联"},      # 全球互联网
    {"code": "012419", "name": "华夏中证动漫游戏联接C"},  # 游戏传媒 (高爆发)
    {"code": "001618", "name": "天弘中证电子C"},          # 消费电子

    # 4. 必选赛道：新能源 (风光锂储车)
    {"code": "002190", "name": "农银新能源主题"},
    {"code": "013195", "name": "创金合信新能源汽车C"},
    {"code": "005669", "name": "前海开源公用事业"},        # 绿电/电力
    {"code": "012831", "name": "华夏中证光伏产业联接A"},

    # 5. 必选赛道：大消费/医药
    {"code": "012414", "name": "招商中证白酒指数C"},      # 白酒
    {"code": "000248", "name": "汇添富中证主要消费"},      # 家电/食品
    {"code": "004854", "name": "广发中证全指汽车C"},       # 整车
    {"code": "018301", "name": "华夏消费电子ETF联接C"},
    {"code": "003095", "name": "中欧医疗健康A"},          # 医疗服务 (葛兰)
    {"code": "006228", "name": "中欧医疗创新A"},          # 创新药
    {"code": "004666", "name": "长城中证医药卫生"},       # 中药/全指医药

    # 6. 周期/资源 (通胀交易)
    {"code": "161724", "name": "招商中证煤炭LOF"},        # 煤炭 (高股息)
    {"code": "011630", "name": "东财有色增强A"},          # 有色金属/铜铝
    {"code": "000217", "name": "华安黄金易ETF联接C"},      # 黄金 (避险)
    {"code": "160216", "name": "国泰中证油气LOF"},        # 石油 (QDII)
    {"code": "165520", "name": "信诚中证基建工程LOF"},    # 基建/一带一路

    # 7. 大金融 (牛市旗手/防御)
    {"code": "001595", "name": "天弘中证证券C"},          # 券商
    {"code": "001594", "name": "天弘中证银行C"},          # 银行

    # 8. QDII (全球配置 - 必须要有，防止A股系统性风险)
    {"code": "000834", "name": "大成纳斯达克100A"},        # 美股科技
    {"code": "006321", "name": "中金优选300(标普500)"},    # 美股蓝筹
    {"code": "006127", "name": "华安日经225ETF联接"},      # 日本股市
    {"code": "000614", "name": "华安德国30(QDII)"},        # 欧洲股市
    {"code": "013013", "name": "华夏恒生科技ETF联接A"}     # 港股科技
]

# === 辅助工具函数：统一获取基金池 ===
def get_pool_by_strategy(strategy_name: str) -> List[Dict]:
    """根据 UI 选择的策略名称，返回对应的基金池"""
    if "激进扫描池" in strategy_name or "全市场" in strategy_name:
        st.info("⚠️ 注意：使用【今日全市场Top榜】回测存在幸存者偏差，仅用于验证策略上限。")
        return DataService.get_market_wide_pool()
    else:
        # 默认返回 静态优选池 + 宽基池
        return STATIC_UNBIASED_POOL + STATIC_OTF_POOL

# === 数据结构 ===

@dataclass
class TaxLot:
    date: str
    shares: float
    cost_per_share: float
    fee_paid: float = 0.0

@dataclass
class Holding:
    code: str
    name: str
    lots: List[TaxLot] = field(default_factory=list)
    atr_at_entry: float = 0.0
    stop_loss_price: float = 0.0
    target_price: float = 0.0
    highest_nav: float = 0.0
    wave_pattern: str = "Unknown"
    partial_profit_taken: bool = False
    
    @property
    def total_shares(self): return sum(lot.shares for lot in self.lots)
    @property
    def avg_cost(self): return sum(lot.shares * lot.cost_per_share for lot in self.lots) / self.total_shares if self.total_shares > 0 else 0
    def market_value(self, current_nav): return self.total_shares * current_nav
    
    def get_holding_days(self):
        if not self.lots: return 0
        try:
            buy_date_str = self.lots[0].date.split(' ')[0]
            buy_date = datetime.datetime.strptime(buy_date_str, "%Y-%m-%d").date()
            return (get_bj_time().date() - buy_date).days
        except:
            return 0

# === 基础服务类 ===

class IndicatorEngine:
    @staticmethod
    def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        data = df.copy()
        
        # 基础均线
        data['ema_21'] = data['nav'].ewm(span=21, adjust=False).mean() 
        data['ema_55'] = data['nav'].ewm(span=55, adjust=False).mean() 
        data['ema_89'] = data['nav'].ewm(span=89, adjust=False).mean() 
        data['ema_144'] = data['nav'].ewm(span=144, adjust=False).mean()
        
        # 唐奇安通道
        data['high_20'] = data['nav'].rolling(window=20).max()
        data['low_20'] = data['nav'].rolling(window=20).min()
        
        # MACD
        exp12 = data['nav'].ewm(span=12, adjust=False).mean()
        exp26 = data['nav'].ewm(span=26, adjust=False).mean()
        data['macd'] = exp12 - exp26
        data['signal'] = data['macd'].ewm(span=9, adjust=False).mean()
        data['hist'] = data['macd'] - data['signal']
        
        # RSI
        delta = data['nav'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        data['rsi'] = 100 - (100 / (1 + rs))
        data['rsi_prev'] = data['rsi'].shift(1)
        
        # ATR
        data['tr'] = data['nav'].diff().abs()
        data['atr'] = data['tr'].rolling(window=14).mean()
        
        # AO Indicator
        data['ao'] = data['nav'].rolling(window=5).mean() - data['nav'].rolling(window=34).mean()
        data['ao_prev'] = data['ao'].shift(1)
        
        # Return for Correlation
        data['pct_change'] = data['nav'].pct_change()
        
        return data

class DataService:
    @staticmethod
    @st.cache_data(ttl=3600)
    def fetch_nav_history(code):
        try:
            df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
            if df.empty: return pd.DataFrame()
            df = df.rename(columns={"净值日期": "date", "单位净值": "nav"})
            df['date'] = pd.to_datetime(df['date'])
            df['nav'] = df['nav'].astype(float)
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
            return df
        except Exception as e: 
            return pd.DataFrame()
        
    @staticmethod
    @st.cache_data(ttl=3600*24)
    def get_market_index_trend():
        try:
            df = DataService.fetch_nav_history("000300")
            if df.empty: return 0 
            df = IndicatorEngine.calculate_indicators(df)
            last_price = df['nav'].iloc[-1]
            ema144 = df['ema_144'].iloc[-1]
            if last_price > ema144: return 1
            else: return -1
        except: return 0 

    @staticmethod
    def get_realtime_estimate(code):
        try:
            ts = int(time.time() * 1000)
            url = f"http://fundgz.1234567.com.cn/js/{code}.js?rt={ts}"
            r = requests.get(url, timeout=1)
            if r.status_code == 200:
                txt = r.text
                match = re.findall(r'\((.*?)\)', txt)
                if match:
                    json_str = match[0]
                    data = json.loads(json_str)
                    return float(data['gsz']), float(data['gszzl']), data['gztime']
            return None, None, None
        except: return None, None, None
    
    @staticmethod
    def get_smart_price(code, cost_basis=0.0):
        df = DataService.fetch_nav_history(code)
        est_p, _, _ = DataService.get_realtime_estimate(code)
        
        curr_price = cost_basis 
        today_str = get_bj_time().date().strftime("%Y-%m-%d")
        used_est = False
        
        if not df.empty:
            last_date_str = str(df.index[-1].date())
            if last_date_str == today_str:
                curr_price = df['nav'].iloc[-1]
            elif est_p:
                curr_price = est_p
                used_est = True
            else:
                curr_price = df['nav'].iloc[-1] 
        elif est_p:
            curr_price = est_p
            used_est = True
            
        return curr_price, df, used_est
    
    @staticmethod
    @st.cache_data(ttl=3600*12)
    def get_market_regime():
        """
        全市场温度计：多维度扫描核心指数
        """
        indices = [
            {"code": "000300", "name": "沪深300 (大盘)"},
            {"code": "000905", "name": "中证500 (中盘)"},
            {"code": "002987", "name": "创业板 (成长)"},
            {"code": "001595", "name": "证券 (情绪)"},
            {"code": "012414", "name": "白酒 (消费)"}
        ]
        
        bullish_count = 0
        details = []
        
        for idx in indices:
            df = DataService.fetch_nav_history(idx['code'])
            status = "⚪"
            if not df.empty and len(df) > 100:
                df = IndicatorEngine.calculate_indicators(df)
                last_p = df['nav'].iloc[-1]
                ema89 = df['ema_89'].iloc[-1]
                if last_p > ema89:
                    bullish_count += 1
                    status = "🔴" 
                else:
                    status = "🟢" 
            details.append(f"{status} {idx['name']}")
            
        score = bullish_count / len(indices)
        
        regime = "震荡/分化"
        if score >= 0.8: regime = "🔥 全面牛市"
        elif score >= 0.6: regime = "📈 结构性牛市"
        elif score <= 0.2: regime = "❄️ 极寒/底部"
        
        return {"score": score, "regime": regime, "details": details}

    @staticmethod
    @st.cache_data(ttl=3600*12)
    def get_sector_rankings():
        """
        行业轮动雷达：计算各大赛道代表ETF的动能
        """
        rankings = []
        for s in SECTOR_ETF_POOL:
            df = DataService.fetch_nav_history(s['code'])
            mom = -999
            if len(df) > 20:
                p_now = df['nav'].iloc[-1]
                p_old = df['nav'].iloc[-20] # 20日动能
                mom = (p_now - p_old) / p_old
            rankings.append({"name": s['name'], "mom": mom})
        
        rankings.sort(key=lambda x: x['mom'], reverse=True)
        return rankings
        
    @staticmethod
    @st.cache_data(ttl=3600*24)
    def get_market_wide_pool():
        try:
            df = ak.fund_open_fund_rank_em(symbol="全部")
            mask_type = df['基金简称'].str.contains('债|货币|理财|美元|定开|持有|养老|以太|比特币|港股|QDII', regex=True) == False
            df = df[mask_type]
            df = df.dropna(subset=['近1年'])
            df_top = df.sort_values(by="近6月", ascending=False).head(600)
            
            best_candidates = {}
            for _, row in df_top.iterrows():
                raw_name = row['基金简称']
                code = str(row['基金代码'])
                clean_name = re.sub(r'[A-Z]$', '', raw_name) 
                clean_name = re.sub(r'发起式$', '', clean_name)
                clean_name = re.sub(r'联接$', '', clean_name)
                clean_name = re.sub(r'ETF$', '', clean_name)
                
                is_current_c = raw_name.endswith('C')
                
                if clean_name not in best_candidates:
                    best_candidates[clean_name] = {"code": code, "name": raw_name, "is_c": is_current_c}
                else:
                    existing_is_c = best_candidates[clean_name]['is_c']
                    if is_current_c and not existing_is_c:
                        best_candidates[clean_name] = {"code": code, "name": raw_name, "is_c": True}
            
            pool = []
            for item in best_candidates.values():
                pool.append({"code": item['code'], "name": item['name']})
                if len(pool) >= 200: 
                    break
            return pool
        except Exception as e: 
            return [{"code": "012414", "name": "招商中证白酒指数C"}]

# === 核心逻辑类 ===

class WaveEngine:
    @staticmethod
    def zig_zag(series: pd.Series, deviation_pct=0.05) -> List[Dict]: 
        pivots = [] 
        if len(series) < 10: return []
        direction = 0; last_pivot_idx = 0; last_pivot_val = series.iloc[0]
        dates = series.index
        pivots.append({'idx': 0, 'date': dates[0], 'val': last_pivot_val, 'type': 'start'})
        for i in range(1, len(series)):
            curr_val = series.iloc[i]
            change = (curr_val - last_pivot_val) / last_pivot_val
            if direction == 0:
                if change >= deviation_pct: direction = 1; last_pivot_idx = i; last_pivot_val = curr_val
                elif change <= -deviation_pct: direction = -1; last_pivot_idx = i; last_pivot_val = curr_val
            elif direction == 1:
                if curr_val > last_pivot_val: last_pivot_idx = i; last_pivot_val = curr_val
                elif change <= -deviation_pct: pivots.append({'idx': last_pivot_idx, 'date': dates[last_pivot_idx], 'val': last_pivot_val, 'type': 'high'}); direction = -1; last_pivot_idx = i; last_pivot_val = curr_val
            elif direction == -1:
                if curr_val < last_pivot_val: last_pivot_idx = i; last_pivot_val = curr_val
                elif change >= deviation_pct: pivots.append({'idx': last_pivot_idx, 'date': dates[last_pivot_idx], 'val': last_pivot_val, 'type': 'low'}); direction = 1; last_pivot_idx = i; last_pivot_val = curr_val
        pivots.append({'idx': last_pivot_idx, 'date': dates[last_pivot_idx], 'val': last_pivot_val, 'type': 'high' if direction==1 else 'low'})
        return pivots

    @staticmethod
    def analyze_structure(df_slice: pd.DataFrame, pivots: List[Dict]) -> Dict:
        if len(df_slice) < 100: return {'status': 'Wait', 'score': 0, 'pattern': 'None', 'stop_loss': 0, 'target': 0, 'desc': '数据不足'}
        
        last_nav = df_slice['nav'].iloc[-1]
        
        ao = df_slice['ao']
        ao_curr = ao.iloc[-1]
        ao_prev = ao.iloc[-2]
        
        high_20 = df_slice['high_20'].iloc[-2] 
        low_20 = df_slice['low_20'].iloc[-2]     
        
        ema21 = df_slice['ema_21'].iloc[-1]
        ema55 = df_slice['ema_55'].iloc[-1]
        ema89 = df_slice['ema_89'].iloc[-1]
        
        atr = df_slice['atr'].iloc[-1] if 'atr' in df_slice else last_nav * 0.01
        rsi = df_slice['rsi'].iloc[-1]
        
        result = {'status': 'Wait', 'score': 0, 'pattern': 'None', 'stop_loss': 0, 'target': 0, 'desc': '', 'atr': atr}
        
        # 基础过滤
        if last_nav < ema89 and rsi > 30:
             return {'status': 'Wait', 'score': 0, 'pattern': 'Bearish', 'stop_loss': 0, 'target': 0, 'desc': '价格在生命线(EMA89)之下，观望', 'atr': atr}

        # === 策略 A: 结构性突破 ===
        if last_nav > high_20:
            if ao_curr > 0 and ao_curr > ao_prev: 
                result.update({
                    'status': 'Buy', 
                    'score': 85, 
                    'pattern': 'Structure Breakout', 
                    'desc': '突破20日新高+动能确认 (浪3特征)',
                    'stop_loss': low_20, 
                    'target': last_nav * 1.3
                })
                return result

        # === 策略 B: 趋势回调 ===
        if ema21 > ema55: 
            if last_nav < ema21 and last_nav > ema55:
                if ao_curr > 0:
                    result.update({
                        'status': 'Buy', 
                        'score': 80, 
                        'pattern': 'Trend Pullback', 
                        'desc': '多头趋势回踩支撑',
                        'stop_loss': ema89, 
                        'target': last_nav * 1.2
                    })
                    return result

        # === 策略 C: 逃顶 ===
        if len(df_slice) > 60:
            price_window = df_slice['nav'].iloc[-60:]
            if last_nav >= price_window.max() * 0.99:
                ao_window = df_slice['ao'].iloc[-60:]
                if ao_curr < ao_window.max() * 0.7: 
                     result.update({
                        'status': 'Sell', 
                        'score': -95, 
                        'pattern': 'Wave 5 Divergence', 
                        'desc': '价格新高但动能衰竭 (顶背离)'
                    })

        return result

    @staticmethod
    def calculate_kelly(win_rate, win_loss_ratio):
        """
        计算凯利公式 (Kelly Criterion)
        f = (bp - q) / b
        b = 赔率 (win_loss_ratio)
        p = 胜率 (win_rate)
        q = 败率 (1 - p)
        """
        if win_loss_ratio <= 0: return 0
        f = (win_loss_ratio * win_rate - (1 - win_rate)) / win_loss_ratio
        return max(0, f) # 不允许负值

class RealBacktester:
    def __init__(self, code, start_date, end_date):
        self.code = code
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.df = DataService.fetch_nav_history(code)
        self.df = IndicatorEngine.calculate_indicators(self.df)
    def run(self, initial_capital=DEFAULT_CAPITAL, partial_profit_pct=0.15):
        if self.df.empty: return {"error": "No Data"}
        mask = (self.df.index >= self.start_date) & (self.df.index <= self.end_date)
        test_dates = self.df.loc[mask].index
        capital = initial_capital; shares = 0; equity_curve = []; trades = []; holding_info = None
        progress_bar = st.progress(0); total_days = len(test_dates)
        
        highest_nav_since_buy = 0 
        partial_sold = False
        
        for i, curr_date in enumerate(test_dates):
            if i % 10 == 0: progress_bar.progress(i / total_days, text=f"Simulating: {curr_date.date()}")
            df_slice = self.df.loc[:curr_date]
            if len(df_slice) < 130: continue 
            current_nav = df_slice['nav'].iloc[-1]
            
            signal = WaveEngine.analyze_structure(df_slice, [])
            
            if shares > 0:
                if current_nav > highest_nav_since_buy: highest_nav_since_buy = current_nav
                
                profit_pct = (current_nav - holding_info['cost']) / holding_info['cost']
                # 分批止盈 (Configurable)
                if partial_profit_pct > 0 and profit_pct > partial_profit_pct and not partial_sold:
                    sell_shares = shares * 0.5
                    revenue = sell_shares * current_nav
                    capital += revenue
                    shares -= sell_shares
                    partial_sold = True
                    trades.append({'date': curr_date, 'action': 'SELL (50%)', 'price': current_nav, 'reason': f"Partial Lock (+{partial_profit_pct:.0%})", 'pnl': revenue - (sell_shares * holding_info['cost'])})
                
                drawdown = (highest_nav_since_buy - current_nav) / highest_nav_since_buy
                is_trailing_stop = drawdown > TRAILING_STOP_PCT and (current_nav > holding_info['cost'] * TRAILING_STOP_ACTIVATE) 
                
                exit_reason = ""
                struct_stop = holding_info['stop_loss']
                hard_stop = holding_info['cost'] * (1 - FUND_STOP_LOSS)
                target_stop = holding_info['target']
                actual_stop = max(struct_stop, hard_stop)
                
                if current_nav >= target_stop and target_stop > 0: exit_reason = "Target Profit Hit (Goal)"
                elif current_nav < actual_stop: exit_reason = "Structure Break / Stop"
                elif is_trailing_stop: exit_reason = f"Trailing Stop (-{TRAILING_STOP_PCT:.0%})"
                elif signal['status'] == 'Sell': exit_reason = signal['desc']
                
                if exit_reason:
                    revenue = shares * current_nav
                    capital += revenue; trades.append({'date': curr_date, 'action': 'SELL', 'price': current_nav, 'reason': exit_reason, 'pnl': revenue - (shares * holding_info['cost'])}); shares = 0; holding_info = None; highest_nav_since_buy = 0; partial_sold = False
            
            elif shares == 0:
                if signal['status'] == 'Buy' and signal['score'] >= 80: 
                    cost_amt = capital * 0.2 
                    if capital >= cost_amt:
                        shares = cost_amt / current_nav; capital -= cost_amt
                        holding_info = {'entry_date': curr_date, 'cost': current_nav, 'stop_loss': signal['stop_loss'], 'target': signal['target']}
                        highest_nav_since_buy = current_nav
                        partial_sold = False
                        trades.append({'date': curr_date, 'action': 'BUY', 'price': current_nav, 'shares': shares, 'reason': signal['desc']})
                    
            equity_curve.append({'date': curr_date, 'val': capital + (shares * current_nav)})
        progress_bar.empty()
        
        # Calculate Win Rate & RR for Kelly
        df_tr = pd.DataFrame(trades)
        win_rate = 0
        win_loss_ratio = 0
        if not df_tr.empty:
            wins = df_tr[df_tr['pnl'] > 0]
            losses = df_tr[df_tr['pnl'] <= 0]
            win_rate = len(wins) / len(df_tr)
            avg_win = wins['pnl'].mean() if not wins.empty else 0
            avg_loss = abs(losses['pnl'].mean()) if not losses.empty else 1
            win_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
            
        return {'equity': equity_curve, 'trades': trades, 'win_rate': win_rate, 'rr': win_loss_ratio}



class PortfolioBacktester:
    def __init__(self, pool_codes, start_date, end_date):
        self.pool = pool_codes
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.data_map = {} 
        
    def preload_data(self):
        progress_text = st.empty()
        progress_bar = st.progress(0)
        
        # 1. 去重逻辑
        unique_pool = []
        seen_names = set()
        for fund in self.pool:
            clean_name = re.sub(r'[A-Z]$', '', fund['name'])
            clean_name = re.sub(r'联接$', '', clean_name)
            if clean_name not in seen_names:
                unique_pool.append(fund)
                seen_names.add(clean_name)
        
        codes_to_load = unique_pool if len(unique_pool) < 100 else unique_pool[:100] 
        total = len(codes_to_load)
        
        # 2. 定义单个下载任务函数
        def load_single_fund(fund_info):
            # 获取数据并计算指标
            df = DataService.fetch_nav_history(fund_info['code'])
            if not df.empty:
                return fund_info['code'], IndicatorEngine.calculate_indicators(df)
            return fund_info['code'], None

        # 3. 并行执行
        progress_text.text(f"🚀 正在并行加速下载 {total} 只基金数据...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            # 提交任务
            future_to_fund = {executor.submit(load_single_fund, fund): fund for fund in codes_to_load}
            
            completed_count = 0
            for future in as_completed(future_to_fund):
                code, data = future.result()
                if data is not None:
                    self.data_map[code] = data
                
                completed_count += 1
                progress_bar.progress(completed_count / total)
        
        progress_text.empty()
        progress_bar.empty()

    def run(self, initial_capital=DEFAULT_CAPITAL, max_daily_buys=999, max_holdings=MAX_POSITIONS_DEFAULT, 
            override_start_date=None, monthly_deposit=0, enable_rebalance=False, rebalance_gap=60, 
            enable_dead_money_check=True, partial_profit_pct=0.15, sizing_model="Kelly"):
        if not self.data_map: return {"error": "No data loaded"}
        
        active_start_date = pd.to_datetime(override_start_date) if override_start_date else self.start_date
        
        # === 获取并对齐基准数据 (沪深300) ===
        benchmark_df = DataService.fetch_nav_history("000300")
        
        all_dates = set()
        for df in self.data_map.values():
            mask = (df.index >= active_start_date) & (df.index <= self.end_date)
            all_dates.update(df.loc[mask].index)
        
        # 确保基准数据也在日期范围内
        if not benchmark_df.empty:
            b_mask = (benchmark_df.index >= active_start_date) & (benchmark_df.index <= self.end_date)
            all_dates.update(benchmark_df.loc[b_mask].index)
            
        sorted_dates = sorted(list(all_dates))
        
        capital = initial_capital
        total_principal = initial_capital 
        
        # Benchmark Variables
        bench_shares = 0
        bench_cash = initial_capital
        if not benchmark_df.empty:
            start_price = 0
            # 找到第一个有效价格
            for d in sorted_dates:
                if d in benchmark_df.index:
                    start_price = benchmark_df.loc[d]['nav']
                    break
            if start_price > 0:
                bench_shares = initial_capital / start_price
                bench_cash = 0
        
        holdings = {}
        receivables = [] 
        
        equity_curve = [] 
        drawdown_curve = [] 
        trades = []
        peak_equity = initial_capital
        
        FIXED_BET_SIZE = initial_capital * 0.2 
        SETTLEMENT_DAYS = 1 
        last_month = -1 
        last_rebalance_idx = -999 
        
        # === 动能筛选参数 (与大屏保持一致) ===
        MOMENTUM_WINDOW = 120 # 看过去 120 个交易日
        TOP_N_COUNT = 50   # 严格对齐大屏：只看排名前 50 的强势品种

        for i, curr_date in enumerate(sorted_dates):
            # === 每月定投 (Benchmark 也定投) ===
            if monthly_deposit > 0:
                if curr_date.month != last_month:
                    if last_month != -1: 
                        capital += monthly_deposit
                        total_principal += monthly_deposit
                        trades.append({'date': curr_date, 'action': 'DEPOSIT', 'code': '-', 'name': '工资定投', 'price': 1, 'shares': monthly_deposit, 'reason': '每月自动充值', 'pnl': 0})
                        
                        # Benchmark 定投
                        if not benchmark_df.empty:
                            b_price = benchmark_df.loc[curr_date]['nav'] if curr_date in benchmark_df.index else 0
                            if b_price == 0: # 回溯找最近价格
                                try:
                                    b_idx = benchmark_df.index.get_indexer([curr_date], method='pad')[0]
                                    if b_idx != -1: b_price = benchmark_df.iloc[b_idx]['nav']
                                except: pass
                            
                            if b_price > 0:
                                bench_shares += monthly_deposit / b_price
                            else:
                                bench_cash += monthly_deposit
                                
                    last_month = curr_date.month

            # 1. 资金结算
            unlocked_cash = 0.0
            new_receivables = []
            for r in receivables:
                if curr_date >= r['unlock_date']:
                    unlocked_cash += r['amount']
                else:
                    new_receivables.append(r)
            receivables = new_receivables
            capital += unlocked_cash 
            
            pending_val = sum([r['amount'] for r in receivables])
            
            # 计算持仓市值
            current_hold_val = 0
            for h_code, h in holdings.items():
                df = self.data_map.get(h_code)
                if df is not None and curr_date in df.index:
                    current_hold_val += h['shares'] * df.loc[curr_date]['nav']
                elif df is not None:
                      idx = df.index.get_indexer([curr_date], method='pad')[0]
                      if idx != -1: current_hold_val += h['shares'] * df.iloc[idx]['nav']
            
            current_equity = capital + current_hold_val + pending_val
            daily_buy_count = 0 
            
            # 计算 Benchmark 市值
            bench_val = bench_cash
            if not benchmark_df.empty:
                b_now = benchmark_df.loc[curr_date]['nav'] if curr_date in benchmark_df.index else 0
                if b_now == 0:
                      try:
                        b_idx = benchmark_df.index.get_indexer([curr_date], method='pad')[0]
                        if b_idx != -1: b_now = benchmark_df.iloc[b_idx]['nav']
                      except: pass
                if b_now > 0:
                    bench_val += bench_shares * b_now
            
            # === 2. 强制换股 (使用自定义 rebalance_gap) ===
            rebalance_sells = set()
            
            if enable_rebalance and (i - last_rebalance_idx >= rebalance_gap) and holdings:
                last_rebalance_idx = i
                
                mom_scores_all = []
                for code, df in self.data_map.items():
                    if curr_date not in df.index: continue
                    idx = df.index.get_indexer([curr_date], method='nearest')[0]
                    if idx < MOMENTUM_WINDOW: continue
                    past_slice = df.iloc[idx-MOMENTUM_WINDOW : idx+1]
                    if past_slice.empty: continue
                    start_p = past_slice['nav'].iloc[0]
                    end_p = past_slice['nav'].iloc[-1]
                    mom = (end_p - start_p) / start_p
                    mom_scores_all.append({'code': code, 'mom': mom})
                
                if mom_scores_all:
                    mom_scores_all.sort(key=lambda x: x['mom'], reverse=True)
                    # 动态 cutoff
                    top_n = min(len(mom_scores_all), TOP_N_COUNT)
                    cutoff_val = mom_scores_all[top_n-1]['mom'] if top_n > 0 else -999
                    
                    for h_code in list(holdings.keys()):
                        curr_mom = next((x['mom'] for x in mom_scores_all if x['code'] == h_code), -999)
                        if curr_mom < cutoff_val:
                            info = holdings[h_code]
                            h_curr_nav = info['cost']
                            if curr_date in self.data_map[h_code].index:
                                h_curr_nav = self.data_map[h_code].loc[curr_date]['nav']
                            
                            h_hold_days = (curr_date - pd.to_datetime(info['entry_date'])).days
                            fee_rate = 0.015 if h_hold_days < 7 else 0.0
                            gross = info['shares'] * h_curr_nav
                            net = gross * (1 - fee_rate)
                            
                            trades.append({'date': curr_date, 'action': 'REBALANCE', 'code': h_code, 'name': info['name'], 'price': h_curr_nav, 'reason': f"动能衰竭 (跌出Top50)", 'pnl': net - (info['shares'] * info['cost'])})
                            
                            unlock_dt = curr_date + datetime.timedelta(days=SETTLEMENT_DAYS)
                            receivables.append({'unlock_date': unlock_dt, 'amount': net})
                            del holdings[h_code]
                            rebalance_sells.add(h_code)

            # --- 3. 常规持仓管理 (止盈止损 + 僵尸持仓清理) ---
            for code in list(holdings.keys()):
                if code in rebalance_sells: continue
                info = holdings[code]
                df = self.data_map.get(code)
                if df is None or curr_date not in df.index: continue
                
                df_slice = df.loc[:curr_date]
                if len(df_slice) < 130: continue
                current_nav = df_slice['nav'].iloc[-1]
                
                if current_nav > info['highest_nav']: holdings[code]['highest_nav'] = current_nav
                
                profit_pct = (current_nav - info['cost']) / info['cost']
                hold_days = (curr_date - pd.to_datetime(info['entry_date'])).days
                
                action_type = None; sell_ratio = 0.0; reason = ""
                
                # 分批止盈 (Configurable)
                if partial_profit_pct > 0 and profit_pct > partial_profit_pct and not info.get('partial_sold', False):
                    action_type = "PARTIAL"; sell_ratio = 0.5; reason = f"Partial Lock (+{partial_profit_pct:.0%})"; info['partial_sold'] = True
                
                dd = (info['highest_nav'] - current_nav) / info['highest_nav']
                is_trailing = dd > TRAILING_STOP_PCT and current_nav > info['cost'] * TRAILING_STOP_ACTIVATE
                signal = WaveEngine.analyze_structure(df_slice, [])
                struct_stop = info['stop_loss']
                hard_stop = info['cost'] * (1 - FUND_STOP_LOSS)
                target_stop = info['target']
                
                sell_str = None
                
                if current_nav >= target_stop and target_stop > 0: sell_str = "Target Profit Hit (Goal)"
                elif current_nav < max(struct_stop, hard_stop): sell_str = "Structure Break"
                elif is_trailing: sell_str = "Trailing Stop"
                elif signal['status'] == 'Sell': sell_str = signal['desc']
                
                # === 新增: Dead Money Check (同步模拟盘逻辑) ===
                if enable_dead_money_check and not sell_str:
                    if hold_days > DEAD_MONEY_DAYS and abs(profit_pct) < DEAD_MONEY_THRESHOLD:
                        sell_str = f"Dead Money (Hold > {DEAD_MONEY_DAYS}d, Returns < {DEAD_MONEY_THRESHOLD:.0%})"
                
                if sell_str: action_type = "CLEAR"; sell_ratio = 1.0; reason = sell_str
                
                if action_type:
                    shares_to_sell = info['shares'] * sell_ratio
                    gross = shares_to_sell * current_nav
                    fee_rate = 0.015 if hold_days < 7 else 0.0
                    net = gross * (1 - fee_rate)
                    trades.append({
                        'date': curr_date, 
                        'action': 'SELL' if sell_ratio==1 else 'SELL(50%)', 
                        'code': code, 
                        'name': info['name'], 
                        'price': current_nav, 
                        'reason': f"{reason}", 
                        'pnl': net - (shares_to_sell * info['cost'])
                    })
                    
                    unlock_dt = curr_date + datetime.timedelta(days=SETTLEMENT_DAYS)
                    receivables.append({'unlock_date': unlock_dt, 'amount': net})
                    
                    if action_type == "CLEAR": del holdings[code]
                    else: info['shares'] -= shares_to_sell

            # --- 4. 买入逻辑 (筛选强动能品种) ---
            current_hold_val = 0
            for h_code, h in holdings.items():
                df = self.data_map.get(h_code)
                if df is not None and curr_date in df.index:
                    current_hold_val += h['shares'] * df.loc[curr_date]['nav']
                elif df is not None:
                      idx = df.index.get_indexer([curr_date], method='pad')[0]
                      if idx != -1: current_hold_val += h['shares'] * df.iloc[idx]['nav']
            current_equity = capital + sum([r['amount'] for r in receivables]) + current_hold_val

            if len(holdings) < max_holdings and capital > 2000:
                candidates = []
                held_clean_names = {re.sub(r'[A-Z]$', '', h['name']) for h in holdings.values()}
                
                momentum_scores = []
                for code, df in self.data_map.items():
                    if curr_date not in df.index: continue
                    idx = df.index.get_indexer([curr_date], method='nearest')[0]
                    if idx < MOMENTUM_WINDOW: continue
                    past_slice = df.iloc[idx-MOMENTUM_WINDOW : idx+1]
                    if past_slice.empty: continue
                    start_p = past_slice['nav'].iloc[0]
                    end_p = past_slice['nav'].iloc[-1]
                    mom_score = (end_p - start_p) / start_p
                    momentum_scores.append({'code': code, 'mom': mom_score})
                
                # 按照120日涨幅排序 (与大屏逻辑一致)
                momentum_scores.sort(key=lambda x: x['mom'], reverse=True)
                # 严格对齐大屏：只看排名前 50 的强势品种
                top_n = min(len(momentum_scores), TOP_N_COUNT)
                whitelist_codes = {x['code'] for x in momentum_scores[:top_n]}
                
                for code, df in self.data_map.items():
                    if code in holdings: continue
                    if code not in whitelist_codes: continue 
                    if curr_date not in df.index: continue
                    df_slice = df.loc[:curr_date]
                    if len(df_slice) < 130: continue
                    sig = WaveEngine.analyze_structure(df_slice, [])
                    if sig['status'] == 'Buy' and sig['score'] >= 80:
                         candidates.append((code, df_slice['nav'].iloc[-1], sig))
                
                candidates.sort(key=lambda x: x[2]['score'], reverse=True)
                
                for cand in candidates:
                    if len(holdings) >= max_holdings: break
                    if capital < 2000: break
                    if daily_buy_count >= max_daily_buys: break 
                    
                    code, price, sig = cand
                    name = next((f['name'] for f in self.pool if f['code'] == code), code)
                    clean_name = re.sub(r'[A-Z]$', '', name)
                    if clean_name in held_clean_names: continue 
                    
                    # === 核心修改：统一仓位管理逻辑 (与模拟盘保持一致) ===
                    target_amt = 0
                    
                    if sizing_model == "Kelly":
                        # 模拟盘逻辑: 胜率55%, 赔率2.5 -> 半凯利 (Half Kelly)
                        # f = (2.5 * 0.55 - 0.45) / 2.5 = 0.37
                        # Half = 0.185 (18.5%)
                        k_f = WaveEngine.calculate_kelly(0.55, 2.5) 
                        target_amt = current_equity * (k_f * 0.5)
                        # 激进凯利也需要封顶，避免单只爆仓
                        target_amt = min(target_amt, current_equity * 0.30)
                        
                    elif sizing_model == "ATR":
                        # 模拟盘逻辑: 2倍ATR止损，总账户风险1%
                        atr_val = sig.get('atr', 0)
                        if atr_val > 0:
                            risk_per_trade = current_equity * RISK_PER_TRADE
                            stop_loss_width = 2 * atr_val
                            shares_to_buy = risk_per_trade / stop_loss_width
                            target_amt = shares_to_buy * price
                            target_amt = min(target_amt, current_equity * 0.30) # 封顶
                        else:
                            # ATR计算失败时回退到均衡
                            target_amt = current_equity * (1.0 / max_holdings)

                    elif sizing_model == "Fixed":
                        # 单利模式 (固定金额)
                        target_amt = FIXED_BET_SIZE
                        
                    else: 
                        # Default: "Equal" (均衡复利滚雪球)
                        # 动态均衡: 资金利用率高，但不如Kelly激进
                        position_ratio = min(0.33, 2.0 / max_holdings) 
                        target_amt = current_equity * position_ratio
                    
                    actual_amt = min(capital, target_amt)
                    
                    if actual_amt >= 100: 
                        capital -= actual_amt
                        shares = actual_amt / price
                        holdings[code] = {'shares': shares, 'cost': price, 'stop_loss': sig['stop_loss'], 'target': sig['target'], 'entry_date': curr_date, 'name': name, 'highest_nav': price}
                        trades.append({'date': curr_date, 'action': 'BUY', 'code': code, 'name': name, 'price': price, 'shares': shares, 'reason': f"{sig['desc']} ({sizing_model})"})
                        held_clean_names.add(clean_name)
                        daily_buy_count += 1
            
            if current_equity > peak_equity: peak_equity = current_equity
            dd_pct = (current_equity - peak_equity) / peak_equity if peak_equity > 0 else 0
            
            equity_curve.append({
                'date': curr_date, 
                'val': current_equity, 
                'bench_val': bench_val, # 添加 Benchmark 净值
                'principal': total_principal,
                'drawdown': dd_pct
            })
            drawdown_curve.append({'date': curr_date, 'val': dd_pct})
            
        return {'equity': equity_curve, 'drawdown': drawdown_curve, 'trades': trades}

class PortfolioManager:
    def __init__(self):
        # 1. 初始化 Supabase 连接
        self.conn = st.connection("supabase", type=SupabaseConnection)
        self.user_id = "default_user"  # 对应数据库中的主键 ID
        
        # 2. 从云端加载数据
        self.data = self.load()
        
        # 3. 每次初始化时，尝试结算在途订单（保持逻辑不变）
        self.settle_orders()

    def load(self):
        """从 Supabase 云端读取数据（已移除本地迁移逻辑）"""
        try:
            # 1. 直接查询云端
            res = self.conn.table("trader_storage").select("portfolio_data").eq("id", self.user_id).execute()
            
            if res.data and len(res.data) > 0:
                data = res.data[0]['portfolio_data']
                
                # --- 核心兼容性保持（防止字段缺失报错） ---
                if "pending_orders" not in data: data["pending_orders"] = []
                if "history" not in data: data["history"] = []
                if "capital" not in data: data["capital"] = DEFAULT_CAPITAL
                
                for h in data.get("holdings", []):
                    if "lots" not in h or not h["lots"]:
                        h["lots"] = [{"date": "2020-01-01", "shares": h["shares"], "cost_per_share": h["cost"]}]
                return data
            else:
                # 2. 如果云端完全没数据，则初始化
                default_data = {"capital": DEFAULT_CAPITAL, "holdings": [], "history": [], "pending_orders": []}
                # 这里不需要立即 save，让后续操作触发即可，或者保留 save 以便立即创建记录
                return default_data
                
        except Exception as e:
            st.error(f"☁️ 云端数据读取失败: {e}")
            return {"capital": DEFAULT_CAPITAL, "holdings": [], "history": [], "pending_orders": []}

    def save(self):
        """将当前内存数据同步到 Supabase 云端"""
        try:
            # 使用 upsert：如果 ID 存在则更新，不存在则插入
            self.conn.table("trader_storage").upsert({
                "id": self.user_id,
                "portfolio_data": self.data
            }).execute()
        except Exception as e:
            st.error(f"❌ 云端同步失败: {e}")
        
    def reset(self):
        """重置账户"""
        self.data = {"capital": DEFAULT_CAPITAL, "holdings": [], "history": [], "pending_orders": []}
        self.save() # 这里会自动同步到云端
        return True, "账户已重置为初始状态"

    # --- 以下逻辑方法保持原样，只需确保内部调用的 self.save() 现在是指向云端 ---

    def _get_settlement_date(self, trade_dt):
        """计算确认日期逻辑保持不变"""
        is_after_3pm = trade_dt.hour >= 15
        add_days = 2 if is_after_3pm else 1
        settle_date = trade_dt.date() + datetime.timedelta(days=add_days)
        if settle_date.weekday() == 5: settle_date += datetime.timedelta(days=2) 
        elif settle_date.weekday() == 6: settle_date += datetime.timedelta(days=1) 
        return settle_date

    def settle_orders(self):
        """在途订单结算逻辑保持不变，最后的 self.save() 会触发云端更新"""
        today = get_bj_time().date() # 建议使用之前改好的北京时间函数
        new_pending = []
        settled_count = 0
        
        orders = self.data.get("pending_orders", [])
        if not orders: return 

        for order in orders:
            try:
                settle_date = datetime.datetime.strptime(order['settlement_date'], "%Y-%m-%d").date()
            except:
                settle_date = today

            if today >= settle_date:
                real_nav = 0.0
                correction_msg = ""
                try:
                    df_nav = DataService.fetch_nav_history(order['code'])
                    trade_date_dt = pd.to_datetime(order['date']) 
                    if not df_nav.empty and trade_date_dt in df_nav.index:
                        real_nav = float(df_nav.loc[trade_date_dt]['nav'])
                except: pass

                est_price = order.get('cost', order.get('price', 0.0))
                if real_nav > 0 and abs(real_nav - est_price) > 0.0001:
                    buy_amount = order['amount']
                    order['shares'] = buy_amount / real_nav
                    order['cost'] = real_nav 
                    correction_msg = f" | 净值修正: {est_price:.4f}->{real_nav:.4f}"

                self._add_to_holdings(order)
                settled_count += 1
                self.data['history'].append({
                    "date": get_bj_time().strftime('%Y-%m-%d %H:%M:%S'),
                    "action": "CONFIRM",
                    "code": order['code'],
                    "name": order['name'],
                    "price": order['cost'],
                    "amount": 0,
                    "reason": f"份额确认 (T+1){correction_msg}", 
                    "pnl": 0
                })
            else:
                new_pending.append(order)
        
        if settled_count > 0:
            self.data["pending_orders"] = new_pending
            self.save()
            
    def _add_to_holdings(self, order):
        """添加持仓逻辑保持不变"""
        code = order['code']
        shares = order['shares']
        price = order.get('cost', order.get('price', 0.0))
        date_str = order['date'] 
        
        existing_idx = -1
        for i, h in enumerate(self.data['holdings']):
            if h['code'] == code: existing_idx = i; break
            
        new_lot = {"date": date_str, "shares": shares, "cost_per_share": price}
        
        if existing_idx >= 0:
            existing = self.data['holdings'][existing_idx]
            total_shares_old = existing['shares']
            total_cost_old = existing['cost'] * total_shares_old
            new_total_shares = total_shares_old + shares
            existing['shares'] = new_total_shares
            existing['cost'] = (total_cost_old + (shares * price)) / new_total_shares if new_total_shares > 0 else 0
            if "lots" not in existing: existing["lots"] = []
            existing['lots'].append(new_lot)
        else:
            self.data['holdings'].append({
                "code": code, "name": order['name'], 
                "shares": shares, "cost": price, 
                "date": date_str, 
                "stop_loss": order.get('stop_loss', 0), 
                "target": order.get('target', 0), 
                "partial_sold": False,
                "lots": [new_lot]
            })

    def execute_buy(self, code, name, price, amount, stop_loss, target, reason):
        """买入逻辑保持不变，self.save() 现在会存入云端"""
        if self.data['capital'] < amount: return False, "可用资金不足"
        now = get_bj_time()
        settlement_date = self._get_settlement_date(now)
        shares = amount / price
        self.data['capital'] -= amount
        
        pending_order = {
            "code": code, "name": name, "shares": shares, "cost": price,
            "amount": amount,
            "date": str(now.date()), 
            "time": now.strftime('%H:%M:%S'),
            "settlement_date": str(settlement_date),
            "stop_loss": stop_loss, "target": target
        }
        self.data["pending_orders"].append(pending_order)
        note = "次日确认" if now.hour >= 15 else "T+1确认"
        self.data['history'].append({
            "date": now.strftime('%Y-%m-%d %H:%M:%S'), 
            "action": "BUY_ORDER", 
            "code": code, "name": name,
            "price": price, "amount": amount, 
            "reason": f"{reason} | {note} | 预计 {settlement_date} 到账"
        })
        self.save()
        return True, f"买入申请已提交，等待份额确认 ({settlement_date})"

    def execute_sell(self, code, price, reason, force=False):
        """卖出逻辑：包含惩罚费计算，并将记录同步到云端、流水及飞书"""
        idx = -1
        for i, h in enumerate(self.data['holdings']):
            if h['code'] == code: idx = i; break
        if idx == -1: return False, "持仓中未找到该基金"
        
        h = self.data['holdings'][idx]
        total_shares_to_sell = h['shares'] 
        lots = h.get('lots', [{"date": "2020-01-01", "shares": total_shares_to_sell, "cost_per_share": h['cost']}])
        lots.sort(key=lambda x: x['date']) 
        
        remaining_sell = total_shares_to_sell
        total_revenue, total_fee, total_cost_basis = 0.0, 0.0, 0.0
        today = get_bj_time().date()
        
        temp_lots = [lot.copy() for lot in lots]
        used_lots_indices, penalty_shares = [], 0 
        
        # 1. 核心计算逻辑：处理批次、惩罚费、收益率
        for i, lot in enumerate(temp_lots):
            if remaining_sell <= 0: break
            can_sell = min(remaining_sell, lot['shares'])
            buy_date = datetime.datetime.strptime(lot['date'].split(' ')[0], "%Y-%m-%d").date()
            hold_days = (today - buy_date).days
            fee_rate = 0.015 if hold_days < 7 else 0.0
            if fee_rate > 0: penalty_shares += can_sell
            
            fee_val = (can_sell * price) * fee_rate
            total_revenue += (can_sell * price) - fee_val
            total_fee += fee_val
            total_cost_basis += can_sell * lot['cost_per_share']
            remaining_sell -= can_sell
            if can_sell == lot['shares']: used_lots_indices.append(i) 
            else: temp_lots[i]['shares'] -= can_sell
        
        # 2. 软确认：如果满 7 天惩罚费警告（除非 force=True）
        if penalty_shares > 0 and not force:
             return False, f"检测到 {penalty_shares:.2f} 份持仓不足7天，将收取惩罚费 ¥{total_fee:.2f}。请再次点击卖出确认。"
        
        # 3. 执行资金变动
        self.data['capital'] += total_revenue
        new_lots = [lot for i, lot in enumerate(temp_lots) if i not in used_lots_indices]
        
        # 4. 计算盈亏金额与百分比（用于流水和战报）
        pnl_val = total_revenue - total_cost_basis
        pnl_pct = pnl_val / total_cost_basis if total_cost_basis > 0 else 0
        
        # 5. 更新持仓数据
        if not new_lots: 
            self.data['holdings'].pop(idx)
        else:
            h['lots'], h['shares'] = new_lots, sum(l['shares'] for l in new_lots)
            h['cost'] = sum(l['shares'] * l['cost_per_share'] for l in new_lots) / h['shares']
            self.data['holdings'][idx] = h
            
        # 6. 【重要】记录同步到历史流水
        fee_note = f" (含惩罚费 ¥{total_fee:.2f})" if total_fee > 0 else ""
        self.data['history'].append({
            "date": get_bj_time().strftime('%Y-%m-%d %H:%M:%S'), 
            "action": "SELL", 
            "code": code, 
            "name": h['name'], 
            "price": price, 
            "amount": total_revenue, 
            "reason": f"{reason}{fee_note}", 
            "pnl": pnl_val
        })
        
        # 7. 同步到云端 Supabase
        self.save()

        # 8. 实时反馈：Toast 提示与飞书推送
        st.toast(f"✅ 已记录卖出流水: {h['name']}", icon="📈")
        
        pnl_icon = "🔴" if pnl_val < 0 else "🟢"
        fs_title = f"{pnl_icon} 平仓战报: {h['name']}"
        fs_content = (
            f"**动作**: 卖出平仓\n"
            f"**净值**: {price:.4f}\n"
            f"**金额**: ¥{total_revenue:,.2f}\n"
            f"**盈亏**: ¥{pnl_val:+.2f} ({pnl_pct:+.2%})\n"
            f"**备注**: {reason}{fee_note}"
        )
        NotificationService.send_feishu(fs_title, fs_content)
        
        return True, f"卖出成功，收益 ¥{pnl_val:+.2f} {fee_note}"

    def execute_deposit(self, amount, note="账户入金"):
        """入金逻辑保持不变"""
        if amount <= 0: return False, "金额必须大于0"
        self.data['capital'] += amount
        self.data['history'].append({
            "date": get_bj_time().strftime('%Y-%m-%d %H:%M:%S'), 
            "action": "DEPOSIT", "code": "-", "name": "银行转入", "price": 1.0, 
            "amount": amount, "reason": note, "pnl": 0
        })
        self.save()
        return True, f"成功入金 ¥{amount:,.2f}"

    def execute_withdraw(self, amount, note="账户出金"):
        """出金逻辑：减少可用现金"""
        if amount <= 0: return False, "金额必须大于0"
        if self.data['capital'] < amount: return False, "可用资金不足，无法出金"
        
        self.data['capital'] -= amount
        now = get_bj_time() # 确保使用北京时间
        self.data['history'].append({
            "date": now.strftime('%Y-%m-%d %H:%M:%S'), 
            "action": "WITHDRAW", 
            "code": "-", "name": "转出至银行", "price": 1.0, 
            "amount": amount, "reason": note, "pnl": 0
        })
        self.save() # 同步到云端
        return True, f"成功出金 ¥{amount:,.2f}"
    
    def check_dead_money(self):
        """
        检查僵尸持仓: 持有时间 > 40天 且 收益率在 +/- 3% 之间
        """
        dead_positions = []
        today_dt = get_bj_time().date()
        
        for h in self.data['holdings']:
            # 获取最新价格
            curr_p, _, _ = DataService.get_smart_price(h['code'], h['cost'])
            
            # 计算最早买入日期
            first_buy = today_dt
            if h.get('lots'):
                first_date_str = h['lots'][0]['date'].split(' ')[0]
                first_buy = datetime.datetime.strptime(first_date_str, "%Y-%m-%d").date()
            elif 'date' in h:
                 # 兼容旧数据
                 first_buy = datetime.datetime.strptime(h['date'].split(' ')[0], "%Y-%m-%d").date()
            
            held_days = (today_dt - first_buy).days
            pnl_pct = (curr_p - h['cost']) / h['cost'] if h['cost'] > 0 else 0
            
            if held_days > DEAD_MONEY_DAYS and abs(pnl_pct) < DEAD_MONEY_THRESHOLD:
                dead_positions.append({
                    "code": h['code'],
                    "name": h['name'],
                    "days": held_days,
                    "pnl": pnl_pct,
                    "price": curr_p
                })
        return dead_positions

# === 绘图辅助 ===
def plot_wave_chart(df, pivots, title, cost=None):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['nav'], mode='lines', name='净值', line=dict(color='#2E86C1', width=2)))
    p_dates = [p['date'] for p in pivots]
    p_vals = [p['val'] for p in pivots]
    fig.add_trace(go.Scatter(x=p_dates, y=p_vals, mode='lines+markers', name='波浪结构', line=dict(color='#E67E22', width=2, dash='solid')))
    fig.add_trace(go.Scatter(x=df.index, y=df['high_20'], name='20日新高线', line=dict(color='green', width=1, dash='dot')))
    fig.add_trace(go.Scatter(x=df.index, y=df['low_20'], name='20日新低线', line=dict(color='red', width=1, dash='dot')))
    colors = ['green' if x >= 0 else 'red' for x in df['ao']]
    fig.add_trace(go.Bar(x=df.index, y=df['ao'], name='AO动量', marker_color=colors, opacity=0.3, yaxis='y2'))
    if cost: fig.add_hline(y=cost, line_dash="dash", line_color="red", annotation_text="持仓成本")
    
    # === 新增：斐波那契时间窗 ===
    if len(pivots) > 0:
        last_pivot = pivots[-1]
        start_date = pd.to_datetime(last_pivot['date'])
        fibo_days = [13, 21, 34, 55, 89]
        
        for d in fibo_days:
            f_date = start_date + datetime.timedelta(days=d)
            if f_date <= df.index[-1]: 
                fig.add_vline(x=f_date, line_width=1, line_dash="dot", line_color="purple")
                fig.add_annotation(x=f_date, y=last_pivot['val'], text=f"T+{d}", showarrow=False, yshift=10, font=dict(color="purple", size=10))
            elif f_date <= df.index[-1] + datetime.timedelta(days=30): 
                 fig.add_vline(x=f_date, line_width=1, line_dash="dot", line_color="purple")
    
    fig.update_layout(title=title, height=450, margin=dict(l=0, r=0, t=30, b=0), showlegend=True, yaxis=dict(title="净值"), yaxis2=dict(title="AO", overlaying="y", side="right", showgrid=False))
    return fig

# === UI 部分 ===
def render_dashboard():
    # 移动端CSS优化
    st.markdown("""
        <style>
        .stButton>button {width: 100%; border-radius: 8px;}
        /* 手机端字体适配 */
        @media (max-width: 640px) {
            h1 {font-size: 1.5rem !important;}
            h2 {font-size: 1.25rem !important;}
            .stMetric {padding: 5px !important;}
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("🌊 Elliott Wave OTF Trader (Pro v37.0)")
    
    if 'pm' not in st.session_state:
        st.session_state.pm = PortfolioManager()
    
    pm = st.session_state.pm
    pm.data = pm.load()

    # === 侧边栏: 推送控制 ===
    with st.sidebar:
        st.header("📱 飞书推送中心")
        st.info("Webhook 已锁定，消息将推送到飞书终端。")
        
        # 修正 TypeError：此处 send_feishu 仅传入 2 个参数
        if st.button("🔔 发送测试推送", use_container_width=True):
            ok, msg = NotificationService.send_feishu("连接测试", "您的飞书推送服务已在云端就绪。")
            if ok: st.toast("✅ 发送成功")
            else: st.error(f"❌ 失败: {msg}")
            
        st.divider()

    # === 侧边栏: 原有功能 ===
    with st.sidebar:
        st.header("📡 机会扫描 & 设置")
        
        # 数据新鲜度检查
        test_df = DataService.fetch_nav_history("000300")
        if not test_df.empty:
            last_date_str = str(test_df.index[-1].date())
            today_str = str(get_bj_time().date())
            if last_date_str == today_str:
                st.caption(f"📅 数据更新至: {last_date_str} (✅ 最新)")
            else:
                st.caption(f"📅 数据更新至: {last_date_str} (⏳ 昨收)")
        
        # 市场多维温度计
        regime = DataService.get_market_regime()
        st.markdown(f"### {regime['regime']}")
        st.progress(regime['score'])
        with st.expander("查看多维指标详情", expanded=False):
            for d in regime['details']:
                st.caption(d)
        
        # 行业轮动雷达
        st.divider()
        st.markdown("🧭 **行业轮动雷达 (Sector)**")
        sector_ranks = DataService.get_sector_rankings()
        if sector_ranks:
            top_sector = sector_ranks[0]
            st.success(f"🔥 领涨: **{top_sector['name']}**")
            # 简单的迷你榜单
            df_sec = pd.DataFrame(sector_ranks).set_index('name')
            st.bar_chart(df_sec['mom'], height=150)
        
        st.divider()
        st.markdown("🔧 **策略微调 (Strategy Tweak)**")
        # 新增：分批止盈阈值设置
        profit_lock_pct = st.slider("分批止盈阈值 (Partial Profit)", 0.05, 0.50, 0.15, 0.05, help="当单笔收益达到此比例时，卖出50%仓位锁定胜局。设为0.5以上约等于不止盈。")
        alloc_pct = st.slider("固定仓位模式 (%)", 5, 50, 10, 5, help="仅当不使用 ATR 波动率定仓时生效")
        
        st.caption(f"当前可用资金: ¥{pm.data['capital']:,.0f}")
        
        now = get_bj_time()
        is_trading_day = now.weekday() < 5 
        is_before_3pm = now.hour < 15
        trade_status = "🟢 盘中" if (is_trading_day and is_before_3pm) else "🔴 盘后"
        action_tip = "当日确认" if (is_trading_day and is_before_3pm) else "次日确认"
        st.info(f"时间: {now.strftime('%H:%M')} | {trade_status} -> **{action_tip}**")

        scan_mode = st.radio("扫描范围", ["精选优选池 (稳健)", "全市场Top200 (激进)"], key="scan_mode_radio")
        
        scan_results = []
        if st.button("🚀 开始扫描"):
            if "全市场" in scan_mode: pool = DataService.get_market_wide_pool()
            else: pool = STATIC_OTF_POOL 
                
            if not pool: st.error("无法获取数据"); st.stop()
            progress = st.progress(0); status_text = st.empty()
            scan_list = pool if len(pool) < 100 else pool[:100]
            
            for i, fund in enumerate(scan_list):
                status_text.text(f"Scanning {fund['name']}...")
                progress.progress((i+1)/len(scan_list))
                
                # 使用智能价格获取
                curr_price, df, _ = DataService.get_smart_price(fund['code'])
                if df.empty: continue
                
                est_nav, _, _ = DataService.get_realtime_estimate(fund['code'])
                
                if est_nav:
                    new_row = pd.DataFrame({'nav': [est_nav]}, index=[df.index[-1] + datetime.timedelta(days=1)])
                    df_sim = pd.concat([df, new_row])
                    df_sim = IndicatorEngine.calculate_indicators(df_sim)
                else:
                    df_sim = IndicatorEngine.calculate_indicators(df)
                
                pivots = WaveEngine.zig_zag(df_sim['nav'][-150:]) 
                res = WaveEngine.analyze_structure(df_sim, pivots)
                if res['status'] == 'Buy' and res['score'] >= 80:
                    scan_results.append({**fund, 'price': curr_price, 'res': res})
            
            progress.empty(); status_text.empty()
            scan_results.sort(key=lambda x: x['res']['score'], reverse=True)
            if scan_results:
                st.success(f"发现 {len(scan_results)} 个机会!")
                st.session_state.scan_results = scan_results
                # 构建推送内容
                opp_list = [f"**{r['name']}** ({r['code']}): {r['res']['score']}分 - {r['res']['pattern']}" for r in scan_results[:8]]
                opp_content = "🚀 **全市场扫描 Top 机会展示**:\n" + "\n".join(opp_list)
                
                if st.button("📱 将以上机会推送到飞书", type="primary"):
                    NotificationService.send_feishu(" Elliott Wave 选股机会", opp_content)
                    st.toast("机会列表已发送到飞书")

        if 'scan_results' in st.session_state and st.session_state.scan_results:
            results_to_show = st.session_state.scan_results
            for i, r in enumerate(results_to_show):
                is_holding = False
                clean_target = re.sub(r'[A-Z]$', '', r['name'])
                duplicate_warning = ""
                for h in pm.data['holdings']:
                    if h['code'] == r['code']: is_holding = True
                    clean_exist = re.sub(r'[A-Z]$', '', h['name'])
                    if clean_exist == clean_target: duplicate_warning = " (同名持仓)"
                
                score = r['res']['score']
                rank_icon = "🥇" if i == 0 else ("🥈" if i == 1 else ("🥉" if i == 2 else f"#{i+1}"))
                
                # === 核心逻辑: ATR 波动率定仓法 ===
                # 假设总账户权益（本金+持仓） * 1% 作为单笔风险金
                total_equity = pm.data['capital'] + sum([h['shares'] * h['cost'] for h in pm.data['holdings']])
                risk_amt = total_equity * RISK_PER_TRADE
                atr_val = r['res'].get('atr', 0)
                
                if atr_val > 0:
                    # 止损距离通常设为 2倍 ATR
                    stop_dist = 2 * atr_val
                    # 买入数量 = 风险金 / 每股止损额
                    shares_atr = risk_amt / stop_dist
                    amt_atr = shares_atr * r['price']
                    # 封顶 30% 仓位
                    amt_atr = min(amt_atr, total_equity * 0.3)
                else:
                    amt_atr = 0
                
                amt_fixed = min(pm.data['capital'], pm.data['capital'] * (alloc_pct / 100.0))
                
                # Kelly Calc
                k_f = WaveEngine.calculate_kelly(0.55, 2.5) # 假设优选池胜率55%, 盈亏比2.5
                amt_kelly = pm.data['capital'] * (k_f * 0.5) # Half Kelly
                amt_kelly = min(amt_kelly, pm.data['capital'] * 0.3)

                with st.expander(f"{rank_icon} [{score}分] {r['name']} ({r['code']}){duplicate_warning}"):
                    c1, c2 = st.columns([2, 1])
                    with c1:
                        st.markdown(f"**形态**: {r['res']['pattern']}")
                        st.write(f"止损: {r['res']['stop_loss']:.4f} | 目标: {r['res']['target']:.4f}")
                        if atr_val > 0:
                            st.caption(f"ATR(14): {atr_val:.4f} | 波动定仓建议: ¥{amt_atr:,.0f}")
                    with c2:
                        if is_holding: st.warning("已持仓")
                        else:
                            # 强制使用凯利公式
                            final_amt = amt_kelly
                            final_amt = min(final_amt, pm.data['capital']) # 不能超现金
                            
                            st.metric("建议买入", f"¥{final_amt:,.0f}", help="基于半凯利公式 (Half-Kelly)")
                            
                            def on_buy_click(code, name, price, amount, sl, target, reason):
                                suc, msg = st.session_state.pm.execute_buy(code, name, price, amount, sl, target, reason)
                                if suc:
                                    st.session_state.op_msg = f"✅ {msg}"
                                    st.session_state.op_status = "success"
                                else:
                                    st.session_state.op_msg = f"❌ {msg}"
                                    st.session_state.op_status = "error"
                            st.button("买入", key=f"b_{r['code']}_{int(time.time())}", on_click=on_buy_click,
                                     args=(r['code'], r['name'], r['price'], final_amt, r['res']['stop_loss'], r['res']['target'], r['res']['desc']))

        if 'op_msg' in st.session_state:
            if st.session_state.op_status == 'success': st.success(st.session_state.op_msg)
            else: st.error(st.session_state.op_msg)
            del st.session_state.op_msg

    # === 🚨 每日决策大屏 (Daily Action Center) ===
    st.subheader("🚨 每日决策大屏 (Action Center)")
    action_container = st.container(border=True)
    
    with action_container:
        alerts = []
        bj_now = get_bj_time() # 获取当前北京时间
        
        for h in pm.data['holdings']:
            curr_p, df, used_est = DataService.get_smart_price(h['code'], h['cost'])
            
            # --- 核心逻辑：在推送中加入波浪诊断 ---
            if not df.empty:
                df_calc = IndicatorEngine.calculate_indicators(df)
                pivots = WaveEngine.zig_zag(df_calc['nav'][-100:])
                res = WaveEngine.analyze_structure(df_calc, pivots)
                
                # 1. 检查诊断卖出信号
                if res['status'] == 'Sell':
                    alerts.append(f"🚨 **波浪卖点**: {h['name']} ({res['desc']})")
            
            # 2. 原有的硬件止损检查
            if h.get('stop_loss', 0) > 0 and curr_p < h['stop_loss']:
                alerts.append(f"🔴 **破位止损**: {h['name']} (现价{curr_p:.4f} < 止损{h['stop_loss']:.4f})")
            
            # 3. 移动止损检查
            dd = (h.get('highest_nav', h['cost']) - curr_p) / h.get('highest_nav', h['cost'])
            if dd > TRAILING_STOP_PCT and curr_p > h['cost'] * TRAILING_STOP_ACTIVATE:
                alerts.append(f"🟠 **回撤止损**: {h['name']} (高点回撤{dd:.1%})")

        # 推送按钮执行
        if alerts:
            st.warning(f"发现 {len(alerts)} 条风险项")
            if st.button("📱 立即推送到飞书", use_container_width=True):
                content = "\n".join(alerts)
                NotificationService.send_feishu(" Elliott Wave 持仓预警", content)
                st.success("已推送")
        else:
            st.success(f"✅ 持仓风险扫描安全 ({bj_now.strftime('%H:%M:%S')})")

    # === 主界面 ===
    tab1, tab2, tab3 = st.tabs(["🔍 我的持仓诊断", "💼 模拟交易台 (Pro)", "📊 策略回测"])
    
    with tab1:
        st.subheader("🏥 持仓深度波浪诊断")
        if st.button("刷新诊断"): st.rerun()
        
        for i, item in enumerate(USER_PORTFOLIO_CONFIG):
            # 1. 获取智能价格和历史 df
            curr_price, df, used_est = DataService.get_smart_price(item['code'], item['cost'])
            
            # 数据防御性检查：如果没有 nav 列，跳过
            if df.empty or 'nav' not in df.columns:
                st.error(f"❌ 无法获取 {item['name']} ({item['code']}) 数据，已跳过")
                continue

            # 2. 【核心】自动定位逻辑买入日与持有期最高点
            lookback_df = df.tail(250).copy()
            # 寻找历史上净值最接近成本价的那一天作为疑似入场日
            lookback_df['diff'] = (lookback_df['nav'] - item['cost']).abs()
            inferred_buy_date = lookback_df['diff'].idxmin()
            
            # 定位持有期间最高点
            hold_period_navs = df.loc[inferred_buy_date:]['nav']
            h_highest = hold_period_navs.max()
            h_highest = max(h_highest, curr_price) # 包含今日估值新高
            
            # 3. 计算实时指标
            drawdown_from_peak = (h_highest - curr_price) / h_highest
            pnl_pct = (curr_price - item['cost']) / item['cost']
            
            # 计算僵尸持仓 (持有>40天且波动小)
            hold_days = (get_bj_time().date() - inferred_buy_date.date()).days
            trigger_dead = hold_days > 40 and abs(pnl_pct) < 0.03
            
            # 4. 运行波浪算法
            if used_est:
                new_row = pd.DataFrame({'nav': [curr_price]}, index=[df.index[-1] + datetime.timedelta(days=1)])
                df_calc = pd.concat([df, new_row])
            else:
                df_calc = df
            df_calc = IndicatorEngine.calculate_indicators(df_calc)
            pivots = WaveEngine.zig_zag(df_calc['nav'][-150:]) 
            res = WaveEngine.analyze_structure(df_calc, pivots)
            
            # 5. 【策略判定】移动止盈
            is_profit_target_hit = (h_highest - item['cost']) / item['cost'] >= 0.05
            trigger_trailing = is_profit_target_hit and drawdown_from_peak >= 0.08

            # --- UI 渲染部分 ---
            est_tag = " (实时)" if used_est else ""
            advice_color = "red" if res['status'] == 'Buy' else ("green" if res['status'] == 'Sell' else "grey")
            
            with st.expander(f"{item['name']} | 盈亏: {pnl_pct:+.2%} | 建议: {res['status']}", expanded=True):
                c1, c2, c3 = st.columns([1, 1, 2])
                with c1:
                    st.metric(f"最新估值{est_tag}", f"{curr_price:.4f}", f"{pnl_pct:.2%}")
                    st.metric("持仓成本", f"{item['cost']:.4f}")
                with c2:
                    st.metric("期间最高", f"{h_highest:.4f}")
                    st.metric("高点回撤", f"{drawdown_from_peak:.2%}", delta_color="inverse")
                with c3:
                    if trigger_trailing:
                        st.error(f"🚨 **移动止盈触发**：从最高点回撤达 {drawdown_from_peak:.1%}，建议离场。")
                    if trigger_dead:
                        st.warning(f"💤 **僵尸持仓预警**：已持有约 {hold_days} 天且无波动，建议更换。")
                    
                    st.markdown(f"### 波浪建议: :{advice_color}[{res['status']}]")
                    st.write(f"**分析**: {res['desc']} (疑似入场日: {inferred_buy_date.date()})")
                
                # 绘图
                fig = plot_wave_chart(df_calc.iloc[-120:], pivots, f"{item['name']} 结构图", cost=item['cost'])
                st.plotly_chart(fig, use_container_width=True, key=f"diag_chart_{item['code']}_{i}")

    with tab2:
        st.header("💼 模拟交易台")
        pm.settle_orders() # 处理 T+1
        holdings = pm.data.get('holdings', [])
        pending = pm.data.get('pending_orders', [])
        history = pm.data.get('history', [])

        # === 🔥 1. 实时风险监控 ===
        st.subheader("1. 实时风险监控 (Risk Monitor)")
        monitor_container = st.container()
        sell_alerts = []
        now_str = get_bj_time().strftime("%H:%M:%S")
        
        if holdings:
            with st.spinner(f"正在扫描 {len(holdings)} 个持仓的实时风险..."):
                for h in holdings:
                    # 使用智能价格获取
                    curr_price, df, used_est = DataService.get_smart_price(h['code'], h['cost'])
                    
                    if not df.empty:
                        if used_est:
                            new_row = pd.DataFrame({'nav': [curr_price]}, index=[df.index[-1] + datetime.timedelta(days=1)])
                            df_calc = pd.concat([df, new_row])
                        else: df_calc = df
                        
                        df_calc = IndicatorEngine.calculate_indicators(df_calc)
                        pivots = WaveEngine.zig_zag(df_calc['nav'][-150:]) 
                        res = WaveEngine.analyze_structure(df_calc, pivots)
                        
                        triggers = []
                        struct_stop = h.get('stop_loss', 0)
                        if struct_stop > 0 and curr_price < struct_stop: triggers.append(f"跌破结构 (现价{curr_price:.4f} < 止损{struct_stop:.4f})")
                        hard_stop_price = h['cost'] * (1 - FUND_STOP_LOSS)
                        if curr_price < hard_stop_price: triggers.append(f"触及硬止损 (亏损 > {FUND_STOP_LOSS:.1%})")
                        if curr_price > h.get('highest_nav', 0): h['highest_nav'] = curr_price
                        dd = (h.get('highest_nav', h['cost']) - curr_price) / h.get('highest_nav', h['cost'])
                        if dd > TRAILING_STOP_PCT and curr_price > h['cost'] * TRAILING_STOP_ACTIVATE: triggers.append(f"移动止损触发 (高点回撤 {dd:.2%})")
                        if res['status'] == 'Sell': triggers.append(f"波浪卖点: {res['desc']}")
                        
                        if triggers:
                            sell_alerts.append({"code": h['code'], "name": h['name'], "price": curr_price, "reasons": triggers, "time": now_str})

        with monitor_container:
            if not sell_alerts: st.success(f"✅ 持仓风险扫描安全 ({now_str})", icon="🛡️")
            else:
                st.error(f"🚨 警报：发现 {len(sell_alerts)} 个持仓触发卖出条件！", icon="⚠️")
                for alert in sell_alerts:
                    with st.expander(f"🔴 {alert['name']} ({alert['code']}) - 建议立即卖出!", expanded=True):
                        c_a, c_b = st.columns([3, 1])
                        with c_a:
                            st.markdown(f"**触发时间**: {alert['time']}")
                            st.markdown(f"**触发价格**: {alert['price']:.4f}")
                            for r in alert['reasons']: st.markdown(f"- 💥 **{r}**")
                        with c_b:
                            if st.button("一键清仓", key=f"alert_sell_{alert['code']}"):
                                suc, msg = pm.execute_sell(alert['code'], alert['price'], f"雷达触发: {','.join(alert['reasons'])}", force=True)
                                if suc: st.success("已提交卖出！"); time.sleep(1); st.rerun()

        # === 🔥 2. 组合健康度透视 (Correlation & Momentum) ===
        st.subheader("2. 组合健康度透视 (Portfolio Health)")
        
        col_health_1, col_health_2 = st.columns(2)
        
        with col_health_1:
            with st.expander("🔥 持仓相关性热力图 (避雷针)", expanded=False):
                st.info("💡 检查是否存在“假分散”。如果您买了5只基金，但颜色都是深红色（相关性>0.9），说明风险极度集中！")
                if st.button("生成热力图"):
                    if len(holdings) < 2:
                        st.warning("持仓少于2只，无法计算相关性。")
                    else:
                        with st.spinner("正在下载历史数据计算相关性..."):
                            df_corr_list = []
                            for h in holdings:
                                df_tmp = DataService.fetch_nav_history(h['code'])
                                if not df_tmp.empty:
                                    # 截取最近1年数据
                                    df_tmp = df_tmp.iloc[-250:]
                                    s_pct = df_tmp['nav'].pct_change()
                                    s_pct.name = h['name']
                                    df_corr_list.append(s_pct)
                            
                            if df_corr_list:
                                df_corr_all = pd.concat(df_corr_list, axis=1).dropna()
                                corr_matrix = df_corr_all.corr()
                                
                                fig_corr = go.Figure(data=go.Heatmap(
                                    z=corr_matrix.values,
                                    x=corr_matrix.columns,
                                    y=corr_matrix.index,
                                    colorscale='RdBu_r', # 红=正相关，蓝=负相关
                                    zmin=-1, zmax=1
                                ))
                                fig_corr.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
                                st.plotly_chart(fig_corr, use_container_width=True)
                            else:
                                st.error("数据不足")

        with col_health_2:
            with st.expander("🩺 动能体检 (优胜劣汰)", expanded=False):
                st.info("💡 比较持仓与全市场的120日涨幅。落在左侧红色区域的持仓是“拖油瓶”，建议更换。")
                if st.button("开始体检"):
                    if not holdings:
                        st.warning("暂无持仓。")
                    else:
                        progress_doc = st.progress(0, text="计算市场基准...")
                        pool = DataService.get_market_wide_pool() if "全市场" in scan_mode else STATIC_OTF_POOL
                        market_moms = []
                        # 抽样计算
                        sample_pool = pool[:50]
                        for idx, fund in enumerate(sample_pool):
                            df = DataService.fetch_nav_history(fund['code'])
                            if len(df) > 120:
                                p_now = df['nav'].iloc[-1]; p_old = df['nav'].iloc[-120]
                                market_moms.append((p_now - p_old)/p_old)
                            progress_doc.progress((idx+1)/len(sample_pool) * 0.5)

                        if market_moms:
                            market_moms.sort(reverse=True)
                            top_30_cutoff = market_moms[int(len(market_moms)*0.3)]
                            
                            fig = go.Figure()
                            fig.add_trace(go.Histogram(x=market_moms, name='市场分布', nbinsx=20, marker_color='#90CAF9', opacity=0.6))
                            
                            for idx, h in enumerate(holdings):
                                df = DataService.fetch_nav_history(h['code'])
                                mom = -999
                                if len(df) > 120:
                                    p_now = df['nav'].iloc[-1]; p_old = df['nav'].iloc[-120]
                                    mom = (p_now - p_old)/p_old
                                
                                line_color = '#FF5252' if mom < top_30_cutoff else '#00E676'
                                fig.add_vline(x=mom, line_width=2, line_dash="solid", line_color=line_color)
                                y_pos = 2 + (idx % 3) * 1.5 
                                fig.add_annotation(x=mom, y=y_pos, text=h['name'][:4], showarrow=True, arrowhead=1, ax=20, ay=-20)
                                progress_doc.progress(0.5 + (idx+1)/len(holdings) * 0.5)
                            
                            fig.add_vline(x=top_30_cutoff, line_width=2, line_dash="dash", line_color="orange", annotation_text="Top 30%")
                            fig.update_layout(title="持仓 vs 市场动能", xaxis_title="120日涨幅", yaxis_title="数量", showlegend=False, height=400, margin=dict(l=0, r=0, t=30, b=0))
                            progress_doc.empty()
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.error("数据不足")

        st.divider()
        
        # === 顶部资产数据卡片: 价格修正 ===
        total_hold_val = 0
        for h in holdings:
            curr_p, _, _ = DataService.get_smart_price(h['code'], h['cost'])
            total_hold_val += h['shares'] * curr_p

        pending_val = sum([p['amount'] for p in pending])
        total_assets = pm.data['capital'] + total_hold_val + pending_val
        # === 就在这里插入盈亏计算代码 ===
        initial_capital = 20000.0  # 你的初始本金
        # 加上你所有的历史入金记录
        total_deposited = initial_capital + sum([h['amount'] for h in history if h['action'] == 'DEPOSIT'])
        # 减去你所有的历史出金记录
        total_withdrawn = sum([h['amount'] for h in history if h['action'] == 'WITHDRAW'])
        # 净投入本金
        net_investment = total_deposited - total_withdrawn

        # 账户总盈亏
        total_pnl_val = total_assets - net_investment
        total_pnl_pct = (total_pnl_val / net_investment) if net_investment > 0 else 0

        # --- UI 展示：实战战报 ---
        st.markdown(f"### 🚩 账户实战战报")
        p1, p2, p3 = st.columns(3)
        pnl_color = "red" if total_pnl_val < 0 else "green"
        p1.metric("投入本金", f"¥{net_investment:,.2f}")
        p2.metric("累计盈亏", f"{total_pnl_val:+.2f}", f"{total_pnl_pct:.2%}", delta_color="normal")
        p3.markdown(f"**战果评估**: :{pnl_color}[{ '账户回撤中' if total_pnl_val < 0 else '账户盈利中' }]")
        st.divider()
        # ============================
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("💰 总权益", f"¥{total_assets:,.2f}")
        k2.metric("💵 可用现金", f"¥{pm.data['capital']:,.2f}")
        k3.metric("📈 持仓市值", f"¥{total_hold_val:,.2f}")
        k4.metric("⏳ 在途/冻结", f"¥{pending_val:,.2f}")
        st.divider()

        c_left, c_right = st.columns([1, 2])
        with c_left:
            st.subheader("📊 资产状态")
            hold_vals = []
            for h in holdings:
                curr_p, _, _ = DataService.get_smart_price(h['code'], h['cost'])
                hold_vals.append(h['shares'] * curr_p)

            labels = ['现金', '在途'] + [h['name'] for h in holdings]
            values = [pm.data['capital'], pending_val] + hold_vals
            plot_data = [(l, v) for l, v in zip(labels, values) if v > 0]
            if plot_data:
                fig_pie = go.Figure(data=[go.Pie(labels=[x[0] for x in plot_data], values=[x[1] for x in plot_data], hole=.4)])
                fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=250, showlegend=False)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with st.expander("💰 资金划转 (入/出金)", expanded=False):
                d_col1, d_col2, d_col3 = st.columns([2, 1, 1])
                amt = d_col1.number_input("金额", min_value=0.0, step=1000.0, value=2000.0)
                if d_col2.button("充值", use_container_width=True):
                    suc, msg = pm.execute_deposit(amt)
                    if suc: 
                        st.toast(msg)
                        st.rerun()
            
                if d_col3.button("出金", use_container_width=True):
                    suc, msg = pm.execute_withdraw(amt)
                    if suc: 
                        st.toast(msg)
                        st.rerun()
                    else: 
                        st.error(msg)

            with st.expander("🛠 手动下单", expanded=False):
                 with st.form("manual_trade"):
                    mc = st.text_input("基金代码", placeholder="005827")
                    mn = st.text_input("基金名称", placeholder="易方达蓝筹")
                    mp = st.number_input("参考净值", min_value=0.01, format="%.4f")
                    ma = st.number_input("买入金额", min_value=100.0, step=1000.0)
                    if st.form_submit_button("买入申请"):
                        suc, msg = pm.execute_buy(mc, mn, mp, ma, 0, 0, "手动买入")
                        if suc: st.success(msg); time.sleep(1); st.rerun()
                        else: st.error(msg)
            
            st.markdown("---")
            if st.button("🔴 重置账户 / 清空缓存"):
                pm.reset()
                st.rerun()

        with c_right:
            if pending:
                st.info("⏳ 待确认份额 (Pending)")
                st.dataframe(pd.DataFrame(pending)[['name', 'code', 'amount', 'settlement_date']], use_container_width=True, hide_index=True)
                st.divider()

            st.subheader("📋 持仓管理 (Holdings)")
            if not holdings: st.caption("暂无持仓")
            else:
                for h in holdings:
                    # 使用智能价格获取
                    curr_price, df, used_est = DataService.get_smart_price(h['code'], h['cost'])
                    
                    can_add = False; add_reason = ""
                    res = {'status': 'Unknown', 'desc': '', 'score': 0}
                    
                    if not df.empty:
                        if used_est:
                            new_row = pd.DataFrame({'nav': [curr_price]}, index=[df.index[-1] + datetime.timedelta(days=1)])
                            df_calc = pd.concat([df, new_row])
                        else: df_calc = df
                        df_calc = IndicatorEngine.calculate_indicators(df_calc)
                        pivots = WaveEngine.zig_zag(df_calc['nav'][-150:]) 
                        res = WaveEngine.analyze_structure(df_calc, pivots)
                        pnl_pct = (curr_price - h['cost']) / h['cost']
                        if pnl_pct > 0.03 and res['status'] == 'Buy' and res['score'] >= 80:
                            can_add = True; add_reason = f"浮盈安全垫({pnl_pct:.1%}) + 趋势延续({res['pattern']})"

                    mkt_val = h['shares'] * curr_price
                    pnl_val = mkt_val - (h['shares'] * h['cost'])
                    pnl_pct = (curr_price - h['cost']) / h['cost'] if h['cost'] > 0 else 0
                    
                    lots = h.get('lots', [])
                    penalty_shares = 0
                    today_dt = get_bj_time().date()
                    for lot in lots:
                        l_date = datetime.datetime.strptime(lot['date'].split(' ')[0], "%Y-%m-%d").date()
                        if (today_dt - l_date).days < 7: penalty_shares += lot['shares']
                    
                    with st.container():
                        c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                        c1.markdown(f"**{h['name']}**")
                        c1.caption(f"{h['code']} | 批次: {len(lots)}")
                        if can_add: c1.success(f"🔥 适合加仓: {add_reason}", icon="📈")
                        if penalty_shares > 0: c1.warning(f"⚠️ {penalty_shares:.0f}份不满7天", icon="⏳")
                        c2.metric("持仓市值", f"¥{mkt_val:,.0f}")
                        c3.metric("浮动盈亏", f"{pnl_val:+.0f}", f"{pnl_pct:.2%}")
                        with c4:
                            col_add, col_sell, col_del = st.columns([1, 1, 1])
                            
                            # 1. 加仓按钮
                            add_amt_sugg = total_assets * 0.10
                            add_amt = min(pm.data['capital'], add_amt_sugg)
                            if col_add.button("➕", key=f"add_{h['code']}", help=f"建议加仓 ¥{add_amt:.0f}"):
                                if pm.data['capital'] < 100: st.error("现金不足！")
                                else:
                                    suc, msg = pm.execute_buy(h['code'], h['name'], curr_price, add_amt, res.get('stop_loss', 0), res.get('target', 0), f"浮盈加仓 (+{pnl_pct:.1%})")
                                    if suc: st.toast(f"✅ 已提交！"); time.sleep(1); st.rerun()
                            
                            # 2. 正常卖出按钮 (计入流水，回笼资金)
                            if col_sell.button("💰", key=f"sell_{h['code']}", help="卖出并结算资金到现金账户"):
                                suc, msg = pm.execute_sell(h['code'], curr_price, "手动卖出", force=True)
                                if suc: st.success(msg); time.sleep(1); st.rerun()
                            
                            # 3. 彻底删除按钮 (新增：用于清理录入错误的废数据，不计入流水)
                            if col_del.button("🗑️", key=f"raw_del_{h['code']}", help="彻底删除此记录 (不计入收益，不退回资金)"):
                                # 执行物理删除
                                pm.data['holdings'].pop(holdings.index(h))
                                pm.save() # 同步到云端
                                st.toast(f"🗑️ {h['name']} 已从云端彻底抹除")
                                time.sleep(1)
                                st.rerun()
                        
                        # === 波浪结构分析图 ===
                        with st.expander(f"📉 {h['name']} 走势与结构分析"):
                            if not df.empty:
                                fig = plot_wave_chart(df_calc.iloc[-120:], pivots, f"{h['name']} 结构图", cost=h['cost'])
                                st.plotly_chart(fig, use_container_width=True)
                                st.info(f"波浪分析: {res['desc']}")
                            else:
                                st.warning("数据不足，无法绘图")

                        st.markdown("---")
        
        st.subheader("📜 交易流水")
        if history:
            # 1. 倒序排列，让最新的在上面
            hist_list = list(reversed(history))
            
            # 2. 增加一个“清理所有”按钮（可选）
            if st.button("🧹 清空所有流水记录", type="secondary"):
                pm.data['history'] = []
                pm.save()
                st.rerun()

            st.markdown("---")
            
            # 3. 循环显示每一条流水
            for idx, item in enumerate(hist_list):
                # 真实的索引（因为 hist_list 是倒序的）
                real_idx = len(history) - 1 - idx
                
                hc1, hc2, hc3 = st.columns([2, 5, 1])
                
                # 第一列：动作和时间
                action_color = "red" if "SELL" in item['action'] or "WITHDRAW" in item['action'] else "green"
                hc1.markdown(f"**:{action_color}[{item['action']}]**")
                hc1.caption(f"{item['date'].split(' ')[0]}") # 只显示日期
                
                # 第二列：详细内容
                pnl_str = f" | 盈亏: {item['pnl']:+.2f}" if item.get('pnl', 0) != 0 else ""
                hc2.write(f"**{item['name']}** ({item['code']})")
                hc2.caption(f"价格: {item['price']:.4f} | 金额: ¥{item['amount']:,.2f}{pnl_str}")
                hc2.info(f"备注: {item['reason']}")
                
                # 第三列：删除按钮
                if hc3.button("🗑️", key=f"hist_del_{real_idx}", help="删除此条流水"):
                    pm.data['history'].pop(real_idx)
                    pm.save() # 同步到云端
                    st.toast("流水记录已删除")
                    time.sleep(0.5)
                    st.rerun()
                st.divider()
            
            # 导出功能保持不变
            df_hist = pd.DataFrame(history).iloc[::-1]
            csv = df_hist.to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 导出流水 (CSV)", data=csv, file_name=f"trade_history_{get_bj_time().date()}.csv", mime="text/csv")

    with tab3:
        st.header("📊 策略时光机 & 压力测试")
        mode = st.radio("选择回测模式", ["单只基金 (压力测试)", "时光机 (组合回测)", "⚔️ 策略 PK (控制变量法)", "📅 择时分析 (入场点全景图)"], horizontal=True)
        col_d1, col_d2 = st.columns(2)
        start_d = col_d1.date_input("开始日期", datetime.date(2022, 1, 1))
        end_d = col_d2.date_input("结束日期", get_bj_time().date())

        if "PK" in mode:
            st.subheader("⚔️ 策略竞技场")
            pk_type = st.radio("⚔️ 你想对比什么？", 
                             ["🅰️ 数量限制 PK: 【宽分散(Max=10)】 vs 【强集中(Max=3)】", 
                              "🅱️ 资金模式 PK: 【复利滚雪球】 vs 【单利固定金额】"])
            
            # === 核心修改: 使用函数获取池子 ===
            pool_choice = st.radio("📡 选择回测股票池", 
                        ["🧪 科学严谨池 (各行业龙头+宽基)", 
                         "🎯 激进扫描池 (今日全市场Top)"],
                        key="pool_choice_pk")

            if st.button("🔥 开始对决"):
                status_box = st.status("正在安排对决...", expanded=True)
                
                # 调用统一函数
                pool = get_pool_by_strategy(pool_choice)
                
                pbt = PortfolioBacktester(pool, str(start_d), str(end_d))
                pbt.preload_data()
                res_A = {}; res_B = {}
                label_A = ""; label_B = ""
                
                if "数量限制" in pk_type:
                    label_A = "红方: 宽分散 (Max=10)"; status_box.write(f"正在运行 {label_A}...")
                    res_A = pbt.run(initial_capital=DEFAULT_CAPITAL, max_daily_buys=999, max_holdings=10, enable_rebalance=True, partial_profit_pct=profit_lock_pct, sizing_model="Kelly")
                    label_B = "蓝方: 强集中 (Max=3)"; status_box.write(f"正在运行 {label_B}...")
                    res_B = pbt.run(initial_capital=DEFAULT_CAPITAL, max_daily_buys=3, max_holdings=3, enable_rebalance=True, partial_profit_pct=profit_lock_pct, sizing_model="Kelly")
                    
                elif "资金模式" in pk_type:
                    label_A = "红方: 复利 (Kelly)"; status_box.write(f"正在运行 {label_A}...")
                    res_A = pbt.run(initial_capital=DEFAULT_CAPITAL, max_daily_buys=3, max_holdings=MAX_POSITIONS_DEFAULT, enable_rebalance=True, partial_profit_pct=profit_lock_pct, sizing_model="Kelly")
                    label_B = "蓝方: 单利 (Fixed)"; status_box.write(f"正在运行 {label_B}...")
                    res_B = pbt.run(initial_capital=DEFAULT_CAPITAL, max_daily_buys=3, max_holdings=MAX_POSITIONS_DEFAULT, enable_rebalance=True, partial_profit_pct=profit_lock_pct, sizing_model="Fixed")

                status_box.update(label="对决完成", state="complete", expanded=False)
                
                # 绘图逻辑
                data_dict = {}
                if res_A.get('equity'): data_dict[label_A] = pd.DataFrame(res_A['equity']).set_index('date')['val']
                if res_B.get('equity'): data_dict[label_B] = pd.DataFrame(res_B['equity']).set_index('date')['val']
                
                if data_dict:
                    df_compare = pd.DataFrame(data_dict)
                    st.subheader("📈 资金曲线对比"); st.line_chart(df_compare)
                    
                    # 汇总表
                    stats = []
                    for lbl, res in zip([label_A, label_B], [res_A, res_B]):
                        if not res: continue
                        eq = pd.DataFrame(res['equity'])
                        tr = pd.DataFrame(res['trades'])
                        dd = pd.DataFrame(res['drawdown'])
                        ret = (eq['val'].iloc[-1] / DEFAULT_CAPITAL) - 1
                        mdd = dd['val'].min()
                        win = len(tr[tr['pnl']>0]) / len(tr) if not tr.empty else 0
                        stats.append({"策略": lbl, "总收益": f"{ret:.2%}", "最大回撤": f"{mdd:.2%}", "胜率": f"{win:.1%}", "交易数": len(tr)})
                    
                    st.dataframe(pd.DataFrame(stats), use_container_width=True)

        elif "择时分析" in mode:
            st.markdown("<div style='background-color: #e3f2fd; padding: 10px; border-radius: 5px; margin-bottom: 20px;'><strong>ℹ️ 功能说明：平行宇宙测试</strong><br>此模式将模拟从过去几年的<strong>不同日期</strong>入场，一直持有到今天。</div>", unsafe_allow_html=True)
            col_t1, col_t2 = st.columns(2)
            step_days = col_t1.slider("采样间隔 (天)", 7, 60, 15)
            max_daily = col_t2.slider("策略限制 (每日买入上限)", 1, 10, 3)
            
            c_s1, c_s2 = st.columns(2)
            enable_deposit = c_s1.checkbox("包含每月定投 (+2000)", value=False)
            
            deposit_amt = 2000 if enable_deposit else 0
            
            # === 核心修改: 使用函数获取池子 ===
            pool_choice = st.radio("📡 选择回测股票池", 
                                ["🧪 科学严谨池 (各行业龙头+宽基)", 
                                    "🎯 激进扫描池 (今日全市场Top)"],
                                key="pool_choice_timing")
            
            if st.button("🚀 开始全景计算"):
                # 调用统一函数
                pool = get_pool_by_strategy(pool_choice)

                pbt = PortfolioBacktester(pool, str(start_d), str(end_d))
                with st.status("正在初始化时光机...", expanded=True) as status:
                    status.write("正在预加载全市场数据 (Parallel Preloading)...")
                    pbt.preload_data()
                    start_dt = pd.to_datetime(start_d); end_dt = pd.to_datetime(end_d)
                    test_points = []; curr = start_dt
                    while curr < end_dt - datetime.timedelta(days=90): test_points.append(curr); curr += datetime.timedelta(days=step_days)
                    
                    if not test_points: status.update(label="错误：时间范围太短", state="error"); st.error("选择的时间范围太短，无法生成足够的采样点。"); st.stop()
                    
                    results = []
                    progress_bar = st.progress(0)
                    status.write(f"即将模拟 {len(test_points)} 个平行宇宙...")
                    
                    for i, test_start in enumerate(test_points):
                        pct = (i + 1) / len(test_points); progress_bar.progress(pct, text=f"正在模拟入场: {test_start.date()} ({i+1}/{len(test_points)})")
                        
                        # 修正: 默认换股周期改为 60天 (稳健)
                        res = pbt.run(initial_capital=DEFAULT_CAPITAL, max_daily_buys=max_daily, monthly_deposit=deposit_amt, override_start_date=test_start, enable_rebalance=True, rebalance_gap=60, partial_profit_pct=profit_lock_pct, sizing_model="Kelly")
                        
                        if "equity" in res and res['equity']:
                            df_eq = pd.DataFrame(res['equity']); df_tr = pd.DataFrame(res['trades']); df_dd = pd.DataFrame(res['drawdown'])
                            if not df_eq.empty:
                                final_val = df_eq['val'].iloc[-1]; final_principal = df_eq['principal'].iloc[-1]
                                total_ret = (final_val - final_principal) / final_principal if final_principal > 0 else 0
                                max_dd = df_dd['val'].min()
                                win_rate = len(df_tr[df_tr['pnl']>0]) / len(df_tr) if not df_tr.empty else 0
                                results.append({"入场日期": test_start, "持有至今收益率": total_ret, "经历最大回撤": max_dd, "交易胜率": win_rate})
                    
                    progress_bar.empty(); status.update(label="全景计算完成！", state="complete", expanded=False)
                
                if results:
                    df_res = pd.DataFrame(results).set_index("入场日期")
                    st.success(f"✅ 模拟完成！共测试了 {len(results)} 个不同的入场时机。")
                    
                    st.subheader("1. 收益率全景 (Yield Curve)"); st.line_chart(df_res['持有至今收益率'])
                    c1, c2 = st.columns(2)
                    with c1: st.subheader("2. 风险分布 (Drawdown)"); st.area_chart(df_res['经历最大回撤'], color="#FF5252")
                    with c2: st.subheader("3. 胜率稳定性 (Win Rate)"); st.line_chart(df_res['交易胜率'], color="#00E676")
                    
                    with st.expander("查看详细数据表"): st.dataframe(df_res.style.format("{:.2%}"), use_container_width=True)

        elif "单只基金" in mode:
            code = st.text_input("代码", "005827")
            if st.button("回测"):
                bt = RealBacktester(code, str(start_d), str(end_d)); res = bt.run(partial_profit_pct=profit_lock_pct)
                if "equity" in res: st.line_chart(pd.DataFrame(res['equity']).set_index('date')['val']); st.dataframe(pd.DataFrame(res['trades']))

        else:
            # 普通时光机模式
            col_s1, col_s2 = st.columns(2)
            monthly_add = col_s1.slider("💰 每月工资定投 (0为不开启)", 0, 10000, 2000, step=1000)
            
            # === 锁定: Kelly 模式 ===
            col_s2.markdown("⚖️ **仓位模型**: :orange[凯利公式 (Kelly Criterion)]")
            run_sizing_mode = "Kelly"
            
            use_rebal = col_s2.checkbox("开启强制换股 (汰弱留强)", value=True) 
            
            # === 核心修改: 增加池子选择 ===
            pool_choice = st.radio("📡 选择回测股票池", 
                                ["🧪 科学严谨池 (各行业龙头+宽基, 避免幸存者偏差)", 
                                    "🎯 激进扫描池 (今日全市场Top, 仅验证上限)"],
                                key="pool_choice_simple") # Added key
            
            if st.button("🚀 启动模拟"):
                # 调用统一函数
                pool = get_pool_by_strategy(pool_choice)

                pbt = PortfolioBacktester(pool, str(start_d), str(end_d)); pbt.preload_data()
                res = pbt.run(initial_capital=DEFAULT_CAPITAL, max_daily_buys=3, monthly_deposit=monthly_add, enable_rebalance=use_rebal, partial_profit_pct=profit_lock_pct, sizing_model=run_sizing_mode)
                
                if "equity" in res and res['equity']:
                    df = pd.DataFrame(res['equity'])
                    final_val = df['val'].iloc[-1]; final_principal = df['principal'].iloc[-1]
                    total_ret = (final_val - final_principal) / final_principal if final_principal > 0 else 0
                    
                    # === 增加 Sharpe & 年化 ===
                    df['pct_change'] = df['val'].pct_change()
                    sharpe = (df['pct_change'].mean() / df['pct_change'].std()) * np.sqrt(252) if df['pct_change'].std() != 0 else 0
                    annual_ret = (total_ret + 1) ** (252 / len(df)) - 1 if len(df) > 0 else 0
                    
                    # === 计算 Alpha (vs HS300) ===
                    alpha_val = 0
                    if 'bench_val' in df.columns:
                        bench_ret = (df['bench_val'].iloc[-1] - df['bench_val'].iloc[0]) / df['bench_val'].iloc[0] if df['bench_val'].iloc[0] > 0 else 0
                        alpha_val = total_ret - bench_ret
                    
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("总资产", f"¥{final_val:,.0f}")
                    c2.metric("累计本金", f"¥{final_principal:,.0f}")
                    c3.metric("总收益率", f"{total_ret:.2%}")
                    c4.metric("最大回撤", f"{pd.DataFrame(res['drawdown'])['val'].min():.2%}")
                    
                    c5, c6, c7 = st.columns(3)
                    c5.metric("📈 年化收益 (CAGR)", f"{annual_ret:.2%}")
                    c6.metric("⚖️ 夏普比率 (Sharpe)", f"{sharpe:.2f}")
                    c7.metric("🦁 超额收益 (Alpha)", f"{alpha_val:.2%}", help="策略收益 - 沪深300同期收益")
                    
                    # === 1. 月度收益热力图 (Heatmap) ===
                    st.subheader("📅 月度收益热力图 (Monthly Heatmap)")
                    df['year'] = df['date'].dt.year
                    df['month'] = df['date'].dt.month
                    # 计算每月收益
                    # 修正的月度收益计算
                    df_monthly = df.set_index('date').resample('M')['val'].last().pct_change().reset_index()
                    df_monthly['year'] = df_monthly['date'].dt.year
                    df_monthly['month'] = df_monthly['date'].dt.month
                    pivot_table = df_monthly.pivot(index='year', columns='month', values='val')
                    
                    fig_heat = go.Figure(data=go.Heatmap(
                        z=pivot_table.values,
                        x=[f"{i}月" for i in range(1, 13)],
                        y=pivot_table.index,
                        colorscale='RdYlGn', 
                        zmid=0,
                        text=np.around(pivot_table.values * 100, 1),
                        texttemplate="%{text}%"
                    ))
                    fig_heat.update_layout(height=400, margin=dict(t=0, l=0, r=0, b=0))
                    st.plotly_chart(fig_heat, use_container_width=True)

                    # === 2. 潜水图 (Underwater Plot) ===
                    c_uw1, c_uw2 = st.columns(2)
                    with c_uw1:
                        st.subheader("🌊 潜水图 (回撤深度 & 时长)")
                        df_dd = pd.DataFrame(res['drawdown']).set_index('date')
                        fig_dd = go.Figure()
                        fig_dd.add_trace(go.Scatter(x=df_dd.index, y=df_dd['val'], fill='tozeroy', line=dict(color='red', width=1)))
                        fig_dd.update_yaxes(title="回撤幅度", tickformat='.1%')
                        fig_dd.update_layout(height=350, margin=dict(t=0, l=0, r=0, b=0), showlegend=False)
                        st.plotly_chart(fig_dd, use_container_width=True)
                    
                    # === 3. 盈亏分布 (PnL Distribution) ===
                    with c_uw2:
                        st.subheader("📊 盈亏分布直方图")
                        trades_df = pd.DataFrame(res['trades'])
                        if not trades_df.empty:
                            # 估算每笔交易收益率
                            trades_pnl = trades_df[trades_df['pnl'] != 0]['pnl']
                            fig_hist = go.Figure(data=[go.Histogram(x=trades_pnl, nbinsx=30, marker_color='#42A5F5')])
                            fig_hist.update_layout(height=350, margin=dict(t=0, l=0, r=0, b=0), xaxis_title="单笔盈亏金额")
                            st.plotly_chart(fig_hist, use_container_width=True)

                    st.subheader("📈 策略表现 vs 市场基准 (Alpha)"); 
                    chart_cols = {'val': '我的策略', 'principal': '本金投入'}
                    if 'bench_val' in df.columns: chart_cols['bench_val'] = '沪深300基准'
                    chart_df = df.set_index('date')[list(chart_cols.keys())].rename(columns=chart_cols)
                    st.line_chart(chart_df, color=["#2962FF", "#BDBDBD", "#FFAB40"])
                    
                    st.subheader("📜 交易记录")
                    df_trades = pd.DataFrame(res['trades']).sort_values(by='date', ascending=False)
                    st.dataframe(df_trades, use_container_width=True, hide_index=True)
                    
                    # 导出回测记录
                    csv_bt = df_trades.to_csv(index=False).encode('utf-8-sig')
                    st.download_button("📥 导出回测记录 (CSV)", data=csv_bt, file_name="backtest_trades.csv", mime="text/csv")

if __name__ == "__main__":
    render_dashboard()