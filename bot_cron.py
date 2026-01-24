import os
import re
import time
import json
import pytz
import requests
import datetime
import pandas as pd
import numpy as np
import akshare as ak
from supabase import create_client


# === 1. 核心配置 ===
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
FEISHU_HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/31bb5f01-1e8b-4b08-8824-d634b95329e8"

print("DEBUG: 执行的是满血增强版 v2.0")

def get_bj_time():
    """强制北京时间"""
    return datetime.datetime.now(pytz.timezone('Asia/Shanghai'))

# === 2. 核心引擎类 ===
class IndicatorEngine:
    @staticmethod
    def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        data = df.copy()
        # 均线系统
        data['ema_21'] = data['nav'].ewm(span=21, adjust=False).mean()
        data['ema_55'] = data['nav'].ewm(span=55, adjust=False).mean()
        data['ema_89'] = data['nav'].ewm(span=89, adjust=False).mean()
        # 通道系统
        data['high_20'] = data['nav'].rolling(window=20).max()
        data['low_20'] = data['nav'].rolling(window=20).min()
        # 动能系统
        data['ao'] = data['nav'].rolling(window=5).mean() - data['nav'].rolling(window=34).mean()
        data['ao_prev'] = data['ao'].shift(1)
        # 波动率 ATR
        data['tr'] = data['nav'].diff().abs()
        data['atr'] = data['tr'].rolling(window=14).mean()
        return data

class DataService:
    @staticmethod
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
        except: return pd.DataFrame()

    @staticmethod
    def get_realtime_estimate(code):
        """抓取实时估值"""
        try:
            url = f"http://fundgz.1234567.com.cn/js/{code}.js?rt={int(time.time())}"
            r = requests.get(url, timeout=3)
            match = re.findall(r'\((.*?)\)', r.text)
            if match:
                data = json.loads(match[0])
                return float(data['gsz']), float(data['gszzl'])
            return None, None
        except: return None, None

    @staticmethod
    def get_market_wide_pool():
        """获取全市场 Top 300 品种"""
        try:
            df = ak.fund_open_fund_rank_em(symbol="全部")
            mask = df['基金简称'].str.contains('债|货币|理财|定开|持有|养老|以太|比特', regex=True) == False
            df = df[mask].dropna(subset=['近6月']).sort_values(by="近6月", ascending=False)
            return [{"code": str(row['基金代码']), "name": row['基金简称']} for _, row in df.head(300).iterrows()]
        except: return []

class WaveEngine:
    @staticmethod
    def analyze_structure(df_slice):
        if len(df_slice) < 60: return {'status': 'Wait', 'score': 0, 'desc': '数据不足'}
        last_nav = df_slice['nav'].iloc[-1]
        ema89 = df_slice['ema_89'].iloc[-1]
        high_20 = df_slice['high_20'].iloc[-2]
        low_20 = df_slice['low_20'].iloc[-2]
        ao_curr = df_slice['ao'].iloc[-1]
        ao_prev = df_slice['ao_prev'].iloc[-1]
        
        # 卖出判定
        if last_nav < ema89: return {'status': 'Sell', 'score': -100, 'desc': '破位：跌破生命线(EMA89)'}
        if last_nav < low_20: return {'status': 'Sell', 'score': -90, 'desc': '破位：跌破20日新低支撑'}
        
        # 买入判定
        if last_nav > high_20:
            if ao_curr > 0 and ao_curr > ao_prev:
                return {'status': 'Buy', 'score': 85, 'desc': '突破：20日新高 + 动能确认 (浪3特征)'}
            return {'status': 'Buy', 'score': 75, 'desc': '突破：20日新高 (等待动能放量)'}
        return {'status': 'Hold', 'score': 50, 'desc': '震荡整理中'}

# === 3. 执行逻辑 ===
def run_cron_mission():
    bj_now = get_bj_time()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table("trader_storage").select("portfolio_data").eq("id", "default_user").execute()
    
    portfolio = res.data[0]['portfolio_data'] if res.data else {}
    
    # 获取不同类型的池子
    real_holdings = portfolio.get('holdings', [])
    pending_list = portfolio.get('pending_orders', []) 
    
    capital = portfolio.get('capital', 20000)
    sections = []

    # --- A. 深度风控巡检 (涵盖持仓 & 模拟交易台) ---
    sell_alerts = []
    scan_pool = [
        {"data": h, "type": "实盘持仓"} for h in real_holdings
    ] + [
        {"data": p, "type": "模拟交易"} for p in pending_list
    ]

    for item in scan_pool:
        h = item['data']
        h_type = item['type']
        
        est_p, _ = DataService.get_realtime_estimate(h['code'])
        df = DataService.fetch_nav_history(h['code'])
        
        if est_p: 
            new_row = pd.DataFrame({'nav': [est_p]}, index=[bj_now])
            df = pd.concat([df, new_row])
            
        df = IndicatorEngine.calculate_indicators(df)
        ans = WaveEngine.analyze_structure(df)
        
        price_now = est_p or (df['nav'].iloc[-1] if not df.empty else 0)
        
        # 判定 Sell 信号
        is_wave_sell = ans['status'] == 'Sell'
        is_stop_loss = h.get('stop_loss', 0) > 0 and price_now < h['stop_loss']
        
        if is_wave_sell or is_stop_loss:
            reason = ans['desc'] if is_wave_sell else f"跌破止损位({h['stop_loss']})"
            sell_alerts.append(f"🚨 **[{h_type}] 卖出建议**: {h['name']} ({h['code']})\n   • 现价:{price_now:.4f} | 原因: {reason}")
    
    # 构造预警板块内容
    if sell_alerts:
        sections.append("🔥 **持仓/模拟风控预警**\n" + "\n".join(sell_alerts))
    else:
        sections.append("✅ **风险巡检**: 当前持仓及模拟交易台表现正常，未发现 Sell 卖出信号。")

    # --- B. 全市场雷达 (Top 15 & 取消 A/C 去重) ---
    buy_opps = []
    market_pool = DataService.get_market_wide_pool()
    
    for fund in market_pool:
        est_m, _ = DataService.get_realtime_estimate(fund['code'])
        df_m = DataService.fetch_nav_history(fund['code'])
        if est_m and not df_m.empty:
            new_row = pd.DataFrame({'nav': [est_m]}, index=[bj_now])
            df_m = pd.concat([df_m, new_row])
        
        df_m = IndicatorEngine.calculate_indicators(df_m)
        ans_m = WaveEngine.analyze_structure(df_m)
        
        # 放宽条件：评分≥70 即可（可选）
        if ans_m['status'] == 'Buy' and ans_m['score'] >= 70:
            total_assets = capital + sum([h['shares'] * h['cost'] for h in real_holdings])
            suggest_amt = total_assets * 0.1
            buy_opps.append(f"✅ **{fund['name']}** ({fund['code']})\n   • 评分: {ans_m['score']} | 建议单位: ¥{suggest_amt:,.0f}\n   • 原因: {ans_m['desc']}")
        
        # 最多显示15只
        if len(buy_opps) >= 15: break

    # 动态标题：显示实际数量
    sections.append(f"🔭 **选股雷达 (强动能 Top {len(buy_opps)})**\n" + ("\n".join(buy_opps) if buy_opps else "⚪ 暂无符合突破条件的强信号。"))

    # --- C. 飞书卡片组装 ---
    content = "\n\n---\n\n".join(sections)
    template = "red" if sell_alerts else "blue"
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"content": f"🌊 波浪策略巡检 ({bj_now.strftime('%H:%M')})", "tag": "plain_text"}, "template": template},
            "elements": [
                {"tag": "div", "text": {"content": content, "tag": "lark_md"}},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"content": f"账户现金: ¥{capital:,.0f} | 实盘持仓: {len(real_holdings)}只 | 模拟交易台: {len(pending_list)}只 | 本次扫描: Top 300 品种", "tag": "plain_text"}]}
            ]
        }
    }
    
    requests.post(FEISHU_HOOK, json=payload, timeout=20)

# === 4. 主入口（带异常兜底） ===
if __name__ == "__main__":
    try:
        run_cron_mission()
    except Exception as e:
        # 兜底报错，防止脚本静默失效
        try:
            requests.post(
                FEISHU_HOOK, 
                json={
                    "msg_type": "text", 
                    "content": {"text": f"❌ 巡检脚本运行故障: {str(e)}\n🕒 故障时间: {get_bj_time().strftime('%Y-%m-%d %H:%M:%S')}"}
                },
                timeout=10
            )
        except:
            # 极端情况：推送报错也失败，打印到终端
            print(f"脚本运行失败，且报错推送失败！错误信息: {e}")
