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

# === 1. 核心配置 (从 GitHub Secrets 读取) ===
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
FEISHU_HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/31bb5f01-1e8b-4b08-8824-d634b95329e8"

# === 2. 基础服务类 ===

def get_bj_time():
    """强制获取北京时间"""
    return datetime.datetime.now(pytz.timezone('Asia/Shanghai'))

class IndicatorEngine:
    @staticmethod
    def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        data = df.copy()
        data['ema_21'] = data['nav'].ewm(span=21, adjust=False).mean()
        data['ema_55'] = data['nav'].ewm(span=55, adjust=False).mean()
        data['ema_89'] = data['nav'].ewm(span=89, adjust=False).mean()
        data['high_20'] = data['nav'].rolling(window=20).max()
        data['low_20'] = data['nav'].rolling(window=20).min()
        data['tr'] = data['nav'].diff().abs()
        data['atr'] = data['tr'].rolling(window=14).mean()
        data['ao'] = data['nav'].rolling(window=5).mean() - data['nav'].rolling(window=34).mean()
        data['ao_prev'] = data['ao'].shift(1)
        return data

class DataService:
    @staticmethod
    def fetch_nav_history(code):
        try:
            for _ in range(3):
                df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
                if not df.empty: break
                time.sleep(1)
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
        try:
            ts = int(time.time() * 1000)
            url = f"http://fundgz.1234567.com.cn/js/{code}.js?rt={ts}"
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                match = re.findall(r'\((.*?)\)', r.text)
                if match:
                    data = json.loads(match[0])
                    return float(data['gsz']), float(data['gszzl'])
            return None, None
        except: return None, None

    @staticmethod
    def get_smart_price_for_cron(code):
        df = DataService.fetch_nav_history(code)
        est_p, _ = DataService.get_realtime_estimate(code)
        if df.empty: return est_p or 0, df
        if est_p:
            last_date = df.index[-1]
            if last_date.date() < get_bj_time().date():
                new_row = pd.DataFrame({'nav': [est_p]}, index=[last_date + datetime.timedelta(days=1)])
                df = pd.concat([df, new_row])
        return df['nav'].iloc[-1], df

    @staticmethod
    def get_market_wide_pool():
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
        
        if last_nav < ema89:
            return {'status': 'Sell', 'score': -100, 'desc': '破位：跌破 EMA89 生命线'}
        if last_nav < low_20:
            return {'status': 'Sell', 'score': -90, 'desc': '破位：跌破 20 日支撑'}
        if last_nav > high_20:
            if ao_curr > 0 and ao_curr > ao_prev:
                return {'status': 'Buy', 'score': 85, 'desc': '突破：20日新高 + 动能确认 (浪3特征)'}
            return {'status': 'Buy', 'score': 70, 'desc': '突破：20日新高 (待放量)'}
        return {'status': 'Hold', 'score': 50, 'desc': '震荡运行中'}

# === 3. 自动化任务执行 ===

def run_daily_mission():
    bj_now = get_bj_time()
    print(f"🚀 开始定时巡检: {bj_now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table("trader_storage").select("portfolio_data").eq("id", "default_user").execute()
    
    if not res.data:
        print("❌ 未发现用户数据")
        return
    
    portfolio = res.data[0]['portfolio_data']
    holdings = portfolio.get('holdings', [])
    capital = portfolio.get('capital', 20000)
    
    report_sections = []

    # --- A. 持仓风险诊断 ---
    sell_alerts = []
    print(f"正在诊断 {len(holdings)} 只持仓...")
    for h in holdings:
        price, df = DataService.get_smart_price_for_cron(h['code'])
        if not df.empty:
            df = IndicatorEngine.calculate_indicators(df)
            analysis = WaveEngine.analyze_structure(df)
            if h.get('stop_loss', 0) > 0 and price < h['stop_loss']:
                sell_alerts.append(f"🔴 **止损触发**: {h['name']} (现价{price:.4f} < 止损{h['stop_loss']:.4f})")
            elif analysis['status'] == 'Sell':
                sell_alerts.append(f"🚨 **卖点预警**: {h['name']} ({analysis['desc']})")
                
    if sell_alerts:
        report_sections.append("🔥 **持仓风险项**\n" + "\n".join(sell_alerts))
    else:
        report_sections.append("✅ **持仓状态**: 目前持仓基金表现稳定，未触发卖出信号。")

    # --- B. 全市场激进扫描 (去重版) ---
    print("正在扫描全市场机会...")
    buy_opps = []
    market_pool = DataService.get_market_wide_pool()
    seen_names = set() # 用于 A/C 类合并
    
    for fund in market_pool:
        # 合并去重逻辑：取基金名称前5个字符进行匹配
        base_name = re.sub(r'[AC]$', '', fund['name']).strip()
        if base_name in seen_names: continue
        
        _, df_m = DataService.get_smart_price_for_cron(fund['code'])
        if len(df_m) > 60:
            df_m = IndicatorEngine.calculate_indicators(df_m)
            analysis_m = WaveEngine.analyze_structure(df_m)
            
            if analysis_m['status'] == 'Buy' and analysis_m['score'] >= 80:
                total_assets = capital + sum([h['shares'] * h['cost'] for h in holdings])
                suggest_amt = total_assets * 0.1
                buy_opps.append(
                    f"✅ **{fund['name']}** ({fund['code']})\n"
                    f"   • 评分: {analysis_m['score']} | 建议买入: ¥{suggest_amt:,.0f}\n"
                    f"   • 原因: {analysis_m['desc']}"
                )
                seen_names.add(base_name)
        
        if len(buy_opps) >= 15: break # 满 15 个停止

    if buy_opps:
        report_sections.append(f"🔭 **选股雷达 (强动能 Top 15)**\n" + "\n".join(buy_opps))
    else:
        report_sections.append("🔭 **选股雷达**: 扫描了 Top 300 品种，暂未发现符合买入标准的强力信号。")

    # --- C. 组装并发送飞书 ---
    content = "\n\n---\n\n".join(report_sections)
    template = "red" if sell_alerts else "blue"
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "template": template,
                "title": {"content": f"🌊 波浪策略定时报告 ({bj_now.strftime('%H:%M')})", "tag": "plain_text"}
            },
            "elements": [
                {"tag": "div", "text": {"content": content, "tag": "lark_md"}},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"content": f"持仓: {len(holdings)} | 现金: ¥{capital:,.0f} | 建议以 Kelly 公式为准", "tag": "plain_text"}]}
            ]
        }
    }
    
    try:
        requests.post(FEISHU_HOOK, json=payload, timeout=15)
        print("✅ 报告已推送到飞书")
    except Exception as e:
        print(f"❌ 推送失败: {e}")

if __name__ == "__main__":
    run_daily_mission()