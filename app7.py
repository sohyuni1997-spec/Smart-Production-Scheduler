"""
생산 계획 하이브리드 수사 시스템
메인 UI + 실행 엔진
"""

import streamlit as st
import pandas as pd
from supabase import create_client, Client
import google.generativeai as genai
from datetime import datetime, timedelta
import plotly.graph_objects as go
import re

# ==================== 핵심 함수 임포트 ====================
from main_engine import ask_professional_scheduler
from functions_part1 import initialize_globals

# ==================== 환경 설정 ====================
URL = "https://qipphcdzlmqidhrjnjtt.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpcHBoY2R6bG1xaWRocmpuanR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY5NTIwMTIsImV4cCI6MjA4MjUyODAxMn0.AsuvjVGCLUJF_IPvQevYASaM6uRF2C6F-CjwC3eCNVk"
GENAI_KEY = "AIzaSyBX25WfvCJ-PE0yjjrIBHlM_t9-TdChRgI"

supabase: Client = create_client(URL, KEY)
genai.configure(api_key=GENAI_KEY)

CAPA_LIMITS = {"조립1": 3300, "조립2": 3700, "조립3": 3600}
FROZEN_DAYS = 3
TEST_MODE = True
TODAY = datetime(2026, 1, 5).date() if TEST_MODE else datetime.now().date()

# ==================== 전역 변수 초기화 ====================
initialize_globals(TODAY, CAPA_LIMITS)

# ==================== 데이터 로드 ====================
@st.cache_data(ttl=600)
def fetch_data(target_date=None):
    try:
        if target_date:
            dt = datetime.strptime(target_date, '%Y-%m-%d')
            start_date = (dt - timedelta(days=10)).strftime('%Y-%m-%d')
            end_date = (dt + timedelta(days=10)).strftime('%Y-%m-%d')
            plan_res = supabase.table("production_plan_2026_01").select("*").gte("plan_date", start_date).lte("plan_date", end_date).execute()
        else:
            plan_res = supabase.table("production_plan_2026_01").select("*").execute()
        
        plan_df = pd.DataFrame(plan_res.data)
        hist_res = supabase.table("production_investigation").select("*").execute()
        hist_df = pd.DataFrame(hist_res.data)

        if not plan_df.empty:
            plan_df['name_clean'] = plan_df['product_name'].apply(lambda x: re.sub(r'\s+', '', str(x)).strip())
            plt_map = plan_df.groupby('name_clean')['plt'].first().to_dict()
            product_map = plan_df.groupby('name_clean')['line'].unique().to_dict()
            for k in product_map:
                if "T6" in k.upper(): 
                    product_map[k] = ["조립1", "조립2", "조립3"]
            return plan_df, hist_df, product_map, plt_map
        return pd.DataFrame(), pd.DataFrame(), {}, {}
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        return pd.DataFrame(), pd.DataFrame(), {}, {}

def extract_date(text):
    """질문에서 날짜 추출"""
    patterns = [r'(\d{1,2})/(\d{1,2})', r'(\d{1,2})월\s*(\d{1,2})일', r'202[56]-(\d{1,2})-(\d{1,2})']
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            m, d = match.groups()
            return f"2026-{int(m):02d}-{int(d):02d}"
    return None

def extract_capa_target(text):
    """질문에서 목표 CAPA 비율 추출"""
    match = re.search(r'(\d+)%', text)
    return int(match.group(1)) / 100 if match else 0.75

# ==================== UI 구성 ====================
st.set_page_config(page_title="하이브리드 AI", layout="wide")
st.title("🤖 생산 계획 하이브리드 수사 시스템")

with st.sidebar:
    st.header("⚙️ 설정")
    st.markdown("### 🔍 수사 방식")
    st.info("""
    **하이브리드 엔진**
    - 🐍 Python: 팩트 수사 (1~4단계)
    - 🤖 AI: 전략 수립 (5단계)
    - 🐍 Python: 최종 검증 (6단계)
    """)
    
    st.markdown("### 📅 기준 정보")
    frozen_date = (datetime.combine(TODAY, datetime.min.time()) + timedelta(days=FROZEN_DAYS)).strftime('%Y-%m-%d')
    st.info(f"**기준일**: {TODAY.strftime('%Y-%m-%d')}\n\n**고정 기간**: ~{frozen_date}")
    
    st.markdown("### 🏭 CAPA 한계")
    for line, limit in CAPA_LIMITS.items():
        st.metric(line, f"{limit:,}개")
    
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    st.markdown("### 📖 사용 가이드")
    st.markdown("""
    **입력 예시:**
    - `1/6 조립1 70%만 생산하고 싶어`
    - `1월 8일 조립2 80% CAPA`
    - `2026-01-10 조립3 75%로 줄여줘`
    
    **6단계 프로세스:**
    1. 🐍 품목/수량 나열
    2. 🐍 누적 납기 계산
    3. 🐍 목적지 CAPA 분석
    4. 🐍 물리 제약 정리
    5. 🤖 AI 전략 수립
    6. 🐍 Python 최종 검증
    """)

# ==================== 메인 채팅 영역 ====================
if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]): 
        st.markdown(msg["content"])

if prompt := st.chat_input("질문을 입력하세요 (예: 1/6 조립1 70%만 생산하고 싶어)"):
    # 사용자 메시지 추가
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"): 
        st.markdown(prompt)
    
    # 날짜 추출
    target_date = extract_date(prompt)
    
    if not target_date:
        answer = "❌ 날짜를 인식할 수 없습니다. 예: `1/6` 또는 `1월 6일` 형식으로 입력해주세요."
        st.session_state.messages.append({"role": "assistant", "content": answer})
        with st.chat_message("assistant"):
            st.markdown(answer)
    else:
        with st.spinner("🔍 하이브리드 수사 진행 중... (Python 분석 + AI 전략 + Python 검증)"):
            # 데이터 로드
            plan_df, hist_df, product_map, plt_map = fetch_data(target_date)
            
            if plan_df.empty:
                answer = "❌ 데이터를 불러올 수 없습니다. 날짜를 확인해주세요."
            else:
                try:
                    # ========== [중요] 엔진 실행 전 전역 변수 재초기화 ==========
                    initialize_globals(TODAY, CAPA_LIMITS)
                    
                    # ========== 메인 엔진 호출 ==========
                    report, success, charts, status = ask_professional_scheduler(
                        question=prompt,
                        plan_df=plan_df,
                        hist_df=hist_df,
                        product_map=product_map,
                        plt_map=plt_map,
                        question_date=target_date,
                        mode="hybrid"
                    )
                    
                    if success:
                        answer = f"✅ {status}\n\n{report}"
                    else:
                        answer = f"⚠️ {status}\n\n{report}"
                
                except Exception as e:
                    answer = f"❌ **오류 발생**\n\n```\n{str(e)}\n```"
                    st.exception(e)
            
            # 응답 저장 및 표시
            st.session_state.messages.append({"role": "assistant", "content": answer})
            
            with st.chat_message("assistant"):
                st.markdown(answer)
                
                # ========== CAPA 차트 추가 ==========
                if not plan_df.empty and 'qty_1차' in plan_df.columns:
                    st.markdown("---")
                    st.subheader("📊 CAPA 사용 현황")
                    
                    daily_summary = plan_df.groupby(['plan_date', 'line'])['qty_1차'].sum().reset_index()
                    daily_summary.columns = ['plan_date', 'line', 'current_qty']
                    daily_summary['max_capa'] = daily_summary['line'].map(CAPA_LIMITS)
                    daily_summary['remaining_capa'] = daily_summary['max_capa'] - daily_summary['current_qty']
                    
                    chart_data = daily_summary.pivot(index='plan_date', columns='line', values='current_qty').fillna(0)
                    
                    fig = go.Figure()
                    colors = {'조립1': '#0066CC', '조립2': '#66B2FF', '조립3': '#FF6666'}
                    
                    for line in ['조립1', '조립2', '조립3']:
                        if line in chart_data.columns:
                            fig.add_trace(go.Bar(
                                name=f'{line}',
                                x=chart_data.index,
                                y=chart_data[line],
                                marker_color=colors[line],
                                hovertemplate='<b>%{x}</b><br>수량: %{y:,}개<extra></extra>'
                            ))
                    
                    # CAPA 한계선 추가
                    for line, limit in CAPA_LIMITS.items():
                        fig.add_hline(
                            y=limit, 
                            line_dash="dash", 
                            line_color=colors[line],
                            annotation_text=f"{line} 한계: {limit:,}",
                            annotation_position="right"
                        )
                    
                    fig.update_layout(
                        barmode='group', 
                        height=400, 
                        xaxis_title='날짜', 
                        yaxis_title='수량 (개)',
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        hovermode='x unified'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # 테이블 요약
                    with st.expander("📋 상세 데이터 보기"):
                        st.dataframe(
                            daily_summary.style.format({
                                'current_qty': '{:,.0f}',
                                'max_capa': '{:,.0f}',
                                'remaining_capa': '{:,.0f}'
                            }),
                            use_container_width=True
                        )
