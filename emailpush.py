#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fund-NAV harvester v0.9.4 (Modified to process ALL emails when requested)
Streamlit Enhanced Version - Chinese UI - With Process All Emails feature
───────────────────────────────────────────────────────────────────────────
1. Streamlit UI for interaction and display (Chinese).
2. IMAP login (163.com, ID handshake)
3. Read last run timestamp (for incremental processing).
4. Option to process new emails since last run, or ALL emails in inbox.
5. On button click, for each selected message:
 • capture subject + sender + full body text
 • capture every attachment (any filename)
 • send ⟨subject + body + attachment text⟩ to GLM-Z1-Flash
6. Parse LLM's JSON response & write rows → 年-月-日 基金净值.xlsx (local save & download)
7. Save current run timestamp.
"""
import streamlit as st
import re, json, tempfile, pathlib, datetime, contextlib, io, warnings
from imapclient import IMAPClient # type: ignore
import pyzmail, pandas as pd, requests # type: ignore
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Optional, nicer HTML-to-text if bs4 is around
try:
    from bs4 import BeautifulSoup # type: ignore
    def html2text(html:str)->str:
        return BeautifulSoup(html, "html.parser").get_text("\n")
except ImportError:
    def html2text(html:str)->str:
        return re.sub(r"<[^>]+>", "", html)

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=SyntaxWarning, module="pyzmail.utils")
warnings.filterwarnings("ignore", category=SyntaxWarning, module="pyzmail.parse")

# ─── creds & endpoints ──────────────────────────────────────────────────
IMAP_HOST = "imap.163.com"
EMAIL_USER = "zhanluekehu@163.com" # 请替换为您的实际邮箱
EMAIL_PWD = "DRqdN38whrnCFPGx" # 请替换为您的实际163邮箱应用授权码
GLM_KEY = "afe7583d73c9d3948f60230e79e08151.Z9HPB84mxuC31DeK" # 请替换为您的实际GLM API Key
GLM_URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
MODEL = "glm-z1-flash" # 或者您偏好的模型，如 "glm-4", "glm-3-turbo"

# ─────────────────────────────────────────────────────────────────────────
TODAY = datetime.date.today().strftime("%Y-%m-%d")
XLSX = f"{TODAY} 基金净值.xlsx"
SHEET = TODAY
COLS = ["日期","基金名称","基金代码","单位净值","累计净值",
        "原邮件名","发件人","发件机构"]

# ─── Timestamp logging for incremental processing ─────────────────────
LOG_DIR = pathlib.Path("log")
LAST_RUN_FILE = LOG_DIR / "last_run.txt"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Initialize session state variables
if 'processing_log' not in st.session_state:
    st.session_state.processing_log = []
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None
if 'run_summary' not in st.session_state:
    st.session_state.run_summary = {}

def append_log(message, level="info"):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    st.session_state.processing_log.append(f"[{timestamp}] {message}")

def get_last_run_datetime() -> datetime.datetime | None:
    if not LAST_RUN_FILE.exists():
        append_log("未找到上次运行时间戳文件。将使用默认时间窗口进行处理。", "info")
        return None
    try:
        with open(LAST_RUN_FILE, 'r', encoding='utf-8') as f:
            timestamp_str = f.read().strip()
            return datetime.datetime.strptime(timestamp_str, DATETIME_FORMAT)
    except Exception as e:
        append_log(f"读取上次运行时间戳时出错: {e}", "error")
        return None

def save_last_run_datetime():
    LOG_DIR.mkdir(exist_ok=True)
    current_time = datetime.datetime.now().strftime(DATETIME_FORMAT)
    try:
        with open(LAST_RUN_FILE, 'w', encoding='utf-8') as f:
            f.write(current_time)
        append_log(f"已保存运行时间戳: {current_time}", "info")
    except Exception as e:
        append_log(f"保存运行时间戳时出错: {e}", "error")

def call_glm_api(prompt_text: str) -> dict:
    """调用 GLM API 并返回解析后的 JSON 响应"""
    headers = {
        "Authorization": f"Bearer {GLM_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user", 
                "content": prompt_text
            }
        ],
        "temperature": 0.1,
        "max_tokens": 2000
    }
    
    try:
        response = requests.post(GLM_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        if "choices" in result and len(result["choices"]) > 0:
            content = result["choices"][0]["message"]["content"]
            
            # 尝试解析JSON
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                return json.loads(json_str)
            else:
                append_log("API响应中未找到JSON格式数据", "warning")
                return {}
        else:
            append_log("API响应格式异常", "error")
            return {}
            
    except requests.exceptions.RequestException as e:
        append_log(f"API请求失败: {e}", "error")
        return {}
    except json.JSONDecodeError as e:
        append_log(f"JSON解析失败: {e}", "error")
        return {}

def extract_email_content(msg_data):
    """提取邮件内容和附件"""
    try:
        msg = pyzmail.PyzMessage.factory(msg_data[b'BODY[]'])
        
        # 获取基本信息
        subject = msg.get_subject() or "无主题"
        sender = msg.get_addresses('from')[0][1] if msg.get_addresses('from') else "未知发件人"
        
        # 提取正文
        body_text = ""
        if msg.text_part is not None:
            body_text = msg.text_part.get_payload().decode(msg.text_part.charset or 'utf-8', errors='ignore')
        elif msg.html_part is not None:
            html_content = msg.html_part.get_payload().decode(msg.html_part.charset or 'utf-8', errors='ignore')
            body_text = html2text(html_content)
        
        # 提取附件内容
        attachment_texts = []
        for mailpart in msg.mailparts:
            if mailpart.is_attachment:
                try:
                    att_content = mailpart.get_payload()
                    if isinstance(att_content, bytes):
                        # 尝试解码为文本
                        try:
                            att_text = att_content.decode('utf-8', errors='ignore')
                            attachment_texts.append(f"附件内容:\n{att_text}")
                        except:
                            attachment_texts.append(f"附件: {mailpart.filename} (二进制文件)")
                    else:
                        attachment_texts.append(f"附件内容:\n{att_content}")
                except Exception as e:
                    append_log(f"处理附件时出错: {e}", "warning")
        
        return {
            'subject': subject,
            'sender': sender,
            'body': body_text,
            'attachments': attachment_texts
        }
        
    except Exception as e:
        append_log(f"解析邮件时出错: {e}", "error")
        return None

def fetch_emails(process_all=False):
    """获取邮件列表"""
    try:
        with IMAPClient(IMAP_HOST) as client:
            client.login(EMAIL_USER, EMAIL_PWD)
            client.select_folder('INBOX')
            
            if process_all:
                # 处理所有邮件
                append_log("正在获取收件箱中的所有邮件...", "info")
                messages = client.search()
                append_log(f"找到 {len(messages)} 封邮件", "info")
            else:
                # 增量处理：只处理新邮件
                last_run = get_last_run_datetime()
                if last_run:
                    # 从上次运行时间开始
                    since_date = last_run.date()
                    append_log(f"正在获取自 {since_date} 以来的新邮件...", "info")
                else:
                    # 默认处理最近30天
                    since_date = datetime.date.today() - datetime.timedelta(days=30)
                    append_log(f"正在获取最近30天的邮件 (自 {since_date})...", "info")
                
                messages = client.search(['SINCE', since_date])
                append_log(f"找到 {len(messages)} 封新邮件", "info")
            
            if not messages:
                append_log("没有找到要处理的邮件", "info")
                return []
            
            # 获取邮件内容
            processed_emails = []
            for i, msg_id in enumerate(messages):
                try:
                    append_log(f"正在处理邮件 {i+1}/{len(messages)}", "info")
                    msg_data = client.fetch([msg_id], ['BODY[]'])
                    
                    email_content = extract_email_content(msg_data[msg_id])
                    if email_content:
                        processed_emails.append(email_content)
                        
                except Exception as e:
                    append_log(f"处理邮件 {msg_id} 时出错: {e}", "error")
                    continue
            
            return processed_emails
            
    except Exception as e:
        append_log(f"连接邮箱失败: {e}", "error")
        return []

def process_email_with_llm(email_content):
    """使用LLM处理单封邮件"""
    prompt = f"""
请分析以下邮件内容，提取基金净值信息。请以JSON格式返回结果：

邮件主题: {email_content['subject']}
发件人: {email_content['sender']}
邮件正文:
{email_content['body']}

附件内容:
{chr(10).join(email_content['attachments'])}

请提取以下信息并以JSON格式返回：
{{
    "funds": [
        {{
            "date": "YYYY-MM-DD",
            "fund_name": "基金名称",
            "fund_code": "基金代码",
            "unit_nav": "单位净值",
            "cumulative_nav": "累计净值"
        }}
    ]
}}

如果邮件中没有基金净值信息，请返回空的funds数组。
"""
    
    result = call_glm_api(prompt)
    return result.get('funds', [])

def main():
    st.set_page_config(page_title="基金净值采集器", layout="wide")
    
    st.title("🏦 基金净值采集器")
    st.markdown("自动从邮件中提取基金净值信息")
    
    # 显示当前配置
    with st.expander("📧 邮箱配置信息"):
        st.info(f"邮箱: {EMAIL_USER}")
        st.info(f"IMAP服务器: {IMAP_HOST}")
        
        last_run = get_last_run_datetime()
        if last_run:
            st.success(f"上次运行时间: {last_run.strftime(DATETIME_FORMAT)}")
        else:
            st.warning("这是首次运行")
    
    # 操作按钮
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 处理新邮件", type="primary"):
            st.session_state.processing_log = []
            st.session_state.processed_df = None
            st.session_state.run_summary = {}
            
            append_log("开始处理新邮件...", "info")
            emails = fetch_emails(process_all=False)
            
            if emails:
                process_emails(emails)
            else:
                st.warning("没有找到新邮件需要处理")
    
    with col2:
        if st.button("📧 处理所有邮件", type="secondary"):
            st.session_state.processing_log = []
            st.session_state.processed_df = None
            st.session_state.run_summary = {}
            
            append_log("开始处理所有邮件...", "info")
            emails = fetch_emails(process_all=True)  # 关键修改：传入 process_all=True
            
            if emails:
                process_emails(emails)
            else:
                st.warning("收件箱中没有找到邮件")
    
    with col3:
        if st.button("🗑️ 清除日志"):
            st.session_state.processing_log = []
            st.session_state.processed_df = None
            st.session_state.run_summary = {}
            st.rerun()
    
    # 显示处理日志
    if st.session_state.processing_log:
        st.subheader("📋 处理日志")
        log_container = st.container()
        with log_container:
            for log_entry in st.session_state.processing_log[-20:]:  # 只显示最近20条
                st.text(log_entry)
    
    # 显示处理结果
    if st.session_state.processed_df is not None:
        st.subheader("📊 处理结果")
        
        # 显示统计信息
        if st.session_state.run_summary:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("处理邮件数", st.session_state.run_summary.get('total_emails', 0))
            with col2:
                st.metric("成功解析", st.session_state.run_summary.get('successful_parses', 0))
            with col3:
                st.metric("提取基金数", st.session_state.run_summary.get('total_funds', 0))
            with col4:
                st.metric("处理时间", f"{st.session_state.run_summary.get('processing_time', 0):.1f}秒")
        
        # 显示数据表格
        st.dataframe(st.session_state.processed_df, use_container_width=True)
        
        # 下载按钮
        if not st.session_state.processed_df.empty:
            csv = st.session_state.processed_df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 下载CSV文件",
                data=csv.encode('utf-8-sig'),
                file_name=f"{TODAY}_基金净值.csv",
                mime="text/csv"
            )

def process_emails(emails):
    """处理邮件列表"""
    start_time = datetime.datetime.now()
    all_funds = []
    successful_parses = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, email_content in enumerate(emails):
        progress = (i + 1) / len(emails)
        progress_bar.progress(progress)
        status_text.text(f"正在处理邮件 {i+1}/{len(emails)}: {email_content['subject'][:50]}...")
        
        append_log(f"处理邮件: {email_content['subject']}", "info")
        
        try:
            funds = process_email_with_llm(email_content)
            if funds:
                successful_parses += 1
                for fund in funds:
                    fund_row = {
                        "日期": fund.get("date", ""),
                        "基金名称": fund.get("fund_name", ""),
                        "基金代码": fund.get("fund_code", ""),
                        "单位净值": fund.get("unit_nav", ""),
                        "累计净值": fund.get("cumulative_nav", ""),
                        "原邮件名": email_content['subject'],
                        "发件人": email_content['sender'],
                        "发件机构": email_content['sender'].split('@')[-1] if '@' in email_content['sender'] else ""
                    }
                    all_funds.append(fund_row)
                    
                append_log(f"成功提取 {len(funds)} 个基金信息", "info")
            else:
                append_log("未找到基金净值信息", "warning")
                
        except Exception as e:
            append_log(f"处理邮件时出错: {e}", "error")
    
    # 创建DataFrame
    if all_funds:
        df = pd.DataFrame(all_funds)
        st.session_state.processed_df = df
        
        # 保存到Excel文件
        try:
            df.to_excel(XLSX, sheet_name=SHEET, index=False)
            append_log(f"结果已保存到 {XLSX}", "info")
        except Exception as e:
            append_log(f"保存Excel文件时出错: {e}", "error")
    else:
        st.session_state.processed_df = pd.DataFrame(columns=COLS)
    
    # 保存运行时间戳
    save_last_run_datetime()
    
    # 更新统计信息
    end_time = datetime.datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    st.session_state.run_summary = {
        'total_emails': len(emails),
        'successful_parses': successful_parses,
        'total_funds': len(all_funds),
        'processing_time': processing_time
    }
    
    progress_bar.progress(1.0)
    status_text.text("处理完成！")
    
    append_log(f"处理完成！共处理 {len(emails)} 封邮件，成功解析 {successful_parses} 封，提取 {len(all_funds)} 个基金信息", "info")

if __name__ == "__main__":
    main()
