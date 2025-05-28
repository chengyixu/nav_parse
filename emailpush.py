#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fund-NAV harvester v0.9.3 (LLM-focused, incremental processing, improved parsing & prompt)
Streamlit Enhanced Version - Chinese UI - With Process All Emails feature
───────────────────────────────────────────────────────────────────────────
1. Streamlit UI for interaction and display (Chinese).
2. IMAP login (163.com, ID handshake)
3. Read last run timestamp (for incremental processing).
4. Option to process new emails since last run, or all emails in a default window.
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
IMAP_HOST  = "imap.163.com"
EMAIL_USER = "zhanluekehu@163.com" # 请替换为您的实际邮箱
EMAIL_PWD  = "DRqdN38whrnCFPGx"    # 请替换为您的实际163邮箱应用授权码
GLM_KEY    = "afe7583d73c9d3948f60230e79e08151.Z9HPB84mxuC31DeK" # 请替换为您的实际GLM API Key
GLM_URL    = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
MODEL      = "glm-z1-flash" # 或者您偏好的模型，如 "glm-4", "glm-3-turbo"
# ─────────────────────────────────────────────────────────────────────────

TODAY   = datetime.date.today().strftime("%Y-%m-%d") 
XLSX    = f"{TODAY} 基金净值.xlsx" 
SHEET   = TODAY 
COLS    = ["日期","基金名称","基金代码","单位净值","累计净值",
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
        content = LAST_RUN_FILE.read_text().strip()
        if not content:
            append_log("上次运行时间戳文件为空。将使用默认时间窗口进行处理。", "info")
            return None
        dt_naive = datetime.datetime.strptime(content, DATETIME_FORMAT)
        dt_utc = dt_naive.replace(tzinfo=datetime.timezone.utc)
        append_log(f"上次运行时间戳: {dt_utc.strftime(DATETIME_FORMAT)} UTC", "info")
        return dt_utc
    except (ValueError, OSError) as e:
        append_log(f"读取或解析上次运行时间戳文件 {LAST_RUN_FILE} 失败: {e}。将使用默认时间窗口进行处理。", "warning")
        return None

def save_current_run_datetime():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        LAST_RUN_FILE.write_text(now_utc.strftime(DATETIME_FORMAT))
        append_log(f"已保存当前运行时间戳: {now_utc.strftime(DATETIME_FORMAT)} UTC 至 {LAST_RUN_FILE}", "info")
        st.session_state.current_run_timestamp_display = f"{now_utc.strftime(DATETIME_FORMAT)} UTC"
    except OSError as e:
        append_log(f"无法保存当前运行时间戳至 {LAST_RUN_FILE}: {e}", "warning")

def fetch_mail(last_run_utc_dt: datetime.datetime | None = None, default_days_lookback: int = 30, progress_bar=None, status_text=None):
    try:
        with IMAPClient(IMAP_HOST, ssl=True) as srv:
            srv.login(EMAIL_USER, EMAIL_PWD)
            try:
                srv.id_({"name":"python-streamlit","version":"0.9.4","vendor":"myclient", # version bump
                         "contact":EMAIL_USER})
            except Exception:
                pass
            
            srv.select_folder("INBOX")
            
            search_description_text = ""
            using_last_run_filter = False

            if last_run_utc_dt:
                if last_run_utc_dt.tzinfo is None or last_run_utc_dt.tzinfo.utcoffset(last_run_utc_dt) is None:
                    last_run_utc_dt = last_run_utc_dt.replace(tzinfo=datetime.timezone.utc)
                since_date_for_imap = last_run_utc_dt.date()
                search_criteria = ["SINCE", since_date_for_imap]
                search_description_text = (f"自 {last_run_utc_dt.strftime(DATETIME_FORMAT)} UTC "
                                      f"(服务器搜索起始日期: {since_date_for_imap.strftime('%Y-%m-%d')})")
                using_last_run_filter = True
            else: # This branch is used for "Process All Emails"
                since_date_for_imap = (datetime.datetime.now(datetime.timezone.utc).date() - 
                                       datetime.timedelta(days=default_days_lookback))
                search_criteria = ["SINCE", since_date_for_imap]
                search_description_text = (f"最近 {default_days_lookback} 天 "
                                      f"(服务器搜索起始日期: {since_date_for_imap.strftime('%Y-%m-%d')})")

            ids = srv.search(search_criteria)
            append_log(f"发现 {len(ids)} 封候选邮件 ({search_description_text})。", "info")
            st.session_state.run_summary['emails_found_server'] = len(ids)
            
            if not ids:
                append_log("没有邮件符合服务器端条件。", "info")
                if progress_bar: progress_bar.progress(1.0)
                if status_text: status_text.text("服务器上未找到符合条件的邮件。")
                return

            fetched_count = 0
            for i, mid in enumerate(ids):
                if progress_bar: progress_bar.progress((i + 1) / len(ids))
                if status_text: status_text.text(f"正在获取和筛选: {i+1}/{len(ids)}")
                
                try:
                    # Specify that we need INTERNALDATE as bytes for the key
                    raw_email_data_map = srv.fetch([mid], [b"RFC822", b"INTERNALDATE"])
                except IMAPClient.Abort as e_abort: 
                    append_log(f"获取邮件ID {mid} 期间发生IMAP中止错误: {e_abort}", "error")
                    raise 
                except Exception as e_fetch:
                    append_log(f"获取邮件ID {mid} 失败: {e_fetch}", "error")
                    continue 

                if not raw_email_data_map or mid not in raw_email_data_map:
                    append_log(f"警告: 无法获取邮件ID {mid} 的完整数据", "warning")
                    continue 
                
                message_data = raw_email_data_map[mid]

                if b"RFC822" not in message_data:
                    append_log(f"警告: 无法获取邮件ID {mid} 的RFC822 (正文)", "warning")
                    continue

                # Client-side filtering for incremental processing if last_run_utc_dt was provided
                if using_last_run_filter and last_run_utc_dt: 
                    internal_date_from_server = message_data.get(b'INTERNALDATE') 
                    
                    if internal_date_from_server:
                        # Ensure internal_date_from_server is timezone-aware (UTC)
                        if internal_date_from_server.tzinfo is None or \
                           internal_date_from_server.tzinfo.utcoffset(internal_date_from_server) is None:
                            internal_date_from_server = internal_date_from_server.replace(tzinfo=datetime.timezone.utc)
                        
                        if internal_date_from_server <= last_run_utc_dt:
                            continue # Skip this email as it's not newer than the last run
                    else:
                        append_log(f"警告: 邮件ID {mid} 缺少INTERNALDATE。无法按确切时间筛选，将基于日期匹配进行处理。", "warning")
                
                fetched_count += 1
                yield pyzmail.PyzMessage.factory(message_data[b"RFC822"])
            
            st.session_state.run_summary['emails_to_process_client'] = fetched_count
            append_log(f"客户端筛选后，总共获取待处理邮件数: {fetched_count}", "info")

    except (IMAPClient.Abort, ConnectionResetError) as e: 
        append_log(f"IMAP连接错误: {e}。请检查网络或凭据后重试。", "error")
        raise 
    except Exception as e:
        append_log(f"邮件获取过程中发生意外错误: {e}", "error")
        import traceback
        st.session_state.processing_log.append(traceback.format_exc())
        raise 

def get_body(msg):
    if msg.text_part:
        charset = msg.text_part.charset or "utf-8"
        return msg.text_part.get_payload().decode(charset, "ignore")
    if msg.html_part:
        charset = msg.html_part.charset or "utf-8"
        html = msg.html_part.get_payload().decode(charset, "ignore")
        return html2text(html)
    return ""

def list_attachments(msg):
    for part in msg.mailparts:
        fn = getattr(part, "filename", None)
        if fn:
            payload_bytes = part.get_payload()
            if not isinstance(payload_bytes, bytes):
                charset = part.charset or "utf-8"
                try:
                    payload_bytes = str(payload_bytes).encode(charset, "ignore")
                except Exception as e:
                    append_log(f"    无法将附件 '{fn}' 内容编码为字节: {e}。已跳过。", "warning")
                    continue
            yield fn, payload_bytes

def glm(prompt:str)->str:
    system_prompt = """您是一位提取金融数据的专家。请从提供的文本（邮件主题、正文和附件）中识别并提取关于公募基金或私募基金的净值信息。
请将信息以 JSON 对象数组的形式返回。每个对象应代表一只独立的基金，并精确包含以下字段：
- "日期": 基金净值的日期，格式为YYYY-MM-DD，来源于文本内容。
- "基金名称": 基金的名称。
- "基金代码": 基金的字母数字代码。
- "单位净值": 单位净值，应为一个数字。
- "累计净值": 累计净值，应为一个数字。

重要提示：
- 仅包含明确的基金净值数据条目。
- 如果列出了多只基金，请为每只基金创建一个单独的 JSON 对象。
- 如果在文本中未找到有效的基金净值数据，请返回一个空的 JSON 数组：[]。
- **您的回复必须严格遵守输出格式。您的回复只能包含一个 JSON 对象数组，不能有任何其他文字、解释、注释或思考过程。绝对不要使用 `<think>` 或任何类似的标签。如果找不到数据，请返回空的 JSON 数组 `[]`。任何偏离此 JSON-only 格式的输出都将被视为失败。**
- 确保“单位净值”和“累计净值”的值是数字。
- 请仔细准确识别基金名称和代码，避免提取通用文本或文件名。
- “日期”应该是与净值相关的特定日期，除非明确说明是净值日期，否则不一定是邮件日期或报告生成日期。

期望的单个基金输出示例：
[
  {
    "日期": "2025-05-26",
    "基金名称": "九招真格量化套利一号私募证券投资基金",
    "基金代码": "SQD546",
    "单位净值": 1.0580,
    "累计净值": 1.5053
  }
]
无数据时输出示例：
[]
"""
    try:
        res = requests.post(
            GLM_URL,
            json={
                "model": MODEL,
                "messages":[
                    {"role":"system", "content": system_prompt},
                    {"role":"user","content":prompt}],
                "temperature":0.2,
                "max_tokens":32000,
                "stream":False},
            headers={"Authorization":f"Bearer {GLM_KEY}"},
            timeout=300) 
        res.raise_for_status()
        return res.json()["choices"][0]["message"]["content"]
    except requests.exceptions.RequestException as e:
        append_log(f"    GLM API 请求失败: {e}", "error")
        return "[]" 
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        response_text = res.text if 'res' in locals() else "N/A (response object not available)"
        append_log(f"    GLM API 响应格式异常或非有效JSON: {e} - 响应内容: {response_text[:200]}", "error")
        return "[]"

def parse_glm(txt:str):
    try:
        cleaned_txt = txt.strip()
        json_start_index = -1
        first_brace = cleaned_txt.find('{')
        first_bracket = cleaned_txt.find('[')

        if first_brace != -1 and first_bracket != -1:
            json_start_index = min(first_brace, first_bracket)
        elif first_brace != -1:
            json_start_index = first_brace
        elif first_bracket != -1:
            json_start_index = first_bracket
        
        if json_start_index > 0:
            preceding_text = cleaned_txt[:json_start_index]
            append_log(f"    已剥离GLM响应中JSON内容之前的文本: '{preceding_text[:100].strip()}...'", "info")
            cleaned_txt = cleaned_txt[json_start_index:]
        elif json_start_index == -1 :
            append_log(f"    GLM输出不包含有效的JSON起始字符([或{{)，或者可能仅为思考过程: '{cleaned_txt[:200].strip()}...'", "warning")
            return []

        if cleaned_txt.startswith("```json"):
            cleaned_txt = cleaned_txt[len("```json"):].strip()
        elif cleaned_txt.startswith("```"):
            cleaned_txt = cleaned_txt[len("```"):].strip()
        if cleaned_txt.endswith("```"):
            cleaned_txt = cleaned_txt[:-len("```")].strip()

        if not cleaned_txt:
            return []
        
        data = json.loads(cleaned_txt)
        parsed_items = []
        expected_keys = {"日期", "基金名称", "基金代码", "单位净值", "累计净值"}

        items_to_process = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        if not items_to_process and data:
             append_log(f"    GLM输出(剥离后)是有效的JSON，但不是列表或字典格式: {cleaned_txt[:200]}", "warning")

        for item in items_to_process:
            if isinstance(item, dict):
                if expected_keys.issubset(item.keys()):
                    try:
                        item["单位净值"] = float(str(item["单位净值"]).replace(',','')) if item.get("单位净值") is not None else None
                        item["累计净值"] = float(str(item["累计净值"]).replace(',','')) if item.get("累计净值") is not None else None
                        parsed_items.append(item)
                    except (ValueError, TypeError):
                        append_log(f"    GLM项目已跳过(净值无法转换为浮点数): {str(item)[:100]}", "warning")
                else:
                    append_log(f"    GLM项目已跳过(缺少预期键): {str(item)[:100]}", "warning")
            else:
                append_log(f"    GLM项目已跳过(非字典格式): {str(item)[:100]}", "warning")
        return parsed_items

    except json.JSONDecodeError:
        append_log(f"    GLM输出(剥离后)不是有效的JSON。原始开头: '{txt[:100].strip()}...'", "warning")
        return []
    except Exception as e:
        append_log(f"    解析GLM输出时发生意外错误: {e}。原始开头: '{txt[:100].strip()}...'", "error")
        return []

def run_processing(process_all_mode: bool = False):
    st.session_state.processing_log = [] 
    st.session_state.processed_df = None
    st.session_state.run_summary = {}
    
    LOG_DIR.mkdir(parents=True, exist_ok=True) 
    
    last_run_dt_utc = None
    if process_all_mode:
        append_log("开始处理所有邮件 (最近30天默认范围)...", "info")
        # last_run_dt_utc remains None, so fetch_mail uses default_days_lookback
    else:
        append_log("开始处理新邮件...", "info")
        last_run_dt_utc = get_last_run_datetime() 
    
    rows = []
    progress_bar = st.progress(0.0)
    status_text = st.empty() 

    actual_emails_processed_count = 0
    
    try:
        mail_fetch_iterator = fetch_mail(
            last_run_utc_dt=last_run_dt_utc, 
            default_days_lookback=30, # Used if last_run_dt_utc is None (i.e., process_all_mode)
            progress_bar=progress_bar,
            status_text=status_text
        )
        
        if mail_fetch_iterator:
            email_processing_status = st.empty()
            for loop_idx, msg in enumerate(mail_fetch_iterator, 1):
                actual_emails_processed_count = loop_idx 
                if msg is None: continue

                sender_addresses = msg.get_addresses("from")
                if sender_addresses:
                    sender_name, sender_email = sender_addresses[0]
                else:
                    sender_name, sender_email = "未知发件人", "unknown@example.com"

                subj = msg.get_subject() or "(无主题)"
                body = get_body(msg)
                atts = list(list_attachments(msg)) 

                log_msg = (f"\n[{actual_emails_processed_count}] 正在处理: {subj}\n"
                           f"    发件人: {sender_name} <{sender_email}>\n"
                           f"    附件数 ({len(atts)}): {[fn for fn,_ in atts]}")
                append_log(log_msg)
                email_processing_status.text(f"正在分析邮件 {actual_emails_processed_count}: {subj[:50]}...")

                payloads_to_process = [(None, b"")] 
                payloads_to_process.extend(atts)

                for fn, blob in payloads_to_process:
                    attach_text = "(无相关文本内容)"
                    source_name = "正文"

                    if fn: 
                        source_name = fn
                        temp_file_path = None
                        try:
                            # Create a temporary file with the correct suffix for pandas to infer type
                            with tempfile.NamedTemporaryFile(delete=False, suffix=pathlib.Path(fn).suffix) as tmp:
                                tmp.write(blob)
                                temp_file_path = tmp.name
                            
                            try:
                                xls_content = pd.read_excel(temp_file_path, sheet_name=None)
                                if isinstance(xls_content, dict): # Multi-sheet excel
                                    combined_df = pd.concat(xls_content.values(), ignore_index=True)
                                else: # Single sheet excel
                                    combined_df = xls_content
                                attach_text = combined_df.to_csv(index=False, header=True)
                            except Exception: # Fallback to text decoding
                                try:
                                    attach_text = blob.decode("utf-8", "ignore")
                                except UnicodeDecodeError:
                                    attach_text = blob.decode("gbk", "ignore") # Common in Chinese emails
                                except Exception:
                                    attach_text = "(二进制文件或无法识别编码)"
                        except Exception as e_file:
                            attach_text = f"(附件处理错误: {e_file})"
                            append_log(f"    处理附件 {fn} 时出错: {e_file}", "warning")
                        finally:
                            if temp_file_path and pathlib.Path(temp_file_path).exists():
                                pathlib.Path(temp_file_path).unlink()
                    
                    prompt_context = f"【邮件正文】\n{body}\n\n"
                    if fn: 
                        prompt_context += f"【附件: {fn}】\n{attach_text}"

                    prompt = (
                        f"邮件主题: {subj}\n"
                        f"发件人: {sender_name} <{sender_email}>\n\n"
                        f"{prompt_context}"
                    )
                    
                    ans = glm(prompt) 
                    parsed = parse_glm(ans) 

                    if parsed:
                        append_log(f"    GLM从 {source_name} 解析到 {len(parsed)} 行数据", "info")
                        for item in parsed:
                            row = {c: "" for c in COLS}
                            row.update(item) 
                            row.update({
                                "原邮件名": subj,
                                "发件人": sender_email,
                                "发件机构": sender_name 
                            })
                            rows.append(row)
                    else:
                        append_log(f"    未能从 {source_name} 解析到数据 (或解析失败)", "info")
            email_processing_status.text(f"邮件分析完成。已处理 {actual_emails_processed_count} 封邮件。")
        
        if progress_bar: progress_bar.progress(1.0) 
        if status_text: status_text.empty() 

    except (IMAPClient.Abort, ConnectionResetError) as e_imap: 
        append_log(f"由于连接错误，IMAP处理已中止: {e_imap}", "error")
        st.session_state.run_summary['error'] = str(e_imap)
        save_current_run_datetime() 
        return 
    except Exception as e_main_loop:
        append_log(f"主处理循环中发生意外错误: {e_main_loop}", "error")
        st.session_state.run_summary['error'] = str(e_main_loop)
        import traceback
        st.session_state.processing_log.append(traceback.format_exc())
        save_current_run_datetime()
        return

    st.session_state.run_summary['emails_analyzed_count'] = actual_emails_processed_count

    if actual_emails_processed_count == 0 and not st.session_state.run_summary.get('emails_found_server', 0) > 0 :
        append_log("\n本次运行未在服务器上发现需要处理的邮件。", "info")
        st.session_state.run_summary['nav_rows_extracted'] = 0
        save_current_run_datetime() 
        return
    elif actual_emails_processed_count == 0 and st.session_state.run_summary.get('emails_found_server', 0) > 0 :
        # This case means emails were found on server but filtered out by client (e.g. already processed based on INTERNALDATE)
        if not process_all_mode: # Only relevant if we are in incremental mode
             append_log("\n服务器上找到邮件，但没有自上次运行以来的新邮件。", "info")
        else: # If process_all_mode, and still 0 analyzed, it means all found emails were filtered out (unlikely if any were found) or an issue.
             append_log("\n服务器上找到邮件，但筛选后无待处理邮件。", "info")
        st.session_state.run_summary['nav_rows_extracted'] = 0
        save_current_run_datetime()
        return

    if not rows:
        append_log("\n已处理邮件，但未捕获到基金净值数据。", "info")
        st.session_state.run_summary['nav_rows_extracted'] = 0
        save_current_run_datetime() 
        return

    df = pd.DataFrame(rows, columns=COLS)
    df.drop_duplicates(inplace=True) 
    st.session_state.run_summary['nav_rows_extracted'] = len(df)

    if df.empty:
        append_log("\n处理并移除重复项后，未捕获到唯一的基金净值数据。", "info")
        save_current_run_datetime() 
        return
    
    st.session_state.processed_df = df

    try:
        file_exists = pathlib.Path(XLSX).exists()
        if file_exists:
            with pd.ExcelWriter(XLSX, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df.to_excel(writer, index=False, sheet_name=SHEET, header=True)
        else:
            with pd.ExcelWriter(XLSX, engine="openpyxl", mode="w") as writer:
                df.to_excel(writer, index=False, sheet_name=SHEET, header=True)
        append_log(f"\n{len(df)} 行唯一数据已写入/更新至 {XLSX} (工作表: {SHEET})", "info")
    except Exception as e:
        append_log(f"    写入本地Excel文件 '{XLSX}' 失败: {e}。", "error")
        timestamp_fallback = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fallback_xlsx = f"{pathlib.Path(XLSX).stem}_fallback_{timestamp_fallback}{pathlib.Path(XLSX).suffix}"
        try:
            df.to_excel(fallback_xlsx, index=False, sheet_name=SHEET)
            append_log(f"\n数据已保存至备用文件: {fallback_xlsx}", "warning")
        except Exception as fe:
            append_log(f"    写入备用Excel文件 '{fallback_xlsx}' 失败: {fe}。", "error")

    save_current_run_datetime() 
    append_log("\n脚本执行周期结束。", "info")

# ─── Streamlit UI Configuration ───────────────────────────────────────
st.set_page_config(layout="wide")
st.title("基金净值邮件提取工具")

st.sidebar.header("配置信息")
st.sidebar.text_input("IMAP 服务器", value=IMAP_HOST, disabled=True)
st.sidebar.text_input("邮箱用户", value=EMAIL_USER, disabled=True) 

st.header("运行控制与信息")

last_run_ts_display = "尚未运行或未找到日志文件。"
if LAST_RUN_FILE.exists():
    try:
        content = LAST_RUN_FILE.read_text().strip()
        if content:
            datetime.datetime.strptime(content, DATETIME_FORMAT) 
            last_run_ts_display = f"{content} UTC"
    except Exception:
        last_run_ts_display = "读取上次运行时间戳错误或格式无效。"
st.info(f"上次成功处理记录于: **{last_run_ts_display}**")

# --- Buttons for processing ---
col1, col2 = st.columns(2)
with col1:
    if st.button("处理新邮件", type="primary", help="仅处理自上次成功运行以来接收的新邮件。"):
        with st.spinner("正在处理新邮件... 请稍候。"):
            run_processing(process_all_mode=False)
with col2:
    if st.button("处理所有邮件 (最近30天)", type="secondary", help="处理最近30天内的所有邮件，忽略上次运行时间。"):
        with st.spinner("正在处理所有邮件 (最近30天)... 请稍候。"):
            run_processing(process_all_mode=True)

if hasattr(st.session_state, 'current_run_timestamp_display') and st.session_state.current_run_timestamp_display:
    st.success(f"当前处理周期完成于: **{st.session_state.current_run_timestamp_display}**")

st.subheader("处理摘要")
summary = st.session_state.get('run_summary', {})
if summary:
    cols_summary = st.columns(3)
    cols_summary[0].metric("发现邮件数 (服务器)", summary.get('emails_found_server', "无"))
    cols_summary[1].metric("已分析邮件数", summary.get('emails_analyzed_count', "无")) # Changed label slightly
    cols_summary[2].metric("提取净值行数", summary.get('nav_rows_extracted', "无"))
    if 'error' in summary:
        st.error(f"处理过程中发生错误: {summary['error']}")
else:
    st.caption("本次会话尚未开始处理，或无摘要信息。")

st.subheader("提取数据")
if st.session_state.processed_df is not None and not st.session_state.processed_df.empty:
    st.dataframe(st.session_state.processed_df)
    
    output_excel = io.BytesIO()
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        st.session_state.processed_df.to_excel(writer, index=False, sheet_name=SHEET)
    excel_bytes = output_excel.getvalue()

    st.download_button(
        label="下载 Excel 文件",
        data=excel_bytes,
        file_name=XLSX,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
elif st.session_state.processed_df is not None and st.session_state.processed_df.empty:
    st.info("上次运行未提取到唯一的基金净值数据。")
else:
    st.caption("尚未处理或提取数据。请点击任一“处理邮件”按钮。")

st.subheader("处理日志")
with st.expander("显示/隐藏详细日志", expanded=False):
    if st.session_state.processing_log:
        for log_entry in reversed(st.session_state.processing_log):
            if "GLM从" in log_entry and "未能从" not in log_entry :
                 st.success(log_entry) 
            elif "警告" in log_entry or "失败" in log_entry or "错误" in log_entry or "Error" in log_entry.title():
                 st.warning(log_entry) 
            else:
                 st.text(log_entry)
    else:
        st.caption("日志为空。")

# To run: streamlit run your_script_name.py
