#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fund-NAV harvester v0.9.3 (LLM-focused, incremental processing, improved parsing & prompt)
───────────────────────────────────────────────────────────────────────────
1. IMAP login (163.com, ID handshake)
2. Read last run timestamp.
3. For each new message (since last run):
     • capture subject + sender + full body text
     • capture every attachment (any filename)
     • send ⟨subject + body + attachment text⟩ to GLM-Z1-Flash
4. Parse LLM's JSON response & write rows → YYYY-MM-DD 基金净值.xlsx
5. Save current run timestamp.
"""

import re, json, tempfile, pathlib, datetime, contextlib, io, warnings
from imapclient import IMAPClient
import pyzmail, pandas as pd, requests
from tqdm import tqdm

# optional, nicer HTML-to-text if bs4 is around
try:
    from bs4 import BeautifulSoup
    def html2text(html:str)->str:
        return BeautifulSoup(html, "html.parser").get_text("\n")
except ImportError:
    def html2text(html:str)->str:
        return re.sub(r"<[^>]+>", "", html)

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ─── creds & endpoints ──────────────────────────────────────────────────
IMAP_HOST  = "imap.163.com"
EMAIL_USER = "zhanluekehu@163.com" # Replace with your actual email
EMAIL_PWD  = "DRqdN38whrnCFPGx"    # Replace with your actual 163 App Authorization Code
GLM_KEY    = "afe7583d73c9d3948f60230e79e08151.Z9HPB84mxuC31DeK" # Replace with your actual GLM API Key
GLM_URL    = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
MODEL      = "glm-z1-flash" # Or your preferred model like "glm-4", "glm-3-turbo"
# ─────────────────────────────────────────────────────────────────────────

TODAY   = datetime.date.today().strftime("%Y-%m-%d") # Used for default Excel sheet name
XLSX    = f"{TODAY} 基金净值.xlsx" # Output Excel filename uses current date
SHEET   = TODAY # Sheet name is current date
COLS    = ["日期","基金名称","基金代码","单位净值","累计净值",
           "原邮件名","发件人","发件机构"]

# ─── Timestamp logging for incremental processing ─────────────────────
LOG_DIR = pathlib.Path("log")
LAST_RUN_FILE = LOG_DIR / "last_run.txt"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S" # UTC datetime format for the log file

def get_last_run_datetime() -> datetime.datetime | None:
    """Reads the last successful run datetime from the log file (expects UTC)."""
    if not LAST_RUN_FILE.exists():
        print("ℹ️ Last run timestamp file not found. Processing with default window.")
        return None
    try:
        content = LAST_RUN_FILE.read_text().strip()
        if not content:
            print("ℹ️ Last run timestamp file is empty. Processing with default window.")
            return None
        dt_naive = datetime.datetime.strptime(content, DATETIME_FORMAT)
        # Assume stored time is UTC, make it timezone-aware
        dt_utc = dt_naive.replace(tzinfo=datetime.timezone.utc)
        print(f"ℹ️ Previous run timestamp: {dt_utc.strftime(DATETIME_FORMAT)} UTC")
        return dt_utc
    except (ValueError, OSError) as e:
        print(f"⚠️ Error reading or parsing last run timestamp from {LAST_RUN_FILE}: {e}. Processing with default window.")
        return None

def save_current_run_datetime():
    """Saves the current datetime (UTC) as the last successful run timestamp."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        LAST_RUN_FILE.write_text(now_utc.strftime(DATETIME_FORMAT))
        print(f"☑️ Saved current run timestamp: {now_utc.strftime(DATETIME_FORMAT)} UTC to {LAST_RUN_FILE}")
    except OSError as e:
        print(f"⚠️ Could not save current run timestamp to {LAST_RUN_FILE}: {e}")

# ─── helper: fetch mail (modified for incremental processing) ───────────
def fetch_mail(last_run_utc_dt: datetime.datetime | None = None, default_days_lookback: int = 30):
    """
    Fetches emails. If last_run_utc_dt is provided, fetches emails SINCE that date
    and then filters by time. Otherwise, fetches emails from default_days_lookback.
    Yields pyzmail.PyzMessage objects.
    """
    with IMAPClient(IMAP_HOST, ssl=True) as srv:
        srv.login(EMAIL_USER, EMAIL_PWD)
        try:
            srv.id_({"name":"python","version":"0.9.3","vendor":"myclient", # Updated version
                     "contact":EMAIL_USER})
        except Exception:
            pass # Optional, continue if ID command fails
        
        srv.select_folder("INBOX")
        
        search_description = ""
        using_last_run_filter = False

        if last_run_utc_dt:
            # IMAP SINCE uses date part. Server returns all emails on or after this date.
            # Time-based filtering will be done client-side using INTERNALDATE.
            # Ensure last_run_utc_dt is UTC-aware for comparison.
            if last_run_utc_dt.tzinfo is None or last_run_utc_dt.tzinfo.utcoffset(last_run_utc_dt) is None:
                last_run_utc_dt = last_run_utc_dt.replace(tzinfo=datetime.timezone.utc)

            since_date_for_imap = last_run_utc_dt.date()
            search_criteria = ["SINCE", since_date_for_imap]
            search_description = (f"candidates since {last_run_utc_dt.strftime(DATETIME_FORMAT)} UTC "
                                  f"(server search from date: {since_date_for_imap.strftime('%Y-%m-%d')})")
            using_last_run_filter = True
        else:
            # Fallback to default lookback period if no last run timestamp
            since_date_for_imap = (datetime.datetime.now(datetime.timezone.utc).date() - 
                                   datetime.timedelta(days=default_days_lookback))
            search_criteria = ["SINCE", since_date_for_imap] # imapclient handles date obj
            search_description = (f"candidates from last {default_days_lookback} days "
                                  f"(server search from date: {since_date_for_imap.strftime('%Y-%m-%d')})")

        ids = srv.search(search_criteria)
        print(f"📬 Found {len(ids)} email {search_description}.")
        
        if not ids:
            print("No emails matched server-side criteria.\n")
            return

        for mid in tqdm(ids, desc="Fetching & Filtering", unit="mail", mininterval=0.5, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'):
            raw_email_data_map = srv.fetch([mid], ["RFC822", "INTERNALDATE"])
            
            if not raw_email_data_map or mid not in raw_email_data_map:
                tqdm.write(f"Warning: Could not fetch full data for message ID {mid}")
                continue 
            
            message_data = raw_email_data_map[mid]

            if b"RFC822" not in message_data:
                tqdm.write(f"Warning: Could not fetch RFC822 (body) for message ID {mid}")
                continue

            if using_last_run_filter:
                internal_date_from_server = message_data.get(b'INTERNALDATE') # datetime obj from imapclient
                
                if internal_date_from_server:
                    if internal_date_from_server.tzinfo is None or \
                       internal_date_from_server.tzinfo.utcoffset(internal_date_from_server) is None:
                        internal_date_from_server = internal_date_from_server.replace(tzinfo=datetime.timezone.utc)
                    
                    if internal_date_from_server <= last_run_utc_dt:
                        continue 
                else:
                    tqdm.write(f"Warning: Message ID {mid} missing INTERNALDATE. Cannot filter by exact time. Processing due to date match.")

            yield pyzmail.PyzMessage.factory(message_data[b"RFC822"])

# ─── helper: full body text ─────────────────────────────────────────────
def get_body(msg):
    if msg.text_part:
        charset = msg.text_part.charset or "utf-8"
        return msg.text_part.get_payload().decode(charset, "ignore")
    if msg.html_part:
        charset = msg.html_part.charset or "utf-8"
        html = msg.html_part.get_payload().decode(charset, "ignore")
        return html2text(html)
    return ""

# ─── helper: list attachments (filename, bytes) ─────────────────────────
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
                    print(f"    ⚠️ Could not encode attachment '{fn}' payload to bytes: {e}. Skipping.")
                    continue
            yield fn, payload_bytes

# ─── helper: call GLM ───────────────────────────────────────────────────
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
                "max_tokens":32000, # Increased as per original example
                "stream":False},
            headers={"Authorization":f"Bearer {GLM_KEY}"},
            timeout=300)
        res.raise_for_status()
        return res.json()["choices"][0]["message"]["content"]
    except requests.exceptions.RequestException as e:
        print(f"    ‼️ GLM API request failed: {e}")
        return "[]" 
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        response_text = res.text if 'res' in locals() else "N/A (response object not available)"
        print(f"    ‼️ GLM API response format unexpected or not valid JSON: {e} - Response: {response_text[:200]}")
        return "[]"

# ─── helper: parse GLM response ─────────────────────────────────────────
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
            if "<think>" in preceding_text.lower(): 
                print(f"    ℹ️ Stripped preceding LLM thought process/text: '{preceding_text[:100].strip()}...'")
            else:
                print(f"    ℹ️ Stripped preceding non-JSON text: '{preceding_text[:100].strip()}...'")
            cleaned_txt = cleaned_txt[json_start_index:]
        elif json_start_index == -1 :
            if "<think>" in cleaned_txt.lower() :
                print(f"    ⚠️ GLM output appears to be only thought process/text without JSON: '{cleaned_txt[:200].strip()}...'")
            else:
                print(f"    ⚠️ GLM output does not contain valid JSON start character ([ or {{): '{cleaned_txt[:200].strip()}...'")
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

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and expected_keys.issubset(item.keys()):
                    try:
                        item["单位净值"] = float(str(item["单位净值"]).replace(',',''))
                        item["累计净值"] = float(str(item["累计净值"]).replace(',',''))
                        parsed_items.append(item)
                    except (ValueError, TypeError):
                        print(f"    ⚠️ GLM list item skipped (net values not convertible to float): {str(item)[:100]}")
                elif isinstance(item, dict):
                    print(f"    ⚠️ GLM list item skipped (missing expected keys): {str(item)[:100]}")
                else:
                    print(f"    ⚠️ GLM list item skipped (not a dictionary): {str(item)[:100]}")
            return parsed_items
        elif isinstance(data, dict): 
            if expected_keys.issubset(data.keys()):
                try:
                    data["单位净值"] = float(str(data["单位净值"]).replace(',',''))
                    data["累计净值"] = float(str(data["累计净值"]).replace(',',''))
                    return [data] 
                except (ValueError, TypeError):
                    print(f"    ⚠️ GLM dict item skipped (net values not convertible to float): {str(data)[:100]}")
                    return []
            else:
                print(f"    ⚠️ GLM dict skipped (missing expected keys): {str(data)[:100]}")
                return []
        else:
            print(f"    ⚠️ GLM output (after stripping) is valid JSON but not a list or dict: {cleaned_txt[:200]}")
            return []

    except json.JSONDecodeError:
        print(f"    ⚠️ GLM output (after stripping) was not valid JSON. Original start: '{txt[:100].strip()}...'")
        return []
    except Exception as e:
        print(f"    ⚠️ Unexpected error parsing GLM output: {e}. Original start: '{txt[:100].strip()}...'")
        return []

# ─── main workflow ──────────────────────────────────────────────────────
def main():
    # Ensure log directory exists (also created by save_current_run_datetime if needed)
    LOG_DIR.mkdir(parents=True, exist_ok=True) 
    
    last_run_dt_utc = get_last_run_datetime()
    
    rows = []
    # Pass the last run datetime to fetch_mail; default_days_lookback is used if last_run_dt_utc is None
    mail_fetch_iterator = fetch_mail(last_run_utc_dt=last_run_dt_utc, default_days_lookback=30)
    
    actual_emails_processed_count = 0
    if mail_fetch_iterator:
        for loop_idx, msg in enumerate(mail_fetch_iterator, 1):
            actual_emails_processed_count = loop_idx 
            if msg is None: continue

            sender_addresses = msg.get_addresses("from")
            if sender_addresses:
                sender_name, sender_email = sender_addresses[0]
            else:
                sender_name, sender_email = "Unknown Sender", "unknown@example.com"

            subj = msg.get_subject() or "(No Subject)"
            body = get_body(msg)
            atts = list(list_attachments(msg))

            print(f"\n[{actual_emails_processed_count}] Processing: {subj}\n    From: {sender_name} <{sender_email}>\n"
                  f"    Attachments ({len(atts)}): {[fn for fn,_ in atts]}")

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
                            if isinstance(xls_content, dict):
                                combined_df = pd.concat(xls_content.values(), ignore_index=True)
                            else:
                                combined_df = xls_content
                            attach_text = combined_df.to_csv(index=False, header=True)
                        except Exception:
                            try:
                                attach_text = blob.decode("utf-8", "ignore")
                            except UnicodeDecodeError:
                                attach_text = blob.decode("gbk", "ignore") 
                            except Exception:
                                attach_text = "(二进制文件或无法识别编码)"
                    except Exception as e_file:
                        attach_text = f"(附件处理错误: {e_file})"
                    finally:
                        if temp_file_path and pathlib.Path(temp_file_path).exists():
                            pathlib.Path(temp_file_path).unlink()
                
                if fn is None: 
                    prompt_context = f"【邮件正文】\n{body}\n\n"
                else: 
                    prompt_context = f"【邮件正文】\n{body}\n\n【附件: {fn}】\n{attach_text}"

                prompt = (
                    f"邮件主题: {subj}\n"
                    f"发件人: {sender_name} <{sender_email}>\n\n"
                    f"{prompt_context}"
                )
                
                ans = glm(prompt)
                parsed = parse_glm(ans)

                if parsed:
                    print(f"    ↪ GLM parsed {len(parsed)} row(s) from {source_name}")
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
                    print(f"    ↪ 0 rows parsed (or parsing failed) from {source_name}")
    
    if actual_emails_processed_count == 0:
        print("\n👀 No new emails were found and processed in this run.")
        save_current_run_datetime() 
        return

    if not rows:
        print("\n👀 Processed new emails, but no NAV data was captured.")
        save_current_run_datetime() 
        return

    df = pd.DataFrame(rows, columns=COLS)
    df.drop_duplicates(inplace=True) 

    if df.empty:
        print("\n👀 No unique NAV data captured after processing and removing duplicates.")
        save_current_run_datetime() 
        return
    
    file_exists = pathlib.Path(XLSX).exists()
    excel_writer_mode = "a" if file_exists else "w" 
    excel_if_sheet_exists = "replace" # Always replace if sheet exists, relevant for mode 'a'

    try:
        with pd.ExcelWriter(XLSX, engine="openpyxl", mode=excel_writer_mode, 
                            if_sheet_exists=excel_if_sheet_exists) as writer:
            # If mode='w' or file didn't exist, it creates a new file.
            # If mode='a' and sheet exists, it's replaced.
            # If mode='a' and sheet doesn't exist, it's added.
            df.to_excel(writer, index=False, sheet_name=SHEET, header=True)
        print(f"\n✅ {len(df)} unique rows written/updated → {XLSX} (Sheet: {SHEET})")
    except Exception as e:
        print(f"    ‼️ Error writing to Excel '{XLSX}': {e}.")
        timestamp_fallback = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fallback_xlsx = f"{pathlib.Path(XLSX).stem}_fallback_{timestamp_fallback}{pathlib.Path(XLSX).suffix}"
        try:
            df.to_excel(fallback_xlsx, index=False, sheet_name=SHEET)
            print(f"\n⚠️ Data saved to fallback file: {fallback_xlsx}")
        except Exception as fe:
            print(f"    ‼️ Error writing to fallback Excel file '{fallback_xlsx}': {fe}.")
            print(f"    ℹ️ Raw data rows collected ({len(df)}):")
            # Limiting output for very large dataframes
            # for record_idx, record in enumerate(df.to_dict('records')):
            #     if record_idx < 10: # Print first 10 records
            #         print(f"      {record}")
            #     elif record_idx == 10:
            #         print(f"      ... (and {len(df)-10} more records)")
            #         break


    save_current_run_datetime() # Save timestamp after all processing for this run is complete

# ─── run ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user.")
    except Exception as e:
        print(f"\n💥 An unexpected error occurred in main execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # This message now prints regardless of success, interrupt, or error in main()
        print("\n👋 Script execution cycle finished or was terminated.")