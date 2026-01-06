# part1.py - Advanced Intelligent Cleanup & Conversion (GitHub Actions Version)
import os
import io
import csv
import re
import datetime
import json
import pandas as pd
import threading
import concurrent.futures
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SALES_ROOT_FOLDER_ID = "1ge-fbJkuph-B5sGR3GThhIKRr5YKO_rS"
CONVERTED_FOLDER_ID = "1F_fA9P01SKnP0SO64fLe3gAvWwGVjPwd"
TRACKING_SHEET_ID = "1r872UNCcsgkdEkV9Y9PnNcuTtPrezs0XE3n8HFZgqyM"   # For UPLOADED → PART1_DONE
LOG_SHEET_ID = "1XhdFj-fpINNJVveiEk_Qp2FRD-4CV6a1GnUKF7RWlVk"         # Your duplicate/invalid log sheet
MAX_WORKERS = 15  # Safe for GitHub Actions runner

SERVICE_ACCOUNT_JSON = json.loads(os.environ['SERVICE_ACCOUNT_KEY'])
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)

# Thread-local storage
thread_local = threading.local()
log_lock = threading.Lock()
log_entries = []  # For duplicate/invalid log sheet

# ==============================================================================
# SERVICES
# ==============================================================================
def get_service(service_name='drive', version='v3'):
    key = f"service_{service_name}_{version}"
    if not hasattr(thread_local, key):
        setattr(thread_local, key, build(service_name, version, credentials=creds))
    return getattr(thread_local, key)

# ==============================================================================
# TRACKING SHEET (UPLOADED → PART1_DONE)
# ==============================================================================
def get_uploaded_files():
    """Get list of (file_id, file_name) marked as UPLOADED"""
    try:
        service = get_service('sheets', 'v4')
        result = service.spreadsheets().values().get(
            spreadsheetId=TRACKING_SHEET_ID,
            range="Sheet1!A:D"
        ).execute()
        rows = result.get('values', [])
        if len(rows) <= 1:
            return []
        uploaded = []
        for row in rows[1:]:
            if len(row) >= 4 and row[3] == "UPLOADED":
                file_id = row[0]
                file_name = row[1] if len(row) > 1 else "Unknown"
                uploaded.append((file_id, file_name))
        return uploaded
    except Exception as e:
        print(f"Error reading tracking sheet: {e}")
        return []

def mark_as_done(file_id, file_name):
    """Mark file as PART1_DONE in tracking sheet"""
    try:
        service = get_service('sheets', 'v4')
        timestamp = datetime.datetime.now().isoformat()
        body = {'values': [[file_id, file_name, timestamp, "PART1_DONE"]]}
        service.spreadsheets().values().append(
            spreadsheetId=TRACKING_SHEET_ID,
            range="Sheet1!A:D",
            valueInputOption="RAW",
            body=body
        ).execute()
    except Exception as e:
        print(f"Error marking done: {e}")

# ==============================================================================
# LOGGING TO DUPLICATE/INVALID SHEET
# ==============================================================================
def add_log(store, month, filename, deleted_ts_status, full_sheet_status):
    with log_lock:
        log_entries.append([
            str(store),
            str(month) if month else "Unknown",
            str(filename),
            str(deleted_ts_status),
            str(full_sheet_status)
        ])

def flush_logs_to_sheet():
    with log_lock:
        if not log_entries:
            return
        try:
            service = get_service('sheets', 'v4')
            body = {'values': log_entries}
            service.spreadsheets().values().append(
                spreadsheetId=LOG_SHEET_ID,
                range="Sheet1!A:E",
                valueInputOption="RAW",
                body=body
            ).execute()
            print(f"Logged {len(log_entries)} entries to log sheet.")
            log_entries.clear()
        except Exception as e:
            print(f"Log flush error: {e}")

# ==============================================================================
# CORE CLEANUP & CONVERSION LOGIC (Your Advanced Code)
# ==============================================================================
def get_file_content(file_id):
    try:
        service = get_service()
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode('ISO-8859-1')
    except Exception as e:
        print(f"Download failed {file_id}: {e}")
        return None

def parse_pos_csv(content_str):
    if not content_str or "Order #:" not in content_str:
        return None, None
    lines = content_str.splitlines(keepends=True)
    header_lines = lines[:2]
    blocks = []
    current_block = {'lines': []}
    block_start_pattern = re.compile(r'^"?[A-Za-z]{3}\s[A-Za-z]{3}\s\d{1,2},\s\d{4}')
    for i in range(2, len(lines)):
        line = lines[i]
        if block_start_pattern.match(line):
            if current_block['lines']:
                blocks.append(process_block(current_block))
            current_block = {'lines': [line]}
        else:
            current_block['lines'].append(line)
    if current_block['lines']:
        blocks.append(process_block(current_block))
    return header_lines, blocks

def process_block(block_dict):
    first_line = block_dict['lines'][0]
    try:
        reader = csv.reader([first_line])
        row = next(reader)
        timestamp = row[0].strip()
        is_log_on = "LOG ON" in first_line
        order_num = None
        if "Order #:" in row:
            idx = row.index("Order #:")
            if idx + 1 < len(row):
                order_num = row[idx + 1].strip()
        unique_id = (timestamp, order_num) if not is_log_on and order_num and order_num not in ['-', ''] else None
        return {'id': unique_id, 'lines': block_dict['lines'], 'is_log_on': is_log_on, 'timestamp': timestamp}
    except:
        return {'id': None, 'lines': block_dict['lines'], 'is_log_on': False, 'timestamp': "Unknown"}

def get_header_signature(blocks):
    for b in blocks:
        if not b['is_log_on'] and b['id']:
            return b['id']
    return None

def normalize_csv_from_string(content_str):
    input_io = io.StringIO(content_str)
    reader = csv.reader(input_io)
    output_io = io.StringIO()
    writer = csv.writer(output_io, quoting=csv.QUOTE_ALL)
    for row in reader:
        writer.writerow(row)
    output_io.seek(0)
    return output_io

def convert_to_final_format(content_str, file_name):
    try:
        normalized_io = normalize_csv_from_string(content_str)
        df = pd.read_csv(normalized_io, delimiter='\t', on_bad_lines='skip', encoding='utf-8')

        def split_text_in_columns(df, delimiter='"'):
            for col in df.columns:
                if df[col].dtype == 'object' and df[col].str.contains(delimiter, na=False).any():
                    df_split = df[col].str.split(delimiter, expand=True)
                    df_split.columns = [f'{col}_split_{i}' for i in range(len(df_split.columns))]
                    df = pd.concat([df, df_split], axis=1)
            return df

        def check_m(string):
            s = str(string)
            if "M  ," in s:
                return "y"
            if re.search(r'[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}', s):
                return "y"
            return "n"

        df = split_text_in_columns(df)
        pattern = r'(?i).*Popeye.*(_0|_1|_3|_5)$'
        cols_to_keep = [c for c in df.columns if re.search(pattern, c)]
        if not cols_to_keep:
            return None
        df = df[cols_to_keep]

        rename_map = {}
        for col in df.columns:
            if '_split_' in col:
                parts = col.split('_split_')
                orig = parts[0]
                suffix = parts[-1]
                nums = re.findall(r'\d+', orig)
                if nums:
                    rename_map[col] = f'POPEYES # {nums[-1]}_split_{suffix}'
        df.rename(columns=rename_map, inplace=True)

        if not df.empty:
            col0 = [c for c in df.columns if re.search(r'(?i)popeye.*_0$', c)]
            if col0:
                target = col0[0]
                df['flag'] = df[target].apply(check_m)
                df['rn'] = range(1, len(df)+1)
                df['group'] = df['rn'].where(df['flag'] == 'y').ffill().fillna(1)
                valid_indices = df['group'].astype(int) - 1
                valid_indices = valid_indices.clip(0, len(df)-1)
                dt_vals = df[target].iloc[valid_indices].values
                pos = df.columns.get_loc(target)
                df.insert(pos, 'Date_time', dt_vals)
                df.drop(columns=['flag','rn','group'], inplace=True, errors='ignore')

        obj_cols = df.select_dtypes(include=['object']).columns
        for col in obj_cols:
            df[col] = df[col].str.replace(r'\s+,', ',', regex=True)

        output_buffer = io.StringIO()
        df.to_csv(output_buffer, index=False)
        return output_buffer.getvalue()
    except Exception as e:
        print(f"Conversion error {file_name}: {e}")
        return None

def get_month_folder_name(filename):
    match = re.search(r'(\d{4})-(\d{2})-\d{2}', filename)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        return datetime.date(year, month, 1).strftime('%B %Y')
    return None

def get_or_create_subfolder(parent_id, folder_name):
    service = get_service()
    query = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
    response = service.files().list(q=query, fields="files(id)").execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=metadata, fields='id').execute()
    return folder['id']

# ==============================================================================
# MAIN: Process Only New Uploaded Files
# ==============================================================================
def main():
    uploaded_files = get_uploaded_files()
    if not uploaded_files:
        print("No new uploaded files to process.")
        return

    print(f"Found {len(uploaded_files)} new file(s) to clean and convert.")

    processed_count = 0
    for file_id, file_name in uploaded_files:
        print(f"\nProcessing: {file_name} (ID: {file_id})")

        content = get_file_content(file_id)
        if not content:
            add_log("Unknown", "Unknown", file_name, "Download Failed", "Yes")
            mark_as_done(file_id, file_name)
            continue

        headers, blocks = parse_pos_csv(content)
        if headers is None:
            month = get_month_folder_name(file_name)
            add_log("Unknown", month, file_name, "N/A", "Yes (Invalid Structure)")
            mark_as_done(file_id, file_name)
            continue

        # Deduplication & cleaning logic
        seen_orders = {}
        new_blocks = []
        deleted_details = []

        for block in blocks:
            bid = block['id']
            if bid:
                if bid in seen_orders:
                    if seen_orders[bid] != file_name:
                        timestamp, order_num = bid
                        details = f"[{timestamp} | Order #{order_num} | Dup of: {seen_orders[bid]}]"
                        deleted_details.append(details)
                else:
                    seen_orders[bid] = file_name
                    new_blocks.append(block)
            else:
                new_blocks.append(block)

        real_orders = any(b['id'] for b in new_blocks)
        if not real_orders:
            month = get_month_folder_name(file_name)
            add_log("Unknown", month, file_name, f"Yes ({len(deleted_details)} deleted)", "Yes (Empty after Clean)")
            mark_as_done(file_id, file_name)
            continue

        cleaned_content = "".join(headers) + "".join(["".join(b['lines']) for b in new_blocks])

        ts_status = "\n".join(deleted_details) if deleted_details else "No"
        if len(ts_status) > 49000:
            ts_status = ts_status[:49000] + "\n... [TRUNCATED]"
        month = get_month_folder_name(file_name)
        add_log("Unknown", month, file_name, ts_status, "No")

        # Convert
        csv_output = convert_to_final_format(cleaned_content, file_name)
        if not csv_output:
            mark_as_done(file_id, file_name)
            continue

        # Upload
        month_name = get_month_folder_name(file_name)
        target_id = get_or_create_subfolder(CONVERTED_FOLDER_ID, month_name) if month_name else CONVERTED_FOLDER_ID
        output_name = "converted_" + file_name

        service = get_service()
        existing = service.files().list(
            q=f"'{target_id}' in parents and name='{output_name}' and trashed=false",
            fields="files(id)"
        ).execute().get('files', [])

       if not existing:
    # CORRECT: MediaIoBaseUpload handles in-memory BytesIO objects
    media = MediaIoBaseUpload(io.BytesIO(csv_output.encode('utf-8')), mimetype='text/csv', resumable=True)
    service.files().create(body={'name': output_name, 'parents': [target_id]}, media_body=media).execute()
    print(f"Uploaded: {output_name}")

        mark_as_done(file_id, file_name)
        processed_count += 1

        flush_logs_to_sheet()

    flush_logs_to_sheet()
    print(f"\nPART1 Complete – Processed {processed_count} new file(s).")

if __name__ == "__main__":
    main()

