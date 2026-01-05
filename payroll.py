# payroll.py - Automated Payroll Processor (GitHub Actions Version)
import os
import io
import re
import csv
import json
import datetime
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================

# Output Root Folder (Must be in a Shared Drive)
OUTPUT_ROOT_ID = "0AEDJ3Yc9IXQcUk9PVA"

# The Payroll Tracking Sheet ID
TRACKING_SHEET_ID = "1O4aYE5mdXdAXtlvyQfcHoaQtqj3GoEyOOGE_UDK0DfI"

# Auth Setup
SERVICE_ACCOUNT_JSON = json.loads(os.environ['SERVICE_ACCOUNT_KEY'])
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)

def get_service(service_name='drive', version='v3'):
    return build(service_name, version, credentials=creds)

# ==============================================================================
# 2. CORE LOGIC
# ==============================================================================

def extract_start_date(filename):
    """Extracts date from filename like '..._11-3-2025to...'"""
    match = re.search(r'(\d{1,2}[-\/]\d{1,2}[-\/]\d{4})', filename)
    if match:
        date_str = match.group(1).replace('/', '-')
        try:
            return datetime.datetime.strptime(date_str, "%m-%d-%Y")
        except ValueError:
            pass
    return None

def parse_duration_to_decimal(duration_str):
    try:
        duration_str = str(duration_str).strip()
        parts = duration_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        return round(hours + (minutes / 60), 2)
    except:
        return 0.0

def get_week_number(day_str, pay_period_start):
    try:
        if isinstance(day_str, str):
            date_obj = datetime.datetime.strptime(day_str, "%m/%d/%Y")
        else:
            date_obj = day_str
        days_diff = (date_obj - pay_period_start).days
        if 0 <= days_diff <= 6:
            return date_obj, 1
        elif 7 <= days_diff <= 13:
            return date_obj, 2
        else:
            return date_obj, None
    except:
        return None, None

def detect_format_from_content(content):
    if not content: return None
    first_lines = content[:1000]
    if 'Previous Payroll Report' in first_lines or 'Reclose Payroll Report' in first_lines:
        return 'payroll'
    elif 'Timeclock Report' in first_lines or 'All Employees:' in first_lines:
        return 'timeclock'
    if 'Clockset' in first_lines and 'ACTIVE' in first_lines:
        return 'timeclock'
    return 'payroll'

# ==============================================================================
# 3. PARSING FUNCTIONS
# ==============================================================================

def parse_payroll_content(content, header_year_default):
    data = []
    store_no = None
    file_lines = io.StringIO(content)
    
    header_year_match = re.search(r'Period: \d{2}/\d{2}/(\d{4})', content)
    header_year = int(header_year_match.group(1)) if header_year_match else header_year_default

    for line in file_lines:
        line = line.strip()
        if not line: continue
        
        if "Popeye's" in line or "Popeyes" in line:
            match = re.search(r"Popeye's\s*#?\s*(\d+)", line, re.IGNORECASE)
            if not match: match = re.search(r"Popeyes\s*#?\s*(\d+)", line, re.IGNORECASE)
            if match: store_no = match.group(1)
            continue
        
        parts = None
        if '","' in line:
            parts = [p.strip().strip('"') for p in line.split('","')]
            if parts:
                parts[0] = parts[0].lstrip('"')
                parts[-1] = parts[-1].rstrip('"\n')
        elif ',' in line and not line.startswith('"'):
            parts = [p.strip() for p in line.split(',')]

        if parts is None:
            ot_match = re.match(r'^\s*"?(\d+)\s+([\d\.]+)\s*"?$', line)
            if ot_match:
                try:
                    data.append({
                        'emp_id': ot_match.group(1).strip(),
                        'first_name': 'OVERTIME', 'last_name': 'REPORTED',
                        'day': '', 'date': '', 'start_time': '', 'end_time': '',
                        'type': 'Overtime_Reported', 
                        'duration': ot_match.group(2).strip(),
                        'decimal_hours': round(float(ot_match.group(2).strip()), 2),
                        'store_no': store_no
                    })
                except: continue
                continue
            continue 

        day_of_week = parts[0].strip()
        if day_of_week in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
            if len(parts) < 11: continue
            
            date_str = parts[1].strip()
            duration_decimal = parts[3].strip() 
            duration_hhmm = parts[2].strip()
            emp_id = parts[6].strip()
            first_name = parts[8].strip() 
            last_name = parts[9].strip()
            
            emp_id_match = re.search(r'^(\d+)', emp_id)
            emp_id = emp_id_match.group(1) if emp_id_match else ''
            first_name = re.sub(r'--.*', '', first_name).strip()
            last_name = re.sub(r'--.*', '', last_name).strip()
            
            if duration_decimal in ['--', ''] or not emp_id: continue
            
            full_date = date_str
            try:
                if '-' in date_str:
                    if any(m in date_str for m in ['Jan','Feb','Mar']):
                         date_obj = datetime.datetime.strptime(f"{date_str}-{header_year}", "%d-%b-%Y")
                    elif len(date_str.split('-')[0]) <= 2:
                        date_obj = datetime.datetime.strptime(f"{date_str}-{header_year}", "%m-%d-%Y")
                    full_date = date_obj.strftime("%m/%d/%Y")
            except: pass
            
            try:
                decimal_hours = float(duration_decimal)
                data.append({
                    'emp_id': emp_id, 'first_name': first_name, 'last_name': last_name,
                    'day': day_of_week, 'date': full_date,
                    'start_time': '', 'end_time': '', 'type': 'Clockset',
                    'duration': duration_hhmm, 'decimal_hours': round(decimal_hours, 2),
                    'store_no': store_no
                })
            except ValueError: continue

    df = pd.DataFrame(data)
    
    if not df.empty and 'type' in df.columns:
        name_map = df[df['type'] == 'Clockset'].groupby('emp_id').agg(
            first_name=('first_name', 'first'), last_name=('last_name', 'first')
        ).reset_index()

        for index, row in df[df['type'] == 'Overtime_Reported'].iterrows():
            match = name_map[name_map['emp_id'].str.startswith(str(row['emp_id']), na=False)]
            if not match.empty:
                df.loc[index, 'first_name'] = match.iloc[0]['first_name']
                df.loc[index, 'last_name'] = match.iloc[0]['last_name']
            else:
                df.drop(index, inplace=True)
                
    return df, store_no

def parse_timeclock_content(content):
    data = []
    current_emp_id = None
    current_first_name = None
    current_last_name = None
    store_no = None
    
    file_lines = io.StringIO(content)
    
    for line in file_lines:
        line = line.strip()
        if not line: continue
            
        if '","' in line:
            parts = [p.strip().strip('"') for p in line.split('","')]
        else:
            parts = [p.strip() for p in line.split(',')]
        
        if "Popeye's" in parts[0] or 'POPEYES' in parts[0]:
            match = re.search(r"Popeye's\s*#?\s*(\d+)", parts[0], re.IGNORECASE)
            if not match: match = re.search(r'#(\d+)', parts[0])
            if match: store_no = match.group(1)
            continue
        
        if any(k in parts[0] for k in ['Timeclock Summary', 'Total Paid', 'Active Employees', 'Timeclock Report']):
            continue
        
        if parts[0].strip().isdigit() and len(parts) >= 3:
            current_emp_id = parts[0].strip()
            current_first_name = parts[1].strip()
            current_last_name = parts[2].strip()
            continue
        
        if len(parts) >= 6 and current_emp_id:
            day_idx = 2
            if parts[1] in ['*O', '*I', '**']: day_idx = 2
            
            if len(parts) > day_idx + 4:
                day_of_week = parts[day_idx].strip()
                date_str_raw = parts[day_idx+1].strip()
                end_time_raw = parts[day_idx+2].strip()
                entry_type = parts[day_idx+3].strip()
                duration = parts[day_idx+4].strip()
                
                if entry_type in ['Clockset', 'Clockset  ', 'Paid Break']:
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', date_str_raw)
                    clean_date = date_match.group(1) if date_match else ''
                    t1 = re.search(r'(\d{1,2}:\d{2})', date_str_raw)
                    t2 = re.search(r'(\d{1,2}:\d{2})', end_time_raw)
                    start_time = t1.group(1) if t1 else ''
                    end_time = t2.group(1) if t2 else ''
                    decimal_hours = parse_duration_to_decimal(duration)
                    data.append({
                        'emp_id': current_emp_id,
                        'first_name': current_first_name,
                        'last_name': current_last_name,
                        'day': day_of_week, 'date': clean_date,
                        'start_time': start_time, 'end_time': end_time,
                        'type': entry_type.strip(),
                        'duration': duration,
                        'decimal_hours': decimal_hours,
                        'store_no': store_no
                    })
    return pd.DataFrame(data), store_no

# ==============================================================================
# 4. DATAFRAME GENERATORS
# ==============================================================================

def prepare_formatted_df(df, store_no):
    if df.empty: return pd.DataFrame()
    daily_df = df[df['type'].isin(['Clockset', 'Paid Break'])].copy()
    output_df = pd.DataFrame({
        'store_no': store_no if store_no else daily_df.get('store_no', ''),
        'emp_id': daily_df['emp_id'],
        'first_name': daily_df['first_name'],
        'last_name': daily_df['last_name'],
        'day': daily_df['day'],
        'date': daily_df['date'],
        'start_time': daily_df['start_time'],
        'end_time': daily_df['end_time'],
        'type': daily_df['type'],
        'duration': daily_df['decimal_hours']
    })
    return output_df

def prepare_pivot_df(df, store_no, pay_period_start):
    if df.empty: return pd.DataFrame()

    reported_ot_df = df[df['type'] == 'Overtime_Reported'].copy()
    daily_clock_df = df[df['type'].isin(['Clockset', 'Paid Break'])].copy()
    
    if daily_clock_df.empty: return pd.DataFrame()

    daily_clock_df[['date_obj', 'week']] = daily_clock_df['date'].apply(
        lambda x: pd.Series(get_week_number(x, pay_period_start))
    )
    daily_clock_df = daily_clock_df[daily_clock_df['week'].notna()].copy()

    weekly_summary = daily_clock_df.groupby(['emp_id', 'first_name', 'last_name', 'week']).agg(
        weekly_hours=('decimal_hours', 'sum')
    ).reset_index()

    weekly_summary['calc_regular'] = weekly_summary['weekly_hours'].apply(lambda x: min(x, 40))
    weekly_summary['calc_overtime'] = weekly_summary['weekly_hours'].apply(lambda x: max(0, x - 40))

    pivot = weekly_summary.groupby(['emp_id', 'first_name', 'last_name']).agg(
        total_hours=('weekly_hours', 'sum'),
        regular=('calc_regular', 'sum'),
        overtime=('calc_overtime', 'sum')
    ).reset_index()

    if not reported_ot_df.empty:
        reported_ot_summary = reported_ot_df.groupby('emp_id').agg(
            reported_overtime=('decimal_hours', 'sum')
        ).reset_index()
        pivot = pd.merge(pivot, reported_ot_summary, on='emp_id', how='left')
        pivot['overtime'] = pivot['reported_overtime'].fillna(pivot['overtime'])
        pivot['regular'] = pivot['total_hours'] - pivot['overtime']
        pivot.drop(columns=['reported_overtime'], inplace=True)

    pivot['store_no'] = store_no
    pivot['name'] = '' 
    pivot = pivot[['store_no', 'name', 'total_hours', 'regular', 'overtime', 'emp_id', 'first_name', 'last_name']]
    pivot.columns = ['store no', 'name', 'total', 'regular', 'overtime', 'id', 'first name', 'last name']
    
    cols = ['total', 'regular', 'overtime']
    pivot[cols] = pivot[cols].round(2)
    
    return pivot

# ==============================================================================
# 5. AUTOMATION INFRASTRUCTURE (Drive & Sheets)
# ==============================================================================

def get_pending_payroll_uploads():
    """
    Returns a list of tuples: (file_id, file_name, row_index)
    Only selects rows where Status (Col D) is 'PAYROLL UPLOADED'.
    """
    try:
        service = get_service('sheets', 'v4')
        result = service.spreadsheets().values().get(
            spreadsheetId=TRACKING_SHEET_ID,
            range="Sheet1!A:D"
        ).execute()
        rows = result.get('values', [])
        
        pending = []
        if len(rows) > 1:
            for i, row in enumerate(rows):
                current_row_num = i + 1
                if current_row_num == 1: continue

                if len(row) >= 4 and row[3] == "PAYROLL UPLOADED":
                    file_id = row[0]
                    file_name = row[1] if len(row) > 1 else "Unknown.csv"
                    pending.append((file_id, file_name, current_row_num))
        return pending
    except Exception as e:
        print(f"Error reading tracking sheet: {e}")
        return []

def mark_payroll_status(row_num, status_message):
    """Updates column D (Status) of the SPECIFIC row number."""
    try:
        service = get_service('sheets', 'v4')
        range_name = f"Sheet1!D{row_num}"
        body = {'values': [[status_message]]}
        service.spreadsheets().values().update(
            spreadsheetId=TRACKING_SHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()
        print(f"   -> Row {row_num} updated to: {status_message}")
    except Exception as e:
        print(f"Error updating status for row {row_num}: {e}")

def get_file_content(file_id):
    try:
        service = get_service('drive', 'v3')
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f"Error downloading {file_id}: {e}")
        return None

def get_or_create_folder(parent_id, folder_name):
    service = get_service('drive', 'v3')
    query = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
    
    results = service.files().list(
        q=query, 
        fields="files(id)", 
        includeItemsFromAllDrives=True, 
        supportsAllDrives=True
    ).execute()
    files = results.get('files', [])
    
    if files:
        return files[0]['id']
    else:
        metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = service.files().create(
            body=metadata, 
            fields='id', 
            supportsAllDrives=True
        ).execute()
        return folder['id']

def upload_csv_to_drive(df, filename, folder_id):
    if df.empty: return
    service = get_service('drive', 'v3')
    
    query = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    results = service.files().list(
        q=query, 
        fields="files(id)", 
        includeItemsFromAllDrives=True, 
        supportsAllDrives=True
    ).execute()
    
    if results.get('files'):
        print(f"   - File exists (Updating): {filename}")
        file_id = results.get('files')[0]['id']
        service.files().delete(fileId=file_id, supportsAllDrives=True).execute()

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    media = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode('utf-8')), mimetype='text/csv')
    metadata = {'name': filename, 'parents': [folder_id]}
    
    service.files().create(
        body=metadata, 
        media_body=media, 
        fields='id', 
        supportsAllDrives=True
    ).execute()
    print(f"   - Uploaded: {filename}")

# ==============================================================================
# 6. MAIN EXECUTION
# ==============================================================================

def main():
    print(">>> Starting Payroll Automation (GitHub Actions)...")
    pending_files = get_pending_payroll_uploads()
    if not pending_files:
        print("No new payroll files to process.")
        return

    print(f"Found {len(pending_files)} pending payroll files.")

    for file_id, file_name, row_num in pending_files:
        print(f"\nProcessing Row {row_num}: {file_name}")
        
        # --- SAFE PROCESS BLOCK ---
        try:
            # 1. Download Content
            content = get_file_content(file_id)
            if not content:
                mark_payroll_status(row_num, "PAYROLL FAULTY: Download Failed")
                continue

            # 2. Extract Date (Auto-detection)
            pay_period_start = extract_start_date(file_name)
            if not pay_period_start:
                mark_payroll_status(row_num, "PAYROLL FAULTY: Bad Date")
                continue

            # 3. Detect Format & Parse
            try:
                fmt = detect_format_from_content(content)
                df = pd.DataFrame()
                store_no = None

                if fmt == 'payroll':
                    df, store_no = parse_payroll_content(content, pay_period_start.year)
                elif fmt == 'timeclock':
                    df, store_no = parse_timeclock_content(content)
                
                if not store_no:
                    match = re.search(r'^(\d+)', file_name)
                    store_no = match.group(1) if match else "Unknown_Store"

                if df.empty:
                    mark_payroll_status(row_num, "PAYROLL FAULTY: Empty Data")
                    continue

            except Exception as e:
                print(f"Parse error for {file_name}: {e}")
                mark_payroll_status(row_num, "PAYROLL FAULTY: Parse Error")
                continue

            # 4. Generate & Upload
            formatted_df = prepare_formatted_df(df, store_no)
            pivot_df = prepare_pivot_df(df, store_no, pay_period_start)

            store_folder_id = get_or_create_folder(OUTPUT_ROOT_ID, str(store_no))
            
            base_name = file_name.replace('.csv', '')
            upload_csv_to_drive(formatted_df, f"{base_name}_Formatted.csv", store_folder_id)
            upload_csv_to_drive(pivot_df, f"{base_name}_Pivot.csv", store_folder_id)

            # 5. Success
            mark_payroll_status(row_num, "PAYROLL DONE")
            print(f"Completed: {file_name}")

        except Exception as e:
            # Catch-all for any other crash to prevent stopping the whole script
            print(f"Critical error on file {file_name}: {e}")
            mark_payroll_status(row_num, "PAYROLL FAULTY: Critical Error")
            continue

if __name__ == "__main__":
    main()
