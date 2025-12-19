# part1.py
import os
import io
import csv
import re
import datetime
import json
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# ================= CONFIGURATION =================
SALES_ROOT_FOLDER_ID = "1ge-fbJkuph-B5sGR3GThhIKRr5YKO_rS"
CONVERTED_FOLDER_ID = "16edTsOusrYf-5LqRgiGqIwMn94H6yzsE"
TRACKING_SHEET_ID = "1r872UNCcsgkdEkV9Y9PnNcuTtPrezs0XE3n8HFZgqyM"  # Your tracking sheet

SERVICE_ACCOUNT_JSON = json.loads(os.environ['SERVICE_ACCOUNT_KEY'])

SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# ================= TRACKING SHEET HELPERS =================
def get_uploaded_files():
    """Return list of (file_id, file_name) that are UPLOADED and not yet processed"""
    try:
        result = sheets_service.spreadsheets().values().get(
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

def log_to_sheet(file_id, file_name, stage):
    timestamp = datetime.datetime.now().isoformat()
    body = {'values': [[file_id, file_name, timestamp, stage]]}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=TRACKING_SHEET_ID,
        range="Sheet1!A:D",
        valueInputOption="RAW",
        body=body
    ).execute()

# ================= DRIVE HELPERS =================
def get_or_create_subfolder(parent_id, folder_name):
    query = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
    response = drive_service.files().list(q=query, fields="files(id)").execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    print(f"Creating folder: {folder_name}")
    metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = drive_service.files().create(body=metadata, fields='id').execute()
    return folder['id']

def get_month_folder_name(filename):
    match = re.search(r'(\d{4})-(\d{2})-\d{2}', filename)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        return datetime.date(year, month, 1).strftime('%B %Y')
    return None

# ================= CORE PROCESSING =================
def normalize_csv(content_bytes):
    text = content_bytes.decode('ISO-8859-1')
    input_io = io.StringIO(text)
    reader = csv.reader(input_io)
    output_io = io.StringIO()
    writer = csv.writer(output_io, quoting=csv.QUOTE_ALL)
    for row in reader:
        writer.writerow(row)
    output_io.seek(0)
    return output_io

def process_file(file_id, original_name):
    print(f"Processing new upload: {original_name} (ID: {file_id})")

    try:
        # Download
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)

        # Normalize and read
        normalized_fh = normalize_csv(fh.read())
        df = pd.read_csv(normalized_fh, delimiter='\t', encoding='ISO-8859-1', on_bad_lines='skip')

        # Your full original cleaning logic
        def split_text_in_columns(df, delimiter='"'):
            for col in df.columns:
                if df[col].dtype == 'object':
                    df_split = df[col].str.split(delimiter, expand=True)
                    df_split.columns = [f'{col}_split_{i}' for i in range(len(df_split.columns))]
                    df = pd.concat([df, df_split], axis=1)
            return df

        df = split_text_in_columns(df)

        pattern = r'(?i).*popeye.*(_0|_1|_3|_5)$'
        df = df[df.filter(regex=pattern).columns]

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
                def check_m(string):
                    return "y" if "M  ," in str(string) else "n"
                df['flag'] = df[target].apply(check_m)
                df['rn'] = range(1, len(df) + 1)
                df['group'] = df['rn'].where(df['flag'] == 'y').ffill().fillna(1)
                valid_indices = df['group'].astype(int) - 1
                dt_vals = df[target].iloc[valid_indices].values
                pos = df.columns.get_loc(target)
                df.insert(pos, 'Date_time', dt_vals)
                df.drop(columns=['flag', 'rn', 'group'], inplace=True, errors='ignore')

        obj_cols = df.select_dtypes(include=['object']).columns
        for col in obj_cols:
            df[col] = df[col].str.replace(r'\s+,', ',', regex=True)

        # Save converted
        month = get_month_folder_name(original_name)
        target_id = get_or_create_subfolder(CONVERTED_FOLDER_ID, month) if month else CONVERTED_FOLDER_ID

        output_name = "converted_" + original_name
        local_path = f"/tmp/{output_name}"
        df.to_csv(local_path, index=False)

        media = MediaFileUpload(local_path, mimetype='text/csv')
        drive_service.files().create(body={'name': output_name, 'parents': [target_id]}, media_body=media).execute()

        os.remove(local_path)
        print(f"SUCCESS → {output_name}")
        return True

    except Exception as e:
        print(f"FAILED → {original_name} | {str(e)}")
        return False

# ================= MAIN =================
def main():
    uploaded_files = get_uploaded_files()
    
    if not uploaded_files:
        print("No new uploaded files to process.")
        return

    print(f"Found {len(uploaded_files)} new file(s) to convert.")

    success_count = 0
    for file_id, file_name in uploaded_files:
        if process_file(file_id, file_name):
            log_to_sheet(file_id, file_name, "PART1_DONE")
            success_count += 1

    print(f"\nPART1 Complete – Successfully converted {success_count} file(s).")

if __name__ == "__main__":
    main()
