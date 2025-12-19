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
SALES_ROOT_FOLDER_ID = "1ge-fbJkuph-B5sGR3GThhIKRr5YKO_rS"          # Raw sales uploads
CONVERTED_FOLDER_ID = "16edTsOusrYf-5LqRgiGqIwMn94H6yzsE"           # Converted CSVs go here
TRACKING_SHEET_ID = "1r872UNCcsgkdEkV9Y9PnNcuTtPrezs0XE3n8HFZgqyM"                   # <<< REPLACE THIS

SERVICE_ACCOUNT_JSON = json.loads(os.environ['SERVICE_ACCOUNT_KEY'])

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']

creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# ================= TRACKING SHEET HELPERS =================
def get_processed_file_ids():
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=TRACKING_SHEET_ID, range="Sheet1!A:D").execute()
        rows = result.get('values', [])
        if len(rows) <= 1:
            return set()
        return {row[0] for row in rows[1:] if len(row) >= 4 and row[3] == "PART1_DONE"}
    except Exception as e:
        print(f"Error reading tracking sheet: {e}")
        return set()

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
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    print(f"Creating folder: {folder_name}")
    metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    folder = drive_service.files().create(body=metadata, fields='id').execute()
    return folder['id']

def get_month_folder_name(filename):
    match = re.search(r'(\d{4})-(\d{2})-\d{2}', filename)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return datetime.date(year, month, 1).strftime('%B %Y')
    return None

# ================= CORE PROCESSING (Your Original Logic) =================
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

def process_file(file_item, destination_folder_id):
    file_id = file_item['id']
    name = file_item['name']
    print(f"Processing → {name}")

    try:
        # Download file
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)

        # Normalize CSV
        normalized_fh = normalize_csv(fh.read())

        # Read into pandas
        df = pd.read_csv(normalized_fh, delimiter='\t', encoding='ISO-8859-1', on_bad_lines='skip')

        # Split quoted columns
        def split_text_in_columns(df, delimiter='"'):
            for col in df.columns:
                if df[col].dtype == 'object':
                    df_split = df[col].str.split(delimiter, expand=True)
                    df_split.columns = [f'{col}_split_{i}' for i in range(len(df_split.columns))]
                    df = pd.concat([df, df_split], axis=1)
            return df

        df = split_text_in_columns(df)

        # Filter Popeyes columns
        pattern = r'(?i).*popeye.*(_0|_1|_3|_5)$'
        df = df[df.filter(regex=pattern).columns]

        # Rename columns with store number
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

        # Add Date_time column
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

        # Standardize spacing
        obj_cols = df.select_dtypes(include=['object']).columns
        for col in obj_cols:
            df[col] = df[col].str.replace(r'\s+,', ',', regex=True)

        # Save and upload
        output_name = "converted_" + name
        local_path = f"/tmp/{output_name}"
        df.to_csv(local_path, index=False)

        file_metadata = {'name': output_name, 'parents': [destination_folder_id]}
        media = MediaFileUpload(local_path, mimetype='text/csv')
        drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        os.remove(local_path)
        print(f"SUCCESS → {output_name}")
        return True

    except Exception as e:
        print(f"FAILED → {name} | {str(e)}")
        return False

# ================= MAIN – Find & Process Only New Files =================
def main():
    processed_ids = get_processed_file_ids()
    new_files = []

    def recursive_collect(folder_id):
        page_token = None
        while True:
            results = drive_service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()
            items = results.get('files', [])
            for item in items:
                if item.get('mimeType') == 'application/vnd.google-apps.folder':
                    recursive_collect(item['id'])
                elif item['name'].lower().endswith('.csv') and not item['name'].startswith('converted_'):
                    if item['id'] not in processed_ids:
                        new_files.append(item)
            page_token = results.get('nextPageToken')
            if not page_token:
                break

    print("Scanning SALES folder for new files...")
    recursive_collect(SALES_ROOT_FOLDER_ID)
    print(f"Found {len(new_files)} new file(s).")

    total = 0
    for file_item in new_files:
        month = get_month_folder_name(file_item['name'])
        target_id = get_or_create_subfolder(CONVERTED_FOLDER_ID, month) if month else CONVERTED_FOLDER_ID

        # Skip if already converted
        dup = drive_service.files().list(
            q=f"'{target_id}' in parents and name='converted_{file_item['name']}' and trashed=false",
            fields="files(id)"
        ).execute().get('files', [])
        if dup:
            log_to_sheet(file_item['id'], file_item['name'], "PART1_DONE")
            continue

        if process_file(file_item, target_id):
            log_to_sheet(file_item['id'], file_item['name'], "PART1_DONE")
            total += 1

    print(f"\nPART1 Complete – Processed {total} new files.")

if __name__ == "__main__":
    main()