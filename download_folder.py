from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import os
import pandas as pd

def get_request(drive_service, mime_type, file_id):
    # If file is Google Sheet
    if(mime_type == "application/vnd.google-apps.spreadsheet"):
        request = drive_service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    # If file is excel, csv or xls
    else:
        request = drive_service.files().get_media(fileId=file_id)
    return request

def get_extention(mime_type):
    if mime_type == "application/vnd.ms-excel":
        return 'xls'
    if mime_type == "text/csv":
        return 'csv'
    else:
        return 'xlsx'
    
def get_subsheet_names(service, file):
    sub_sheet_names = []
    spreadsheet = service.spreadsheets().get(spreadsheetId=file['id']).execute()
    sheets = spreadsheet['sheets']
    for sheet in sheets:
        if 'hidden' not in sheet['properties']:
            sub_sheet_names.append(sheet['properties']['title'])
    return sub_sheet_names
    
def convert_to_xlsx(extention, file_path, output_folder, file_id):
    if extention=='xls':
        df = pd.read_excel(file_path, header=None)
    else:
        df = df = pd.read_csv(file_path, header=None)
    new_file= f"{output_folder}/localspreadsheet{file_id}.xlsx"
    df.to_excel(new_file, index=False, header=False)
    os.remove(file_path)

def download_excel_folder(folder_id, output_folder, credentials_path):
    # Authenticating drive folder
    credentials = service_account.Credentials.from_service_account_info(credentials_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
    drive_service = build('drive', 'v3', credentials=credentials)
    service = build('sheets', 'v4', credentials=credentials)
    
    drive_results = drive_service.files().list(q="'" + folder_id + "' in parents",includeItemsFromAllDrives=True,supportsTeamDrives=True).execute()
    items = drive_results.get('files', [])  

    file_ids, file_names, sub_sheets_list = [], [], []  

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)  

    supported_mime_types = [
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.ms-excel',
        'application/vnd.google-apps.spreadsheet',
        'text/csv'
    ]

    for file in items:
        if file["mimeType"] in supported_mime_types:

            file_id=file['id']
            file_ids.append(file_id)
            file_names.append(file['name'])

            request = get_request(drive_service, file['mimeType'], file_id)
            extention = get_extention(file['mimeType'])

            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_path = os.path.join(output_folder, f"localspreadsheet{file_id}.{extention}")
            with open(file_path, 'wb') as f:
                f.write(fh.getvalue())

            # extract subsheet names from drive
            if file['mimeType'].lower()=="application/vnd.google-apps.spreadsheet":
                sub_sheet_names = get_subsheet_names(service, file)
                sub_sheets_list.append(sub_sheet_names)
            else:
                sub_sheets_list.append([])

            # If File is .xls or .csv then it needs to be converted to .xlsx
            if extention in ["csv", "xls"]:
                convert_to_xlsx(extention, file_path, output_folder, file_id)

    return file_ids, file_names, sub_sheets_list


def download_pdf_or_txt_folder(folder_id, output_folder, credentials_file):
    service = build('drive', 'v3', credentials=service_account.Credentials.from_service_account_info(credentials_file))

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Get the list of files in the folder
    results = service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)", includeItemsFromAllDrives=True,
            supportsTeamDrives=True).execute()
    files = results.get('files', [])

    file_ids = list()
    file_names = list()
    
    for file in files:
        file_id = file['id']
        file_ids.append(file_id)
        file_name = file['name']
        file_names.append(file_name)

        file_path = os.path.join(output_folder, file_id)

        # Download each file
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

    return file_ids, file_names


def download_word_folder(folder_id, output_folder, credentials_file):
    service = build('drive', 'v3', credentials=service_account.Credentials.from_service_account_info(credentials_file))

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Get the list of files in the folder
    results = service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)", includeItemsFromAllDrives=True,
            supportsTeamDrives=True).execute()
    files = results.get('files', [])

    file_ids = list()
    file_names = list()
    
    for file in files:
        file_id = file['id']
        file_ids.append(file_id)
        file_name = file['name']
        file_names.append(file_name)

        file_path = os.path.join(output_folder, file_id)

        # Download each file
        file_metadata = service.files().get(fileId=file_id,supportsAllDrives=True).execute()

        # If File is of Google Docs 
        if file_metadata["mimeType"]=="application/vnd.google-apps.document":
            request = service.files().export(fileId=file_id,mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
        
        # If Microsoft .docx file is uploaded to Google Drive
        elif file_metadata["mimeType"]=="application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            request = service.files().get_media(fileId=file_id)

        else:
            request = service.files().get_media(fileId=file_id)
        
        fh = io.FileIO(f'{file_path}.docx', 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

    return file_ids, file_names