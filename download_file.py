from google.oauth2 import service_account
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import pandas as pd
import os
import uuid
import requests
from utils import utils
import traceback
import subprocess
import platform

def get_libreoffice_path():
    """
    Determine the LibreOffice executable path based on the operating system.
    """
    if platform.system() == "Windows":
        return r"C:\Program Files\LibreOffice\program\soffice.exe"
    elif platform.system() == "Darwin":
        return r"/Applications/LibreOffice.app/Contents/MacOS/soffice" #Mac OS
    else:
        return "/usr/bin/libreoffice" #Linux

def convert_to_new_format(ext,file_path):

    # If File is .xls then it needs to be converted to .xlsx
    if ext=='xls':
        df = pd.read_excel(file_path, header=None)
        new_file= f'localspreadsheet{str(uuid.uuid4())}.xlsx'
                    
        df.to_excel(new_file, index=False, header=False)
        os.remove(file_path)
        file_path = new_file
    
    # If File is .csv then it needs to be converted to .xlsx
    if ext=='csv':
        df = pd.read_csv(file_path, header=None)
        new_file= f'localspreadsheet{str(uuid.uuid4())}.xlsx'
                    
        df.to_excel(new_file, index=False, header=False)
        os.remove(file_path)
        file_path = new_file
    
    # If File is .ppt then it needs to be converted to .pptx
    if ext=='ppt':

        output_path = file_path.replace('.ppt', '.pptx')
        current_dir = os.getcwd()

        libreoffice_path = get_libreoffice_path()
        # Convert using LibreOffice command-line interface
        subprocess.run([libreoffice_path, '--headless', '--convert-to', 'pptx', file_path, '--outdir', current_dir])

        os.remove(file_path)
        file_path = output_path
    
    # If File is .doc then it needs to be converted to .docx
    if ext=='doc':
        output_path = file_path.replace('.doc', '.docx')
        current_dir = os.getcwd()

        libreoffice_path = get_libreoffice_path()
        # Convert using LibreOffice command-line interface
        subprocess.run([libreoffice_path, '--headless', '--convert-to', 'docx', file_path, '--outdir', current_dir])

        os.remove(file_path)
        file_path = output_path
    
    return file_path
        

def download_spreadsheet(file_id, credentials_path):
    credentials = service_account.Credentials.from_service_account_info(credentials_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
    drive_service = build('drive', 'v3', credentials=credentials)

    file_metadata = drive_service.files().get(fileId=file_id,supportsAllDrives=True).execute()

    file_name = file_metadata['name']
    
    ext='xlsx'

    # Mime Type if file is .xls
    if file_metadata["mimeType"]=="application/vnd.ms-excel":
        request = drive_service.files().get_media(fileId=file_id)
        ext='xls'

    # If File is of Google SHeet 
    elif file_metadata["mimeType"]=="application/vnd.google-apps.spreadsheet":
        request = drive_service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    # If File is .csv
    elif file_metadata["mimeType"]=="text/csv":
        request = drive_service.files().get_media(fileId=file_id)
        ext='csv'

    # If Microsoft Xlsx file is uploaded to Google Drive
    else:
        request = drive_service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_path=f'localspreadsheet{file_id}{str(uuid.uuid4())}.{ext}' 

    with open(file_path, 'wb') as f:
        f.write(fh.getvalue())
    
    if ext in ['csv','xls']:
        file_path=convert_to_new_format(ext,file_path)
    
    # extract subsheet names from drive
    sub_sheet_names = list()
    if(file_metadata['mimeType'].lower()=="application/vnd.google-apps.spreadsheet"):
        service = build('sheets', 'v4', credentials=credentials)
        spreadsheet = service.spreadsheets().get(spreadsheetId=file_id).execute()
        sheets = spreadsheet['sheets']
        for sheet in sheets:
            if 'hidden' not in sheet['properties']:
                sub_sheet_names.append(sheet['properties']['title'])

    return file_path,file_name,sub_sheet_names

def download_pdf_file(file_id, credentials_file):
    service = build('drive', 'v3', credentials=service_account.Credentials.from_service_account_info(credentials_file))

    file_metadata = service.files().get(fileId=file_id,supportsAllDrives=True).execute()
    file_name = file_metadata['name']

    file_path=f'local_pdf_file_{file_id}{str(uuid.uuid4())}.pdf'

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    return file_path,file_name
            
def download_json_file(file_id, credentials_file):
    service = build('drive', 'v3', credentials=service_account.Credentials.from_service_account_info(credentials_file))

    file_metadata = service.files().get(fileId=file_id,supportsAllDrives=True).execute()
    file_name = file_metadata['name']

    file_path=f'local_json_file_{file_id}{str(uuid.uuid4())}.json'

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    return file_path,file_name

def download_txt_file(file_id, credentials_file):
    service = build('drive', 'v3', credentials=service_account.Credentials.from_service_account_info(credentials_file))

    file_metadata = service.files().get(fileId=file_id,supportsAllDrives=True).execute()
    file_name = file_metadata['name']

    file_path=f'local_txt_file_{file_id}{str(uuid.uuid4())}.txt'

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    return file_path,file_name

def download_word_file(file_id, credentials_file):
    service = build('drive', 'v3', credentials=service_account.Credentials.from_service_account_info(credentials_file))

    file_metadata = service.files().get(fileId=file_id,supportsAllDrives=True).execute()
 
    file_name = file_metadata['name']
    ext='docx'

    # If File is of Google Docs 
    if file_metadata["mimeType"]=="application/vnd.google-apps.document":
        request = service.files().export(fileId=file_id,mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
    
    # If Microsoft .docx file is uploaded to Google Drive
    elif file_metadata["mimeType"]=="application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        request = service.files().get_media(fileId=file_id)
    
    else:
        ext='doc'
        request = service.files().get_media(fileId=file_id)
    
    file_path=f'local_word_file_{file_id}{str(uuid.uuid4())}.{ext}'

    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    fh.close()

    # If File is .doc then it needs to be converted to .docx
    if ext=='doc':
        file_path=convert_to_new_format(ext,file_path)

    return file_path,file_name

def download_ppt_file(file_id, credentials_file):
    service = build('drive', 'v3', credentials=service_account.Credentials.from_service_account_info(credentials_file))

    file_metadata = service.files().get(fileId=file_id,supportsAllDrives=True).execute()
 
    file_name = file_metadata['name']
    ext='pptx'

    if file_metadata["mimeType"] == 'application/vnd.openxmlformats-officedocument.presentationml.presentation':
        request = service.files().get_media(fileId=file_id)
    # If File is .ppt
    else:
        ext = 'ppt'
        request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.presentationml.presentation')
    
    file_path=f'local_ppt_file_{file_id}{str(uuid.uuid4())}.{ext}'

    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    fh.close()

    # If File is .ppt then it needs to be converted to .pptx
    if ext=='ppt':
        file_path=convert_to_new_format(ext,file_path)

    return file_path,file_name

def download_sharepoint_file(url):
    try:
        
        head_response = requests.head(url)

        file_name=head_response.headers['Content-Disposition'].split(';')[-1][len('filename="'):][:-1]

        file_path=f"local_sharepoint_{str(uuid.uuid4())}_{file_name}"

        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(file_path, 'wb') as file:
            file.write(response.content)

        ext=file_name.split('.')[-1]
        if ext in ['xls','csv','ppt','doc']:
            file_path=convert_to_new_format(ext,file_path)
        
        return file_path,file_name

    except Exception as e:
        utils.exception_handling("Exception in processing file from sharepoint url", {"url": url}, e, traceback.print_exc())
        return None,None

def download_bucket_file(url):
    try:
        
        storage_client = storage.Client()
        url=url.replace('%5C','\\')

        url_split=url.split('/')

        bucket_name=url_split[3]
        bucket = storage_client.bucket(bucket_name)

        file_name=url_split[-1]
        file_path=f"local_google_bucket_{str(uuid.uuid4())}_{file_name}"

        source_blob_name='/'.join(url_split[4:])
        blob = bucket.blob(source_blob_name)

        blob.download_to_filename(file_path)

        ext=file_name.split('.')[-1]

        if ext in ['xls','csv','ppt','doc']:
            file_path=convert_to_new_format(ext,file_path)

        return file_path,file_name

    except Exception as e:
        utils.exception_handling("Exception in processing file from google bucket url", {"url": url}, e, traceback.print_exc())
        return None,None