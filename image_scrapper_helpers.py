import random
import json
import requests
from urllib.parse import urlparse, urljoin
import threading
import os
import uuid
import cairosvg
from selenium.webdriver.firefox.service import Service as FirefoxService
import platform
from selenium import webdriver
import json_repair
from PIL import Image


class ValidationConfig:
    def __init__(self):
        self.SOCIAL_MEDIA_DOMAINS={"facebook.com", 
                                "twitter.com", 
                                "instagram.com", 
                                "linkedin.com", 
                                "tiktok.com", 
                                "youtube.com",
                                "pinterest.com",
                                "snapchat.com",
                                "reddit.com"}
        self.block_types={
            
            # "application/EDI-X12",
            # "application/EDIFACT",
            # "application/javascript",
            # "application/octet-stream",
            # "application/ogg",
            "application/pdf",
            # "application/xhtml+xml",
            # "application/x-shockwave-flash",
            "application/json",
            # "application/ld+json",
            # "application/xml",
            "application/zip",
            # "application/x-www-form-urlencoded",

            "audio/mpeg",
            "audio/x-ms-wma",
            "audio/vnd.rn-realaudio",
            "audio/x-wav",

            "image/gif",
            "image/jpeg",
            "image/png",
            "image/tiff",
            "image/vnd.microsoft.icon",
            "image/x-icon",
            "image/vnd.djvu",
            "image/svg+xml",

            # "multipart/mixed",
            # "multipart/alternative",
            # "multipart/related",
            # "multipart/form-data",

            # "text/css",
            # "text/csv",
            # "text/html",
            # "text/javascript",
            # "text/plain",
            # "text/xml",

            "video/mpeg",
            "video/mp4",
            "video/quicktime",
            "video/x-ms-wmv",
            "video/x-msvideo",
            "video/x-flv",
            "video/webm",

            "application/vnd.oasis.opendocument.text",
            "application/vnd.oasis.opendocument.spreadsheet",
            "application/vnd.oasis.opendocument.presentation",
            "application/vnd.oasis.opendocument.graphics",
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-powerpoint",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/msword",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.mozilla.xul+xml",

                    }


def png_from_svg_tag(tag,folder_name="svg_to_png", prefix=None):    
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
    img_name=os.path.join(folder_name, f"{prefix}_{uuid.uuid4()}.png")
    cairosvg.svg2png(bytestring=tag.encode('utf-8'), write_to=img_name)
    with Image.open(img_name) as img:
        width, height = img.size
        if (width<200) or (height<200):
            cairosvg.svg2png(bytestring=tag.encode('utf-8'), write_to=img_name, scale=(200//(min(height, width))))
    return img_name

def png_from_svg_url(url, folder_name="svg_to_png", prefix=None):
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)        
    try:
        response = requests.get(url, stream=True,timeout=15,headers={'User-Agent':user_agent()})
    except:
        response = requests.get(url, stream=True,timeout=15)
    response.raise_for_status()
    content_type=(f"{response.headers.get('Content-Type', '').lower()}")
    
    if content_type=="image/svg+xml":
        svg_data=response.content
        img_name=os.path.join(folder_name, f"{prefix}_{uuid.uuid4()}.png")
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)
        cairosvg.svg2png(bytestring=svg_data, write_to=img_name)
        with Image.open(img_name) as img:
            width, height = img.size
            if (width<200) or (height<200):
                cairosvg.svg2png(bytestring=svg_data, write_to=img_name, scale=(200//(min(height, width))))
        return img_name


def user_agent():
    data = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
    ]
    agent = random.choice(data)
    return agent

def extract_valid_json(response,get_json=True):
    if get_json:
        start_index = response.index("{")
        end_index = response.rindex("}") + 1
        json_str = response[start_index:end_index]
        res=json_repair.loads(json_str)
        assert type(res)==dict
        for key, value in res.items():
            assert type(value)==str

        return res
    else:
        return response


def check_image_link(image_links, url):
    accessible_links = []

    def check_link(link):
        try:
            if not link.startswith("http://") and not link.startswith("https://"):
                link = urljoin(url, link)
            
            response = requests.head(link, timeout=15,headers={'User-Agent':user_agent()})
            content_type = response.headers.get('content-type')

            if response.status_code == 200 and 'image' in content_type :
                accessible_links.append(link)

        except Exception as e:
            pass

    threads = []

    for link in image_links:
        thread = threading.Thread(target=check_link, args=(link,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return accessible_links


class FireFoxDriverHelper:
    def __init__(self):
        options = webdriver.FirefoxOptions()
        os_name = platform.system()

        home_dir = os.path.expanduser("~")
        if os_name == "Windows":
            options.binary_location = os.path.join(home_dir, r'AppData\Local\Mozilla Firefox\firefox.exe')
        elif os_name == "Linux":
            options.binary_location = '/usr/bin/firefox'  
        elif os_name == "Darwin":  
            options.binary_location = '/Applications/Firefox.app/Contents/MacOS/firefox'
        else:
            raise Exception(f"Unsupported OS: {os_name}")

        u_agent = user_agent()
        options.add_argument(f'--user-agent={u_agent}')
        options.add_argument("--headless")
        options.add_argument('--allow-running-insecure-content')
        options.add_argument("--disable-extensions")
        options.add_argument("--proxy-server='direct://'")
        options.add_argument("--proxy-bypass-list=*")
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('ignore-certificate-errors')
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-notifications")

        script_dir = os.path.dirname(os.path.realpath(__file__))
        if os_name == "Windows":
            gecko_driver_path = os.path.join(script_dir, 'geckodriver.exe')
        else:
            gecko_driver_path = os.path.join(script_dir, 'geckodriver')

        self.driver = webdriver.Firefox(service=FirefoxService(gecko_driver_path), options=options)