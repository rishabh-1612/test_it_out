import fitz
from operator import itemgetter
import nltk
from nltk.corpus import words
import logging
from vertexai.preview.generative_models import Image, GenerationConfig
from PIL import Image as PILImage
from io import BytesIO
import tempfile
import os
import time
import vertexai.preview.generative_models as generative_models
from Enums.model_type import google_model_name
from helperClasses import GoogleAI
import re

nltk.download("words")

def fonts(doc, granularity=False):
    styles = {}
    font_counts = {}

    for page in doc:
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:  # iterate through the text blocks
            if block["type"] == 0:  # block contains text
                for line in block["lines"]:  # iterate through the text lines
                    for span in line["spans"]:  # iterate through the text spans
                        if granularity:
                            identifier = "{0}_{1}_{2}_{3}".format(
                                span["size"], span["flags"], span["font"], span["color"]
                            )
                            styles[identifier] = {
                                "size": span["size"],
                                "flags": span["flags"],
                                "font": span["font"],
                                "color": span["color"],
                            }
                        else:
                            identifier = "{0}".format(span["size"])
                            styles[identifier] = {"size": span["size"], "font": span["font"]}

                        font_counts[identifier] = (
                            font_counts.get(identifier, 0) + 1
                        )  # count the fonts usage

    font_counts = sorted(font_counts.items(), key=itemgetter(1), reverse=True)

    if len(font_counts) < 1:
        raise ValueError("Zero discriminating fonts found!")

    return font_counts, styles

def font_tags(font_counts, styles):

    p_style = styles[
        font_counts[0][0]
    ]  # get style for most used font by count (paragraph)
    p_size = p_style["size"]  # get the paragraph's size

    # sorting the font sizes high to low, so that we can append the right integer to each tag
    font_sizes = []
    for font_size, count in font_counts:
        font_sizes.append(float(font_size))
    font_sizes.sort(reverse=True)

    # aggregating the tags for each font size
    idx = 0
    size_tag = {}
    for size in font_sizes:
        idx += 1
        if size == p_size:
            idx = 0
            size_tag[size] = "<p>"
        if size > p_size:
            size_tag[size] = "<h{0}>".format(idx)
        elif size < p_size:
            size_tag[size] = "<s{0}>".format(idx)

    return size_tag
# merge sentences based on punctuation
def merge_sentences(sentences):
    merged_sentences = []
    current_sentence = ""
    
    for sentence in sentences:
        sentence = sentence.strip()

        if current_sentence:
            if re.match(r'^[a-z]', sentence) or current_sentence.endswith((',', ';', 'and', 'or', 'but', 'so', 'the', 'a')) or not current_sentence.endswith('.'):
                current_sentence += "\n" + sentence
            else:
                # Otherwise, finalize the current sentence and start a new one
                merged_sentences.append(current_sentence)
                current_sentence = sentence
        else:
            current_sentence = sentence

    # Append the last sentence if exists
    if current_sentence:
        merged_sentences.append(current_sentence.strip())

    return merged_sentences
def check_encoding(text):
    list_words = words.words()
    text = text.lower()
    text = text.split(" ")

    for word in text[:100]:
        if word in list_words:
            return False

    return True

def check_table(page):
    try:
        if len(page.find_tables().tables):
            return True
    except Exception as e:
        logging.info('Internal Error in Check Table')
        return True
    return False

def extract_json_response(input_str):
    if not input_str:
        return {}, False
    empty_dict_pattern = r'(```\s*)?(json\s*)?\{\s*\}(\s*```)?'
    if re.fullmatch(empty_dict_pattern, input_str.strip(), re.IGNORECASE):
        return {}, True
    json_block_pattern = r'```(?:json)?\s*([\s\S]+?)\s*```'
    match = re.search(json_block_pattern, input_str, re.IGNORECASE)
    if match:
        json_str = match.group(1).strip()
    else:
        json_str = input_str.strip()
    try:
        parsed_json = json.loads(json_str)
        if isinstance(parsed_json, dict):
            return parsed_json, True
    except json.JSONDecodeError:
        brace_match = re.search(r'\{[\s\S]*\}', input_str)
        if brace_match:
            fallback_json_str = brace_match.group(0).strip()
            try:
                parsed_json = json.loads(fallback_json_str)
                if isinstance(parsed_json, dict):
                    return parsed_json, True
            except json.JSONDecodeError:
                pass
    return None, False


def process_page_num(response,page):
    logging.info("Extracting page number")
    prompt =f"""
    You will recieve two inputs
        1. The extracted OCR text from the image of that page.
        2. An image of that page.

    Your sole task is to determine the page number by analyzing both the extracted text and the image. The page number may be explicitly mentioned in the text (e.g., "Page 5") or visually visible in the image (e.g., at the footer or header).

    Respond only with a JSON object in the following format:
        {{
            "page_number": "<extracted_page_number>"
        }}

    NOTE: The value of page_number must be an integer, as defined by Python's int type.
    If the page number cannot be determined from either source, return:

    {{
        "page_number": "<null>"
    }}

    Do not include any additional explanation or comments—only the JSON object. Be precise and reliable.

    ```
        Extracted OCR Text: {str(response)}
    ```
    """
    system_prompt = """
    You are a Computer Vision expert who specializes in extracting page number present in an image or in the extracted OCR text.
    """ 
    image_data = page.get_pixmap(dpi=600).pil_tobytes(format="PNG")
    pil_image = PILImage.open(BytesIO(image_data))
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
            pil_image.save(temp_file, format="PNG")
            temp_file_path = temp_file.name
    image = Image.load_from_file(temp_file_path)
    models = [google_model_name.RESPONSE_GEMINI_1_5_PRO.value, 
              google_model_name.RESPONSE_CLAUDE_SONNET_35.value]
    for model in models:
        for attempt in range(3):
            try:
                response, _, _ = GoogleAI.GoogleAI.generate_response([system_prompt + prompt, image], "", [], {}, model=model)
                resp_text,isformatted = extract_json_response(response)
                if isformatted:
                    os.remove(temp_file_path)
                    return resp_text.get('page_number',-1)
                else:
                    if model == google_model_name.RESPONSE_CLAUDE_SONNET_35.value and attempt == 2:
                        return -1
                    else:
                        continue
            except Exception as e:
                logging.info(f'Sleeping for 5 sec for Timeout. Current Attempt {attempt + 1}')
                time.sleep(5)
    os.remove(temp_file_path)
    logging.error('Output blocked by content filtering policy')
    return  -1

def send_to_llm_vision(page):
    prompt = """
    Understand the image and provide me with all the data present in it. 
    If a table is present, understand the complete table and provide meaningful complete sentences that explains every cell in every row of that table without missing any context or content. Every cell detail needs to be explained in a seperate sentence.
    And do not just provide an outline of the table or just examples from the table, but instead provide the entire content that is available in it.
    Do not miss any parts or portions of text from the image even if it is a table.
    If there are multiple paragraphs present in the image, separate them by two newline characters ('\\n\\n').
    """

    system_prompt = """
    You are a Computer Vision expert who specializes in extracting all the data present in an image including paragraphs and tables.
    """ 

    image_data = page.get_pixmap(dpi=600).pil_tobytes(format="PNG")
    pil_image = PILImage.open(BytesIO(image_data))

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
            pil_image.save(temp_file, format="PNG")
            temp_file_path = temp_file.name
    
    # Load the image from the temporary file
    image = Image.load_from_file(temp_file_path)

    # Create the generative multimodal model instance
    config = GenerationConfig(temperature=0)
    safety_settings = {
        generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }
    
    models = [google_model_name.RESPONSE_GEMINI_1_5_FLASH.value, 
              google_model_name.RESPONSE_CLAUDE_SONNET_35.value]
    for model in models:
        for attempt in range(3):
            try:
                response, _, _ = GoogleAI.GoogleAI.generate_response([system_prompt + prompt, image], "", [], {}, model=model)
                resp_text = response
                os.remove(temp_file_path)
                return True, resp_text
            except Exception as e:
                logging.info(f'Sleeping for 15 sec for Timeout. Current Attempt {attempt + 1}')
                time.sleep(15)

    os.remove(temp_file_path)
    logging.error('Output blocked by content filtering policy')
    return True,  "\n"

def send_to_llm_vision_book(page):
    prompt = """
    Understand the image and extract all data from it. Follow the instructions below strictly:
        Purpose:
            The extracted data will be used to render a book chunk in the UI, so the output must exactly match the original image in content, structure, and formatting—no corrections, paraphrasing, or summarization allowed.
        Content Handling:
        If the image contains a table, follow these rules:
            Understand the entire table and provide complete, meaningful sentences that explain every cell in every row.
            You must never skip, summarize, or modify any content. Every cell must be included.
            Do not just outline or provide examples, the entire table must be fully explained.
            Include them in "content" key.
        If there are multiple paragraphs present in the image, follow these rules:
            Separate them by two newline characters ('\\n\\n').
            Include them in "content" key.
        Do not alter any part of the content from the image.
        Maintain all escape sequences such as '\n' and '\t' exactly as they appear in the image, but whenever there in a new paragraph then separate them by two newline characters ('\\n\\n').
        Clearly separate and extract the header, footer, and content into their respective sections. Content section should contain the paragraphs separated by two new line characters ('\\n\\n').
        The content field must never contain the page number. Page number, if present, may appear in the header or footer, but never in the content.
        Response Structure:
        You should strictly return a JSON of the format:
            {
                "footer": <footer of the page>,
                "header": <header of the page>,
                "content": <main content paragraph wise, where in paragraphs are separated by ('\\n\\n')>
            }
        Critical Notes:
            No extra commentary or explanations should be included—only the required structured data.
            Ensure the extracted data maintains exact text alignment and representation for accurate rendering in the UI.
            If there are multiple paragraphs present in the image, separate them by two newline characters ('\\n\\n').
            Strictly follow the JSON response structure.
    """

    system_prompt = """
    You are a Computer Vision expert who specializes in extracting all the data present in an image including paragraphs and tables.
    """ 

    image_data = page.get_pixmap(dpi=600).pil_tobytes(format="PNG")
    pil_image = PILImage.open(BytesIO(image_data))

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
            pil_image.save(temp_file, format="PNG")
            temp_file_path = temp_file.name
    
    # Load the image from the temporary file
    image = Image.load_from_file(temp_file_path)

    # Create the generative multimodal model instance
    config = GenerationConfig(temperature=0)
    safety_settings = {
        generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }
    
    models = [google_model_name.RESPONSE_GEMINI_1_5_PRO.value,google_model_name.RESPONSE_CLAUDE_SONNET_35.value]
    for model in models:
        for attempt in range(3):
            try:
                response, _, _ = GoogleAI.GoogleAI.generate_response([system_prompt + prompt, image], "", [], {}, model=model)
                resp_text = response
                os.remove(temp_file_path)
                return True, resp_text
            except Exception as e:
                logging.info(f'Sleeping for 15 sec for Timeout. Current Attempt {attempt + 1}')
                time.sleep(15)

    os.remove(temp_file_path)
    logging.error('Output blocked by content filtering policy')
    return True,  "\n"

def llm_helper(content):
    parsed_content,is_parsed = extract_json_response(content)
    logging.info(f"Parsed Content is: {parsed_content}")
    if not is_parsed:
        return {}
    return parsed_content
def post_process_response(text_ebook):
    page_source = dict()
    compare_key = -1
    compare_value = -1
    for k, v in text_ebook.items():
        page = v.get('page_number')
        if page not in (None, -1):
            diff = int(k) - int(page)
            page_source[diff] = page_source.get(diff, 0) + 1
            if page_source[diff] > compare_value:
                compare_value = page_source[diff]
                compare_key = diff
    compare_key = 0 if compare_key < 0 else compare_key
    for k, v in text_ebook.items():
        v['page_number'] = int(k) - compare_key
    return text_ebook

def headers_para(doc, type_of_response=None):
    text_ebook = {}  
    normal_text = "" 
    first = True  
    previous_s = {}  
    prev_bbox = [0, 0, 0, 0]
    for page_num, page in enumerate(doc, start=1): 
        text = page.get_text()
        claude_response = False
        
        is_encoded = check_encoding(text)
        if is_encoded or check_table(page):
            status, content = send_to_llm_vision(page)
            if status and content.strip():
                claude_response = True
                if type_of_response == 'paragraph':
                    content = content.split("\n\n")
                    if page_num not in text_ebook:
                        text_ebook[page_num] = content
                    else:
                        text_ebook[page_num].extend(content) 
                else:
                    normal_text += content.strip() + "\n"  
            continue  

        if not claude_response:
            try:
                blocks = page.get_text("dict")["blocks"]
                page_paragraphs = []  
                for block in blocks:
                    block_bbox = block["bbox"]
                    if first:
                        prev_bbox = block_bbox
                    elif block_bbox[0] - prev_bbox[0] > 180 and abs(block_bbox[2] - prev_bbox[2]) < 20:
                        logging.info(f"Citation-footer is skipped on page number: {page_num}")
                        continue
                    prev_bbox = block_bbox
                    if block["type"] == 0: 
                        block_text = ""  
                        for line in block["lines"]:
                            line_bbox = line["bbox"]
                            # if first:
                            #     prev_bbox = line_bbox
                            # elif line_bbox[0] - prev_bbox[0] > 180 and abs(line_bbox[2] - prev_bbox[2]) < 20:
                            #     print(line_bbox[0] - prev_bbox[0])
                            #     print("Continued")
                            #     continue
                            # prev_bbox = line_bbox
                            line_text = "" 
                            for span in line["spans"]:
                                span_bbox = span["bbox"]
                                # if first:
                                #     prev_bbox = span_bbox
                                # elif span_bbox[0] - prev_bbox[0] > 150:
                                #     print("Continued")
                                #     continue
                                # prev_bbox = span_bbox
                                if first:
                                    previous_s = span
                                    first = False
                                
                                if span["size"] == previous_s["size"]:
                                    line_text += (" " + span["text"]).strip()
                                else:
                                    if line_text.strip():
                                        block_text += line_text + "\n"
                                    line_text = span["text"]
                                previous_s = span
                            
                            if line_text.strip():
                                block_text += line_text + "\n"
                        
                        if block_text.strip():
                            page_paragraphs.append(block_text.strip()) 

                if page_paragraphs:
                    if type_of_response == 'paragraph':
                        text_ebook[page_num] = page_paragraphs  
                    else:
                        normal_text += "\n".join(page_paragraphs) + "\n" 

            except Exception as e:
                logging.warning('Internal Error in Header Para')
                status, content = send_to_llm_vision(page)
                if status and content.strip():
                    if type_of_response == 'paragraph':
                        if page_num not in text_ebook:
                            text_ebook[page_num] = [content]
                        else:
                            text_ebook[page_num].append(content)  
                    else:
                        normal_text += content.strip() + "\n"  
        if type_of_response == 'paragraph':
            text_ebook[page_num] = merge_sentences(text_ebook[page_num])
    if type_of_response == 'paragraph':
        return text_ebook  
    else:
        return normal_text 

def book_para(doc, type_of_response=None):
    text_ebook = {}  
    normal_text = "" 
    first = True  
    previous_s = {}  
    prev_bbox = [0, 0, 0, 0]
    for page_num, page in enumerate(doc, start=1):
        text = page.get_text()
        claude_response = False
        try:
            status, content_dict = send_to_llm_vision_book(page)
            content_dict = llm_helper(content_dict)
            if isinstance(content_dict,list):
                content_dict = content_dict[0]
            if not isinstance(content_dict,dict):
                content_dict = {}
            header = content_dict.get("header",None)
            footer = content_dict.get("footer",None)
            content = content_dict.get("content","")
            if status and content.strip():
                claude_response = True
                if type_of_response == 'book':
                    content = content.split("\n\n")
                    if page_num not in text_ebook:
                        text_ebook[page_num] = content
                    else:
                        text_ebook[page_num].extend(content)
                    chunks = text_ebook[page_num]
                    page_number = process_page_num(response=chunks,page=page)
                    try:
                        if isinstance(page_number,str):
                            page_number=int(page_number)
                    except Exception as e:
                        page_number=None
                    text_ebook[page_num] = {
                        "chunks" : chunks,
                        "page_number": page_number,
                        "header":header,
                        "footer":footer,
                        "llm" : True

                    }
                else:
                    normal_text += content.strip() + "\n"
                continue
        except Exception as e:
            claude_response = False
        try:
            if not claude_response:
                try:
                    content = []
                    header = None
                    footer = None
                    llm_used = False
                    blocks = page.get_text("dict")["blocks"]
                    logging.info(f"BLOCKS: {blocks}")
                    page_paragraphs = []  
                    for block in blocks:
                        block_bbox = block["bbox"]
                        if first:
                            prev_bbox = block_bbox
                        elif block_bbox[0] - prev_bbox[0] > 180 and abs(block_bbox[2] - prev_bbox[2]) < 20:
                            logging.info(f"Citation-footer is skipped on page number: {page_num}")
                            continue
                        prev_bbox = block_bbox
                        if block["type"] == 0: 
                            block_text = ""  
                            for line in block["lines"]:
                                line_bbox = line["bbox"]
                                line_text = "" 
                                for span in line["spans"]:
                                    span_bbox = span["bbox"]
                                    if first:
                                        previous_s = span
                                        first = False
                                    
                                    if span["size"] == previous_s["size"]:
                                        line_text += (" " + span["text"]).strip()
                                    else:
                                        if line_text.strip():
                                            block_text += line_text + "\n"
                                        line_text = span["text"]
                                    previous_s = span
                                
                                if line_text.strip():
                                    block_text += line_text + "\n"
                            
                            if block_text.strip():
                                page_paragraphs.append(block_text.strip()) 
                    if type_of_response == 'book':
                        text_ebook[page_num] = page_paragraphs  
                    else:
                        normal_text += "\n".join(page_paragraphs) + "\n" 
                except Exception as e:
                    logging.info('Internal Error in Header Para')
                    status, content_dict = send_to_llm_vision_book(page)
                    content_dict = llm_helper(content_dict)
                    if isinstance(content_dict,list):
                        content_dict = content_dict[0]
                    if not isinstance(content_dict,dict):
                        content_dict = {}
                    header = content_dict.get("header",None)
                    footer = content_dict.get("footer",None)
                    content = content_dict.get("content","")
                    llm_used = True
                    if status and content.strip():
                        if type_of_response == 'book':
                            if page_num not in text_ebook:
                                text_ebook[page_num] = [content]
                            else:
                                text_ebook[page_num].append(content)  
                        else:
                            normal_text += content.strip() + "\n"  
            if type_of_response == 'book':
                chunks = merge_sentences(text_ebook[page_num])
                page_number = process_page_num(response=chunks,page=page)
                try:
                    if isinstance(page_number,str):
                        page_number=int(page_number)
                except Exception as e:
                    page_number=None
                text_ebook[page_num] = {
                    "chunks" : chunks,
                    "page_number": page_number,
                    "header": header,
                    "footer": footer,
                    "llm": llm_used
                }
        except Exception as e:
            text_ebook[page_num] = {
                "chunks" : [],
                "page_number": None,
                "header": None,
                "footer": None,
                "llm": False
            }

    if type_of_response == 'book':
        text_ebook = post_process_response(text_ebook)
        return text_ebook  
    else:
        return normal_text 
    
def pdf_processing(file_path, type_of_response=None):
    doc = fitz.open(file_path)
    if type_of_response == 'book':
        text_ebook = book_para(doc, type_of_response)
    else:
        text_ebook = headers_para(doc, type_of_response)
    doc.close()
    return text_ebook