"""Code to extract text content from PPT Slides including Complex Shapes"""
import logging
import subprocess
import os
import time
import uuid
# import pymupdf

from vertexai.preview.generative_models import Image
from data_extraction.download_file import get_libreoffice_path
from helperClasses import GoogleAI
from utils import utils_process_vision_query
from Enums.model_type import google_model_name
    
def extract_text_from_shape(shape):
    """ 
    Helper function to extract text from shapes. Recursively if there are nested shapes.
    """
    text_runs = []
    if hasattr(shape, "text"):
        text_runs.append(shape.text)
    elif hasattr(shape, "text_frame"):
        for paragraph in shape.text_frame.paragraphs:
            for run in paragraph.runs:
                text_runs.append(run.text)
    if hasattr(shape, "shapes"):
        for sub_shape in shape.shapes:
            text_runs.extend(extract_text_from_shape(sub_shape))
    return text_runs

def save_slide_as_png(file_path, slideIndex):
    """
    Helper function to convert index+1th Slide in given PPT to a PNG Image.
    """
    # Convert Slide to PDF
    libreoffice_path = get_libreoffice_path()
    subprocess.run([libreoffice_path, '--headless', '--convert-to', 'pdf:draw_pdf_Export:{"PageRange":{"type":"string","value":"' + str(slideIndex+1) + '"}}', f'{file_path}'])
    
    # Convert PDF to PNG
    change_extension = lambda filename, new_ext: f"{'.'.join(filename.split('.')[:-1])}.{new_ext.lstrip('.')}"
    pdf_filename = change_extension(file_path, 'pdf')

    # Convert First PDF Page to PNG
    doc = pymupdf.open(pdf_filename)
    pic = doc[0].get_pixmap(dpi = 192)
    pic.save(f"Slide_{pdf_filename.rsplit('.',1)[0]}.png")
    doc.close()
    
    # Remove unused PDF File
    if os.path.exists(pdf_filename):
        os.remove(pdf_filename)


def generate_vision_response(file, prompt):
    download_folder = str(uuid.uuid4())
    os.makedirs(download_folder, exist_ok=True)
    
    image_contents = [prompt]
    models = [google_model_name.RESPONSE_GEMINI_1_5_FLASH.value, 
              google_model_name.RESPONSE_CLAUDE_SONNET_35.value]

    for model in models:
        for attempt in range(3):
            try:
                image = Image.load_from_file(file)
                image_contents.append(image)
                response, _, _ = GoogleAI.GoogleAI.generate_response(image_contents, "", [], {}, model=model)
                utils_process_vision_query.start_delete_thread(download_folder)
                return response
            except Exception as e:
                logging.info(f'Sleeping for 15 sec for Timeout. Current Attempt {attempt + 1}')
                time.sleep(15)
    
    return None


prompt = """
I have a project roadmap table in an image. The x-axis represents the weeks of the project (starting from Week 0), and the y-axis represents the various tasks to be accomplished. The content within the table are subtasks, each taking a specific number of weeks.

Please create a dictionary from this data with the following structure:
{
"x-axis": ["Week 0", "Week 1", "Week 2", ...],
"y-axis": ["Task 1", "Task 2", "Task 3", ...],
"actions": {
"Subtask 1": ["Task on the y-axis under which the subtask belongs to", ["Start week"]],
"Subtask 2": ["Task on the y-axis", ["Week x", "Week y"]],
...
}
}

Ensure:

Weeks start from Week 0.
Include all tasks and their respective subtasks.
Do not print anything other than the dictionary object itself.
Start the output with the opening curly braces of the dictionary and end the message with the closing curly braces.
Do not include newlines in the json text at any point. Use your inference as best you can.
Mention the specific weeks each subtask spans.
"""