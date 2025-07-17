"""Extraction code based on the current SOW format that we have"""

from docx import Document
import re

def process_sow_data(file_path):
    """
    Extracts the required fields from the input SOW in a json format.
    """
    doc_data = Document(file_path)
    sections_to_be_excluded = ["Pricing","Roles and Responsibilities"]
    headings_to_be_excluded = [
        "Estimated Cloud Consumption",
        "High Level Architecture Diagram",
        "Project Timeline",
    ]
    normal_headings_to_be_excluded =[
        "Terms and Schedule",  
        "Change Order Procedure",
        "Key Personnel", 
        "Statement of Work Acceptance", 
        "Exhibit B", 
        "Project and Deliverables", 
        'Architecture Diagram' 
    ]
    sections = []
    current_section_key = "CoverPage" # heading 1 property elements present in the doc
    current_subsection_key = "" # heading 2 property elements present in the doc
    current_subsection_content = "" # normal property elements present in the doc
    current_section_list = []
    template_value = "Techolution"

    # Iterating through the all the elements in the doc file
    for doc_element in doc_data.iter_inner_content():
        style_name = doc_element.style.name
        #Add the extracted data to a dictionary for that particular section heading 1
        is_underline = False
        try:
            for run in doc_element.runs:
                if run.underline:
                    is_underline = True
                    break
        except Exception:
            pass

        if style_name == "Heading 1" or is_underline:

            if "Executive Summary" in doc_element.text.strip():
                template_value = "PSF"

            heading_found = doc_element.text.strip()
            heading_found = re.sub(r'\d+\.', '', heading_found)
            heading_found=heading_found.strip()

            if current_section_key == "CoverPage" and "Introduction" not in heading_found:
                current_subsection_content += heading_found
            else:
                if current_section_key not in normal_headings_to_be_excluded:
                    current_section_list.append({"subsection":current_subsection_key,"text":current_subsection_content})

                    sections.append({"section":current_section_key,"contents":current_section_list})

                current_section_list = []
                current_section_key = heading_found
                current_subsection_key = ""
                current_subsection_content = ""

        #Add the extracted data to a dictionary for that particular subsection heading 2
        elif style_name == "Heading 2":
            current_heading_2 = doc_element.text.strip()
            if current_heading_2 != "":
                current_section_list.append({"subsection":current_subsection_key,"text":current_subsection_content})
                current_subsection_key = current_heading_2
                current_subsection_content = ""

        # For extracting text of normal type
        elif style_name == "normal" or style_name=='Normal':
            curr_text = doc_element.text.strip()
            if curr_text != "":
                current_subsection_content += "\n"+doc_element.text.strip()

        # The current data format has table data property as None. The following extracts the data for the table
        else:
            table_data = ""
            for row in doc_element.rows:
                row_data = [cell.text for cell in row.cells]
                
                if row_data:
                    if row_data[0] == "No.":
                        table_data += "\n" + row_data[1] + "\n"
                    else:
                        table_data += f"\n{row_data[0]}: {row_data[1:]}"
            current_subsection_content += table_data

    if current_section_key not in normal_headings_to_be_excluded:

        current_section_list.append({"subsection":current_subsection_key,"text":current_subsection_content})
        sections.append({"section":current_section_key,"contents":current_section_list})

    # Removes the section or subsections not required (Note : As per the current format it is suggested to make sections and subsections not required as hardcoded)
    filtered_sections = [i for i in sections if i['section'] not in sections_to_be_excluded]
    for section in range(len(filtered_sections)):
        filtered_sections[section] = {'section':filtered_sections[section]['section'],'contents':[content for content in filtered_sections[section]['contents'] if content['subsection'] not in headings_to_be_excluded]}

    # Formats the data in the format to be in ingested in pinecone
    final_data = []
    curr_section_order = 0 # current section order
    for section in filtered_sections:
        current_section_data = ""
        for subsection in section['contents']:
            if subsection['text'] != "":
                current_section_data += f"\n\n**{subsection['subsection']}**\n"
                current_section_data += subsection['text']
        
        if section['section']=='Project Overview':
            start_idx=current_section_data.find('Project Description:')
            end_idx=current_section_data.find('Project contacts:')
            current_section_data=current_section_data[start_idx:end_idx]

            final_data.append({'section':section['section'],'order':curr_section_order, 'text':current_section_data,"template": template_value})
            curr_section_order+=1

        elif section['section']=="Milestone Table":
            final_data[-1]['text']=current_section_data

        elif section['section']:
            final_data.append({'section':section['section'],'order':curr_section_order, 'text':current_section_data,"template": template_value})
            curr_section_order+=1

    return final_data