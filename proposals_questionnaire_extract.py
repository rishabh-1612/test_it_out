from docx import Document
import re

def all_elements_same(lst):
    "Checks if all the elements in a given list are same"
    if not lst:
        return True

    first_element = lst[0]
    return all(element == first_element for element in lst)

def all_words_first_letter_uppercase(word_list):
    "Checks if all the words have first letter as capital in a given list"
    try:
        for word in word_list:
            if not word[0].isupper():
                return False
        return True
    except Exception as _:
        return False
    
def is_blank_string(input_string):
    return re.fullmatch(r'\s*', input_string) is not None

def process_questionnaire_data(file_path):
    "Process the data in the given questionnaire in a specified format"
    doc_data = Document(file_path)
    document_text = ""
    for doc_element in doc_data.iter_inner_content():
        style_name = doc_element.style.name
        if style_name == "Heading 1":
            current_heading = doc_element.text.lstrip()
            if not is_blank_string(current_heading):
                document_text += "\n#################   " +current_heading + "   ##################\n"
        elif style_name == "Heading 2":
            current_sub_heading = doc_element.text.lstrip()
            if not is_blank_string(current_sub_heading):
                document_text += ("\n\n################   "+ current_sub_heading + "   #################\n")
        elif style_name == "Normal (Web)" or style_name == "Normal" or style_name == "normal":
            word_lst = doc_element.text.lstrip().split(' ')
            current_text = doc_element.text.lstrip()
            if not is_blank_string(current_text):
                if (len(word_lst) == (2 or 3)) and all_words_first_letter_uppercase(word_lst):
                    document_text += ("\n\n"+current_text+"\n")
                else:
                    document_text += ("\n"+current_text)
        elif style_name == "Normal Table":
            document_text += "\n"
            table_data = ""
            question_key = ""
            answer_key = ""
            for row in doc_element.rows:
                row_data = [cell.text for cell in row.cells]
                if all_elements_same(row_data):
                    table_data += ("\n\n"+row_data[0])
                elif row_data[0] == "#":
                    question_key = row_data[1]
                    answer_key = row_data[2]
                else:
                    table_data += f"\n{question_key} : {row_data[1]}\n{answer_key} : {row_data[2]}\n"
            document_text += table_data
    
    return document_text
