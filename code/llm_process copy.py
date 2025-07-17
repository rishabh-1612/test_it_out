import traceback
from helperClasses import AzureOpenAI, GoogleAI
from Enums.model_type import azure_openai_model_name, google_model_name
import logging
import time
from utils.utils import exception_handling


# retry count for the iteration.
retry_count = 3




def getCorrectColumnInitial(tables):
    length = len(tables)
    initialColumnProcessPrompt = f"""
    You are given {length} 2d-nested-lists and each nested list have 3 lists inside them, which represents first 3 rows of a table.
    For every table, you have to detect column names if column names are present
    Out of those 3 lists inside every nested-list, there can be a row with column names or there might also be a case where no row which represents column names.
    If no rows have column names, then answer is 0, If 1st row is a column name, the answer is 1, if 2nd row is a column name then the answer is 2 and lastly if 3rd row is a column name the answer is 3
    If all the three rows are not similar to each other, then most probably, the answer is 3.
    If all the three rows look similar to each other for most of the values, then the answer is 0.
    And finally, The output format should be list of answers, so if there are 6 tables, then in output, there should be a list of 6 elements where each element is an answer which represent the correct row with column names(Meaning element in this output list should be a correct row number which has a column name or 0). 
    Remember the output should only contain this list and nothing else.
    Also make sure that the number of elements in the output should be exactly equal to {length}.

    Example:
    Input1:
    table_1: [['samsung', 'phone', 'snapdragon'],['apple', 'laptop', 'M1 pro'],['dell', 'laptop', 'intel i5']]
    table_2: [[None,'Value','28','Decrease','20'],['Roll.no','Address','City','Date'],['Ramesh','east mumbai 2020','mumbai','20 may 2020']]
    table_3: [['Name','Age','City'],['Aditya','28','washim'],['John',None,'chicago']]
    table_4: [[None,'None','None','None','20'],['#','','90',None],['Roll.no','Address','City','Date']]
    table_5: [['English'],['Sanskrit'],['Hindi']]
    Output:
    [0, 2, 1, 3, 0]

    Input:
    {' '.join([f'table{i+1}: {value}' for i, value in enumerate(tables)])}
    Output:
    """

    primary_model = azure_openai_model_name.RESPONSE_4_32K.value
    fallback_model = google_model_name.RESPONSE_CLAUDE_SONNET_35.value

    for attempt in range(retry_count):
        try:
            response, _, _ = AzureOpenAI.AzureOpenAI.generate_response(initialColumnProcessPrompt, "", [], model=primary_model)
            return response
        except Exception as e:
            if hasattr(e, 'code') and e.code == 429:
                logging.warning(f"Attempt {attempt + 1} for getCorrectColumnInitial: Rate limit reached.")
                try:
                    sleep_time = int((e.message).split('after ')[1].split()[0])
                except Exception as e:
                    exception_handling("Error while fetching the sleep time from response from getCorrectColumnInitial function", {}, e, traceback.format_exc(), error_code=0)
                    sleep_time = 60  # Default sleep time

                logging.warning(f"Token limit exceeded, sleeping for {sleep_time + 1} seconds")
                time.sleep(sleep_time + 1)
                
            else:
                logging.warning(f"Exception in getCorrectColumnInitial: {e}, attempting fallback.")
                try:
                    response, _, _ = GoogleAI.GoogleAI.generate_response(initialColumnProcessPrompt, "", [], {}, model=fallback_model)
                    return response
                except Exception as e:
                    if hasattr(e, 'code') and e.code == 429:
                        logging.warning(f"Attempt {attempt + 1} for getCorrectColumnInitial: Rate limit reached.")
                        try:
                            sleep_time = int((e.message).split('after ')[1].split()[0])
                        except Exception as e:
                            exception_handling("Error while fetching the sleep time from response from else block function", {}, e, traceback.format_exc(), error_code=0)
                            sleep_time = 60  # Default sleep time

                        logging.warning(f"Token limit exceeded, sleeping for {sleep_time + 1} seconds")
                        time.sleep(sleep_time + 1)
    logging.error("Max attempts reached in getCorrectColumnInitial.")
    return []


def isSameColumnCombined(nestedRows):
    length = len(nestedRows)
    isSameColumnNestedPrompt = f"""
    You are given {length} number of 2d nested lists and each nested list have 2 lists inside them, Among these 2 lists, the 1st list always represents column names of a table and 2nd list may be a row in that particular table or column names for another table.
    Your task is to classify whether that 2nd list in every nested list is a row under the 1st list or whether it represent the column names of a completely different/another similar table.
    If the 2nd list comes under the 1st list, then the answer is True, if the 2nd list do not come under the 1st table and it looks like column names for a different table then the answer is False.
    Now finally, The output is the list of answers, so if there are 6 nested lists then the output will be list of 6 True/False statements.
    If lengths of values in a list is very long, then most probably that list represnets same row under the 1st list
    Remember the output should only contain this list and nothig else.
    There are also few cases where the 2 lists seem diffrent but belong to the same table.
    Also remember that the number of element in the output should be exactly equal to {length}.

    Example:
    Input1:
    Nested List 1: [['Name','Age','City'], ['John','28','New Jersey']]
    Nested List 2: [['Roll.no','Address','City','Date'], ['Ramesh','east mumbai 2020','mumbai','20 may 2020]]
    Nested List 3: [['Name','Age','Joined date'], ['Project Title','Time taken','Manager name']]
    Nested List 4: [['Team','Feature','Projected to Complete in Q4','Priority','Team'], ['User Incremental (WING)','None','Yes','None', 'None']]
    Nested List 5: [['Name','DOB','EmpID'], ['Process', 'None', 'None']]
    Output:
    [True, True, False, True, False]


    Input:
    {' '.join([f'Nested List {i+1}: {value}' for i, value in enumerate(nestedRows)])}
    Output:
    """
    
    primary_model = azure_openai_model_name.RESPONSE_4_32K.value
    fallback_model = google_model_name.RESPONSE_CLAUDE_SONNET_35.value

    for attempt in range(retry_count):
        try:
            response, _, _ = AzureOpenAI.AzureOpenAI.generate_response(isSameColumnNestedPrompt, "", [], model=primary_model)
            return response
        except Exception as e:
            if hasattr(e, 'code') and e.code == 429:
                logging.warning(f"Attempt {attempt + 1} for isSameColumnCombined: Rate limit reached.")
                try:
                    sleep_time = int((e.message).split('after ')[1].split()[0])
                except Exception as e:
                    exception_handling("Error while fetching the sleep time from response from isSameColumnCombined function", {}, e, traceback.format_exc(), error_code=0)
                    sleep_time = 60  # Default sleep time

                logging.warning(f"Token limit exceeded, sleeping for {sleep_time + 1} seconds")
                time.sleep(sleep_time + 1)
            else:
                logging.warning(f"Exception in isSameColumnCombined: {e}, attempting fallback.")
                try:
                    response, _, _ = GoogleAI.GoogleAI.generate_response(isSameColumnNestedPrompt, "", [], {}, model=fallback_model)
                    return response
                except Exception as fallback_e:
                    if hasattr(fallback_e, 'code') and fallback_e.code == 429:
                        logging.warning(f"Attempt {attempt + 1} for isSameColumnCombined: Rate limit reached.")
                        try:
                            sleep_time = int((e.message).split('after ')[1].split()[0])
                        except Exception as e:
                            exception_handling("Error while fetching the sleep time from response from isSameColumnCombined function", {}, e, traceback.format_exc(), error_code=0)
                            sleep_time = 60  # Default sleep time

                        logging.warning(f"Token limit exceeded, sleeping for {sleep_time + 1} seconds")
                        time.sleep(sleep_time + 1)
                
    logging.error("Max attempts reached in isSameColumnCombined.")
    return []
