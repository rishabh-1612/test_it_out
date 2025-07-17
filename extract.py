from copy import deepcopy
import pandas as pd
from data_extraction import  sendParallelLLMCalls
from datetime import datetime
import logging

        
def is_row_bold(row):
    bold = 0
    for cell in row:
        if(cell.value is None):
            continue
        elif cell.font.bold:
            bold = 1
        else:
            bold = 0
            break
    if(bold == 1):
        return True
    return False


def length_of_row(row):
    len = 0
    for cell in row:
        if(cell.value is not None):
            len = len+1
    return len

def length_of_list_without_none_values(row_list):
    len = 0
    for val in row_list:
        if (val is not None):
            len += 1
    return len

def get_merged_cells(sheet):
    merged_cells_ranges = sheet.merged_cells.ranges
    merged_cell_list = []
    for merged_Cell in merged_cells_ranges:
      merged_cell_list.append((str(merged_Cell)).split(':'))
    simple_merged_coordinates = list()
    for k in range(len(merged_cell_list)):
        cell_tup = (sheet[merged_cell_list[k][0]:merged_cell_list[k][1]])
        simple_list = []
        # row wise merging
        if(len(cell_tup) > 1):
            for i in range(len(cell_tup)):
                simple_list.append(cell_tup[i][0].coordinate)
        # column wise merging
        elif(len(cell_tup) == 1):
            for i in range(len(cell_tup[0])):
                simple_list.append(cell_tup[0][i].coordinate) 
        simple_merged_coordinates.append(simple_list)
    return simple_merged_coordinates

# removes redundant data outside the tables
def drop_from_empty_column(df, cutoff=3):
    empty_columns = 0

    for col in df.columns:
        if df[col].isnull().all():
            empty_columns += 1
            if empty_columns == cutoff:
                return df.iloc[:, :df.columns.get_loc(col)]
        else:
            empty_columns = 0

    return df

# splits tables if there are more than {cutoffVal} consecutive empty columns
def split_dfs_acc_col(df, cutoffVal=2):
    df = drop_from_empty_column(df)
    dfs=[]
    current_col = pd.DataFrame()
    emptyColCount = 0
    for col in df.columns:
        
        if df[col].notnull().any():
            current_col = pd.concat([current_col, df[col]], axis=1)
            emptyColCount = 0
        else:
            emptyColCount += 1

        if emptyColCount >= cutoffVal:
            if not current_col.empty:
                dfs.append(current_col)
                current_col = pd.DataFrame()

    if not current_col.empty:
        dfs.append(current_col)

    return dfs

# returns the coordinate of the side heading
def get_side_heading_coordinates(sheet):
    rows = sheet.iter_rows()
    val_coor = list()
    toBeRetrurned = list()
    isSideHEading = False

    for row in rows:
        if is_row_bold(row) and length_of_row(row) == 1 and row[0].value is not None:
            side_heading = row[0].value
            val_coor.append([side_heading, row[0].coordinate])
            for i in range(3):
                if sheet[(row[0].coordinate[0])+(str((int(row[0].coordinate[1:]))+i+1))].value is not None:
                    isSideHEading = False
                    break
                else:
                    isSideHEading = True
                    
            if(isSideHEading):
                toBeRetrurned.append(row[0].coordinate)  
    return toBeRetrurned

# Returns range of the coordinates, for which the side headings has to be replicated
def get_side_heading_ranges(sheet):
    sheet_data = list()
    rows = sheet.iter_rows()
    to_be_returned = list()
    for row in rows:
        row_data = list()
        for cell in row:
            row_data.append(cell.value)
        sheet_data.append(row_data)

    side_heading_coordinates = get_side_heading_coordinates(sheet)
    end_coordinate_List = list()

    for i in side_heading_coordinates:
        row_num = int(i[1:])
        for j in range(row_num+1, len(sheet_data)):
            # Side heading will be replicated in the next row until an empty row is detected
            if(all(value is None for value in sheet_data[j])):
                end_coordinate_List.append('A'+str(j))
                break
            # Side heading will be replicated in the next row until a A[row_num] has a value
            elif sheet[i[0]+str(j)].value is not None:
                end_coordinate_List.append('A'+str(j-1))
                break
    for i in range(len(side_heading_coordinates)):
        entire_coord_list = list()
        for j in range(int(side_heading_coordinates[i][1:]), int(end_coordinate_List[i][1:])+1):
            entire_coord_list.append('A'+str(j))
        to_be_returned.append(entire_coord_list)
    return to_be_returned

emoji_desc = {
    '⛽️': "Fuel Site Launch Automation",
    '🌮': "Restaurant Site Launch Automation",
    '💰': "Grocery Site Launch Automation",
    '👀': "Migrate Site creation & configuration steps to Retool",
    '👆🏻': "Automate Dispute Process",
    '🙅🏼‍♂️': "Ability to cancel cash outs in Retool",
    '💳': "PwGC Site Ingest",
    '💸': "Pay Billing Tasks",
    '🫡': "Pay Command Handler",
    '🗞️': "Pay Publisher Updates",
    '💻': "Pay Retool UI/denormalizer",
    # special case
    'x': "Worked on",
}

def replace_emoji_with_description(cell):
    if(cell.value in emoji_desc):
        cell.value = emoji_desc[cell.value]

def specialChars(cell):

    if((isinstance(cell.value,int) or isinstance(cell.value,float)) and (cell.number_format)[1] == '$'):
        dollar_value = '$' + str(int(cell.value))
        cell.value = dollar_value
    if(cell.number_format.endswith('%')):
        if(cell.value is not None and  not isinstance(cell.value, str)):
            extracted_cell = (cell.value)*100
            cell.value = str(round(float(extracted_cell), 2))+'%'

def dateFormatting(cell):
    if isinstance(cell.value, datetime):
        formatted_date = (cell.value).strftime("%Y-%m-%d")
        cell.value = formatted_date

# Extracts hyperlinks and strike through's in a cell
def hyperlinkExtracter(cell, row_data):
    if(cell.hyperlink and cell.value is not None):
        if(cell.font.strike):
            row_data.append((cell.value,cell.hyperlink.target, 'striked=True'))
        else:
            row_data.append((cell.value,cell.hyperlink.target))
    else:
        if(cell.font.strike and cell.value is not None):
            row_data.append((cell.value, 'striked=True'))
        else:
            row_data.append(cell.value)

def extract_dataframes(wb,sheets,file_name,subsheetNames):
    llmOutputs = sendParallelLLMCalls.getLLMOutputs(wb,sheets)
    data_from_sheets = {}
    dataframes_in_dict = dict()
    for i in sheets:
        blank_count = 0
        rows = wb[i].iter_rows()
        sheet_data = []
        curr_table = []
        # Get side headings coordinate range
        side_heading_coordinates = (get_side_heading_ranges(wb[i]))
        # Get merged cells coordinate range
        merged_cells = get_merged_cells(wb[i])

        for row in rows:
            row_data = []
            for cell in row:
                # Check for an emoji and replace it with its respective description
                replace_emoji_with_description(cell)
                # Function to check dollar and percentage sign
                specialChars(cell)
                # If a cell is in merged(both column wise and row wise), then the value of the merged cell will be considered as this current cell value
                if(cell.value is None):
                    for m, mm in enumerate(merged_cells):
                        if cell.coordinate in mm:
                            cell = wb[i][merged_cells[m][0]]
                # If a sheet has a side heading, that will be replicated to all rows with that side heading
                if(cell.value is None):
                    for m, mm in enumerate(side_heading_coordinates):
                        if cell.coordinate in mm:
                            cell = wb[i][side_heading_coordinates[m][0]]
                dateFormatting(cell)
                # Function to detect hyperlinks and strike through texts
                hyperlinkExtracter(cell, row_data)

            # Initially, all the rows along with empty rows will be appended to the sheet data
            blank_line = all(value is None for value in row_data)
            if(not blank_line):
                sheet_data.append(row_data)
                blank_count = 0
            elif(blank_line and blank_count < 5):
                sheet_data.append(row_data)
                blank_count += 1

            elif(blank_count == 5):
             
                break

        data_from_sheets[i] = sheet_data
        # All that sheet data will be converted to a pd df
        df = (pd.DataFrame(data_from_sheets[i]))

        # Extra data will be removed and the tables will be splitted according to columns
        data_dfs = split_dfs_acc_col(df)


        # Extra rows which occur before the column namaes will be skipped
        current_Sheet = list()
        colTableCount = 0
        for col_table in data_dfs:
            noneTableCount = 0
            initial = 0
            first_row = list()
            blank_count_llm = 0
            curr_table = list()
            indecesToSkip = list()
            startIndex = 0
            # for index, rows in col_table.iterrows():
            for index, rows in col_table.iterrows():
                if(initial == 0):
                    blank_line = all(value is None for value in rows)
                    if(blank_line):
                        continue
                    else:
                        startIndex = index
                        initial = 1
                # If sheet has only 1 row
                if(col_table.shape[0] == 1):
                    curr_table.append(col_table.loc[index].to_list())
                    break
                if index in indecesToSkip:
                    continue

                current_row = list()
              
                if(index == startIndex):

                    correctColumn = llmOutputs[i][colTableCount][noneTableCount][0]
                    noneTableCount += 1

                    #IF nothing is a column name, append ['None']*len(row) values along with first row into the current table
                    if(int(correctColumn) == 0):
                        first_row = col_table.loc[index+int(correctColumn)].to_list()
                        curr_table.append([None]*(len(first_row)))
                    # If 1st row is column
                    elif(int(correctColumn) == 1):
                        if(len(col_table) < int(correctColumn)):
                            first_row = [None]
                        else:
                            first_row = col_table.loc[index+int(correctColumn)-1].to_list()
                    # If 2nd row is a column then 1st row will not be extracted
                    elif(int(correctColumn) == 2):
                        indecesToSkip.append(index+1)
                        if(len(col_table) < int(correctColumn)):
                            first_row = [None]
                        else:
                            first_row = col_table.loc[index+int(correctColumn)-1].to_list()
                    # If 3rd row is a column, then 1 and 2 rows will not be extracted
                    else:
                        indecesToSkip.append(index+1)
                        indecesToSkip.append(index+2)
                        if(len(col_table) < int(correctColumn)):
                            first_row = [None]
                        else:
                            first_row = col_table.loc[index+int(correctColumn)-1].to_list()

                    curr_table.append(first_row)
                    continue

                blank_line = all(value is None for value in rows)
                if(blank_line):
                    blank_count_llm += 1

                # If current row is not a blank line and previous row is a blank line
                elif(blank_count_llm != 0):

                    # In this case, this row will be compared to the column names in previous table,
                    # If this current row belongs to that column, then this row will be appended to prev table
                    # Else, a new table gets created
                    current_row = col_table.loc[index].to_list()
                    if(index+1 < len(col_table)):
                        next_row = col_table.loc[index+1].to_list()
                        if(length_of_list_without_none_values(current_row) < (length_of_list_without_none_values(next_row)/2)):
                            current_row = next_row
                            indecesToSkip.append(index+1)


                    if(len(llmOutputs[i][colTableCount]) > noneTableCount):
                        isSameCol = llmOutputs[i][colTableCount][noneTableCount][0][0]
                    else:
                        isSameCol = "True"


                    # Condition if the current row does not belong to prev table
                    if(isSameCol == 'False'):
                        current_Sheet.append(curr_table)
                        curr_table = []

                        correctColumn = llmOutputs[i][colTableCount][noneTableCount][0][1]
                        
                        noneTableCount += 1

                        # If nothing is a column name, append ['None']*len(row) values along with first row into the current table
                        if(int(correctColumn) == 0):
                            correct_row = col_table.loc[index+int(correctColumn)].to_list()
                            curr_table.append([None]*(len(correct_row)))
                        elif(int(correctColumn) == 1):
                            correct_row = col_table.loc[index+int(correctColumn)-1].to_list()
                        elif(int(correctColumn) == 2):
                            indecesToSkip.append(index+1)
                            correct_row = col_table.loc[index+int(correctColumn)-1].to_list()
                        else:
                            indecesToSkip.append(index+1)
                            indecesToSkip.append(index+2)
                            correct_row = col_table.loc[index+int(correctColumn)-1].to_list()

                        curr_table.append(correct_row)
                        first_row = current_row
                    # Condition if current row belong to prev table
                    else:
                        noneTableCount += 1
                        curr_table.append(current_row)

                    blank_count_llm = 0
                else:
                    current_row = list()
                    current_row = col_table.loc[index].to_list()
                    curr_table.append(current_row)

            # Current sheet will be a nested list, [sheet[table[row]]]
            current_Sheet.append(curr_table)
            colTableCount += 1

        dataframes_in_dict[i] = current_Sheet

    # Same code from prev logic
    # Converting all the extracted data into a proper json format
    new_dataframes = dict.fromkeys(dataframes_in_dict.keys(), [])
    for sheet_name in dataframes_in_dict.keys():
        new_sheets = []
        for table in dataframes_in_dict[sheet_name]:
            
            table_df = (pd.DataFrame(table, columns = table[0]))
            new_sheets.append(table_df)
        new_dataframes[sheet_name] = deepcopy(new_sheets)
        new_sheets.clear()

    if subsheetNames:
        excel_keys = list(new_dataframes.keys())
        spreadsheetKeys = subsheetNames
        new_dataframes = {spreadsheetKeys[subName]: new_dataframes[excel_keys[subName]] for subName in range(len(excel_keys))}

    new_data = {}
    z = 0
    for i in new_dataframes.keys():
        k=0
        subsheet_data = {}
        for j in new_dataframes[i]:
            df = j.drop(columns=[col for col in j.columns if col is None])
            
            df.columns = [f'{col}_{i}' if df.columns.tolist().count(col) > 1 or col is None else col for i, col in enumerate(df.columns)]
            df = df.iloc[1:,:]
            try:
                df.columns = df.columns.astype(str)
            except Exception:
                df.columns = [str(col) for col in df.columns]
                logging.info(df.columns)

            df = df.astype(str)
            if(df.shape[1] > 0 and df.shape[0] == 0):
               
                k= k-1
            else:
                subsheet_data[k] = df.to_dict(orient='records')
                for record_num in range(len(subsheet_data[k])):
                    for key, value in list(subsheet_data[k][record_num].items()):
                        if value == "None":
                            del subsheet_data[k][record_num][key]

            k+=1
        new_data[z] = {'subsheet_name':i,'subsheet_data':subsheet_data}
        z+=1

    final_data = {"spreadsheet_name":file_name,
                  "spreadsheet_data":new_data}

    return final_data