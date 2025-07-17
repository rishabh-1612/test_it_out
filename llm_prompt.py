column_checking_prompt = """
You are given 2 rows of a dataframe in the form of lists.
Among these rows, 1st row is always the column name
Analyse the data in the rows given to you and based on your intellect and decide whether the 2nd row is a row under that column or is entirely new column
This data is mostly about the project tracking with info on team name, objective, stakeholders, etc (all other similar features) so column names are mostly like this.

Finally return True if 2nd row belongs to that column and return False if 2nd row is entirely new columns

The answer should only True or False and nothing else

Input 1:
1 : ['Name','Age','City']
2 : ['Aditya','28','washim']
Output : True

Input 2: 
1 : ['Roll.no','Address','City','Date']
2 : [None,'Value','28','Decrease','20']
Output : True

Input 3:
1 : ['Team','Feature','Projected to Complete in Q4','Priority','Team']
2 : ['User Incremental (WING)','None','None','None', 'None']
Output : True

Input 4:
1 : ['Name','Age','Joined date']
2 : ['Project name','Time taken','Manager name']
Output : False

Input 5:
1 : ['Name','DOB','EmpID']
2 : ['Project name', 'None', 'None']
Output : False

now analyse for this 2 given rows below and return me True or False

Input :
1 : {row_1}
2 : {row_2}
Output:
"""


column_selection_prompt = """
You are given 1-3 rows of a dataframe in the form of lists.
Analyse the data in the rows given to you and based on your intellect decide which row has the column names given(Meaning that row which don't seem like individual data points but rather describe the data in the other rows).
This data is mostly about the project tracking with its related info, so column names are mostly like this.
If the number of columns are 1 or 2, then in most of the cases the row is 1st.
If there are no column names (meaning all the 3 rows which are given to you look very similar to each other), in that case return 0
Remember, return 0 only if you are 100 percent sure that there are no column name/column names and do not output 0 if you are not exactly sure
Consider the context of the data. If one row seems to contain labels or descriptors that apply to the other rows, it might be a column name
Finally if there are any column names then return the row number which has the column names.

The answer should only contain number and nothing else

Input 1:
1 : ['Name','Age','City']
2 : ['Aditya','28','washim']
3 : ['Ramesh',None,'mumbai']
Output : 1

Input 2:
1 : [None,'Value','28','Decrease','20']
2 : ['Roll.no','Address','City','Date']
3 : ['Ramesh','east mumbai 2020','mumbai','20 may 2020']
Output : 2

Input 3:
1 : [None,'None','None','None','20']
2 : ['#','','90',None]
3 : ['Roll.no','Address','City','Date']
Output : 3

Input 4:
1: ['Language:']
2: ['Sanskrit']
3: ['Hindi']
Output : 1

Input 5:
1: ['samsung', 'phone', 'snapdragon']
2: ['apple', 'laptop', 'M1 pro']
3: ['dell', 'laptop', 'intel i5']
Output : 0

Input 6:
1: ['English']
2: ['Sanskrit']
3: ['Hindi']
Output : 0


Input :
1 : {row_1}
2 : {row_2}
3 : {row_3}
Output :"""
