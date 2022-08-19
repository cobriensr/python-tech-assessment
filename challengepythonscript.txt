# the pandas and numpy libraries provide the operations necessary to perform the transformation step in the ETL layer
import pandas as pd
import numpy as np
# this function takes a max character length "n" and cuts x number of characters at "n"
# this function is O(1) as the number of steps does not increase as the number of x instances increase
def max_char_len(x, n):
    if len(x) > n: 
        return x[:n]
    return x 
# import the excel spreadsheet, specifically the "Prospective Learners" tab into a dataframe and use the 0 indexed row as the header, with no index column being set and to use the openpyxl engine as the excel file  is the newer .xlsx version
df = pd.read_excel('C:\\Users\\Charl\\Documents\\Python\\Interview\\Excel-Mapping.xlsx', sheet_name='Prospective Learners', header=0, index_col=None, engine='openpyxl', 
names=[ 'student_id',
        'Name', 
        'How should we refer to you?',	
        'gender',
        'pell_eligible',	
        'applied',
        'can we text you?',	
        'inquiry_date',
        'application_date',	
        'organization_name',	
        'coach_name',
        'degree_program',
        'application_status',	
        'application_status_override'
    ], na_filter=False, #this option is not available in the docs but does exist
usecols=[   'student_id',
            'Name',
            'How should we refer to you?',
            'gender',
            'pell_eligible',
            'applied',
            'can we text you?',
            'inquiry_date',
            'application_date',
            'organization_name',
            'coach_name',
            'degree_program',
            'application_status',
            'application_status_override'
        ]
)
# using a dictionary to replace values as dictionary key values are accessed in O(1) time
df.replace({'Name': {       'Dr. ' : '',
                            ' MD' : '', 
                            ' DDS' : '', 
                            ' PhD' : '', 
                            ' DVM' : '',
                            ' Jr.' : '',
                            ' IV' : '',
                            'Mr. ' : '',
                            'Mrs. ' : '',
                            'Ms. ' : '',
                            ',' : '',
                            ' III' : '',
                            'Miss ' : '',
                            'Von Doom' : 'Von-Doom',
                            'Van Dyne' : 'Van-Dyne',
                            'J. ' : 'J.',
                            'Mary Alice' : 'Mary-Alice',
                            'Barbara Ann' : 'Barbara-Ann',
                            'En Sabah' : 'En-Sabah',
                            ' "Hank"' : ''
                        }
            }, inplace=True, regex=True
        )
# split the name column into two separate columns named "first_name" and "last_name"
df[['first_name', 'last_name']]=df['Name'].str.split(' ', expand=True)
# take out hyphens that were used to concatenate spaced name values for name split
df['first_name'].replace('-', ' ', inplace=True, regex=True)
df['last_name'].replace('-',' ', inplace=True, regex=True)
# drop unneeded columns from the dataframe
df.drop('Name', axis=1, inplace=True)
df.drop('applied', axis=1, inplace=True)
# reposition columns in the dataframe
first_name = df.pop('first_name')
df.insert(1, 'first_name', first_name)
last_name = df.pop('last_name')
df.insert(2, 'last_name', last_name)
# using a dictionary to replace values as dictionary key values are accessed in O(1) time
df.rename(columns={ 'How should we refer to you?':'preferred_name', 
                    'can we text you?':'ok_to_text',
                    'organization_name':'institution_name',
                    'degree_program':'program',
                    'application_status':'current_status'
                }, inplace=True
        )
# setting a blank last name value to keep the observation
df.loc[df.first_name == 'Loki', ['last_name']] = 'NoLastName'
# converting into a datetime object and then setting the missing dates to epoch time as the ETL work is mainly handled in snaplogic which uses JavaScript so epoch time is the most time friendly selection
df['inquiry_date'] = pd.to_datetime(df['inquiry_date'])
df['inquiry_date'].fillna('01/01/1970',inplace=True)
df['inquiry_date'] = df['inquiry_date'].dt.date
# replace blanks with NaN values so we can override the current_status values with application_status_override values where application_status_override values are NOT NaN
df['application_status_override'].replace(r'^\s*$', 'NaN', regex=True, inplace = True)
# using a dictionary to replace values as dictionary key values are accessed in O(1) time
df.replace({'application_status_override': {    'is submitted' : 'Submitted Application',
                                                'not yet submitted' : 'Started Application',
                                                'Started' : 'Started Application',
                                                'Submitted' : 'Submitted Application',
                                                'withdrawn' : 'Application Withdrawn',
                                                'Withdrawn' : 'Application Withdrawn'
                                            }
            }, inplace=True, regex=True
        )
# override values in current_status with values from the application_status_override column where the value in the application_status_override column is NOT 'NaN'
df['current_status'] = np.where(df['application_status_override'] !='NaN', df['application_status_override'], df['current_status'])
# drop unneeded column
df.drop('application_status_override', axis=1, inplace=True)
# replace blank string values with NaN values in order to drop those observations as there is not an allowable "missing data / no data" value to use
df['current_status'].replace(r'^\s*$', np.nan, regex=True, inplace = True)
df = df.dropna(axis=0, subset = ['current_status'])
# replacing value to match data mapping required values
df['institution_name'].replace('C', 'Organization C', inplace=True)
# using a dictionary to replace values as dictionary key values are accessed in O(1) time
df.replace({'gender': { 'Prefer Not to Specify': 'Not Specified',
                        'Non-Binary' : 'Non-binary'
                    }
            }, inplace=True, regex=True
        )
# replacing values to match data mapping required values. Not using a dictionary value for this replacement as it needs a regex search for the missing values
df['gender'].replace(r'^\s*$', 'Not Specified', regex=True, inplace = True)
# using a dictionary to replace values as dictionary key values are accessed in O(1) time
df.replace({'ok_to_text': { 'no' : False,
                            'yes' : True
                        }
            }, inplace=True, regex=True
        )
# using a dictionary to replace values as dictionary key values are accessed in O(1) time
df.replace({'pell_eligible': {  True : 'Eligible',
                                False : 'Not Eligible'
                            }
            }, inplace=True, regex=True
        )
# replacing blank values with NaN values to fill in missing values with "Not Specified"
df['pell_eligible'].replace(r'^\s*$', np.nan, regex=True, inplace = True)
df['pell_eligible'].fillna('Not Specified',inplace=True)
# this lambda (anonymous) function is in O(n) time as it loops through each element of the column to apply the character length function and n increases as each element of the column increases. This is basically a for x in in program loop.
df['program'] = df['program'].apply(lambda x: max_char_len(x, 15)) 
# drop the 1 duplicate value in the student_id "external_id" column and keep the first observation
df.drop_duplicates(subset=['student_id'], keep='first', inplace=True)
# export the file as a csv to import into the DB. Instead of exporting to a CSV, you can connect to a SQL DB instance and load the dataframe via a db connector (pymysql, pyodbc, pymssql, etc)
df.to_csv('C:\\Users\\Charl\\Documents\\Python\\Interview\\Output.csv', index=False)