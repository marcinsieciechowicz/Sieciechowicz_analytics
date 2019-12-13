import datetime
import os
import sys
import pandas as pd
from pandas import isnull
import sqlite3

# basename = sys.argv[1]
# diagnosis_df = diagnosis_df.fillna('M')
# print(diagnosis_df)
# exit()

def save_queries_into_file():
    with open(r'res\%s.sql' % basename, 'w+') as sql_file:
        for index, row in diagnosis_df.iterrows():
            visit_dt = conv_date(row)
            # print(visit_dt, sex, row.icd10)
            sql_file.write(
                'INSERT INTO Diagnosis_diagnosis ("when", sex, icd10) VALUES (\'%s\', \'%s\', \'%s\');\r\n' % (
                    visit_dt, row.sex, row.icd10))


#save_queries_into_file()



def save_to_database():
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        cur = conn.cursor()
        for index, row in diagnosis_df.iterrows():
            visit_dt = conv_date(row)
            sql = "INSERT INTO Diagnosis_diagnosis ('when', sex, icd10) VALUES (?, ?, ?)"
            cur.execute(sql, (visit_dt, row.sex, row.icd10))
            print(cur.lastrowid)

# save_to_database()


def save_to_database_2():
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        diagnosis_df.to_sql('Diagnosis_diagnosis', conn, index=False, if_exists='append')


# save_to_database_2()
# src_dir = 'src'
# for filename in os.listdir(src_dir):
#     if filename.endswith('.csv'):
#         diagnosis_df = pd.read_csv(os.path.join(src_dir, filename))
#         print(diagnosis_df)

# for variable in os.listdir(src_dir):
src_dir = 'src'
for entry in os.listdir(src_dir):
    if entry.endswith('.csv'):
        csv_filepath = os.path.join(src_dir, entry)
        diagnosis_dataframe = pd.read_csv(csv_filepath)
        print(diagnosis_dataframe)