import datetime
import sys
import pandas as pd
from pandas import isnull
import sqlite3

basename = sys.argv[1]
diagnosis_df = pd.read_csv(r'src\%s.csv' % basename)
diagnosis_df = diagnosis_df.fillna('M')
# diagnosis_df.to_string(formatters={'when':conv_date})
# print(diagnosis_df)
# exit()
mmm2mm = {
    'sty': 1,
    'lut': 2,
    'mar': 3,
    'kwi': 4,
    'maj': 5,
    'cze': 6,
    'lip': 7,
    'sie': 8,
    'wrz': 9,
    'paź': 10,
    'lis': 11,
    'gru': 12,
}


def conv_date(row):
    when_parts = row.when.split(',')
    when_parts_1 = when_parts[0].split(' ')
    day = int(when_parts_1[0])
    month = int(mmm2mm[when_parts_1[1]])
    year = int(when_parts[1])
    hour_and_minutes = when_parts[2].split(':')
    hh = int(hour_and_minutes[0])
    mm = int(hour_and_minutes[1])
    dt = datetime.datetime(year, month, day, hh, mm)
    return dt


def set_m_ifnull(row):
    sex = row.sex
    if isnull(sex):
        sex = 'M'
    return sex


def save_queries_into_file():
    with open('res\%s.sql' % basename, 'w+') as sql_file:
        for index, row in diagnosis_df.iterrows():
            visit_dt = conv_date(row)
            sex = set_m_ifnull(row)
            # print(visit_dt, sex, row.icd10)
            sql_file.write(
                'INSERT INTO Diagnosis_diagnosis ("when", sex, icd10) VALUES (\'%s\', \'%s\', \'%s\');\r\n' % (
                    visit_dt, sex, row.icd10))


#save_queries_into_file()



def save_to_database():
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        cur = conn.cursor()
        for index, row in diagnosis_df.iterrows():
            visit_dt = conv_date(row)
            sex = set_m_ifnull(row)
            sql = "INSERT INTO Diagnosis_diagnosis ('when', sex, icd10) VALUES (?, ?, ?)"
            cur.execute(sql, (visit_dt, sex, row.icd10))
            print(cur.lastrowid)

# save_to_database()


def save_to_database_2():
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        diagnosis_df.to_sql('Diagnosis_diagnosis', conn, index=False, if_exists='append')


save_to_database_2()


