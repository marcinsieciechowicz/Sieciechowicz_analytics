import os

import pandas as pd
import sqlite3


def save_to_database():
    src_dir = 'src'
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        cur = conn.cursor()
        for entry in os.listdir(src_dir):
            for src_dir in os.walk('.csv', topdown=True):
                if entry.endswith('.csv'):
                    csv_filepath = os.path.join(src_dir, entry)
                    diagnosis_df = pd.read_csv(csv_filepath)
                    diagnosis_df = diagnosis_df.fillna('M')
                    for index, row in diagnosis_df.iterrows():
                        sql = "INSERT INTO Diagnosis_diagnosis ('when', sex, icd10) VALUES (?, ?, ?)"
                        # cur.execute(sql, (row.when, row.sex, row.icd10))
                    os.rename(csv_filepath, csv_filepath.replace('.csv', '.imported'))


save_to_database()


def save_to_database_2():
    src_dir = 'src'
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        for entry in os.listdir(src_dir):
            if entry.endswith('.csv'):
                csv_filepath = os.path.join(src_dir, entry)
                diagnosis_df = pd.read_csv(csv_filepath)
                diagnosis_df = diagnosis_df.fillna('M')
                diagnosis_df.to_sql('Diagnosis_diagnosis', conn, index=False, if_exists='append')


#save_to_database_2()


# os.listdir nie dziala, bo dziala tylko dla jednego poziomu, a chcemy wchodzic glebiej i glebiej, ta funkcja to os.walk.
# Wykminic jak to zrobic, zastapic tak, by dzialalo