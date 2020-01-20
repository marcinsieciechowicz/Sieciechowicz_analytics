import os

import pandas as pd
import sqlite3

class CsvImporter:
    pass
x = CsvImporter()

def save_to_database():
    src_dir = 'src'
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        cur = conn.cursor()
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                if file.endswith('.csv'):
                    csv_filepath = os.path.join(root, file)
                    diagnosis_df = pd.read_csv(csv_filepath)
                    diagnosis_df = diagnosis_df.fillna('M')
                    for index, row in diagnosis_df.iterrows():
                        sql = "INSERT INTO Diagnosis_diagnosis ('when', sex, icd10) VALUES (?, ?, ?)"
                        cur.execute(sql, (row.when, row.sex, row.icd10))
                    rename_source_file(csv_filepath)

all([])
def rename_source_file(csv_filepath):
    new_filename = csv_filepath.replace('.csv', '.imported')
    try:
        os.rename(csv_filepath, new_filename)
    except OSError:
        print('unable to rename file:', csv_filepath)
        os.replace(csv_filepath, new_filename)


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
                rename_source_file(csv_filepath)

#save_to_database_2()
