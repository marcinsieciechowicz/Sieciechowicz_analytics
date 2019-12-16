import os
import pandas as pd
import sqlite3


def save_to_database():
    conn = sqlite3.connect('..\..\db.sqlite3')
    with conn:
        cur = conn.cursor()
        for index, row in diagnosis_df.iterrows():
            sql = "INSERT INTO Diagnosis_diagnosis ('when', sex, icd10) VALUES (?, ?, ?)"
            cur.execute(sql, (row.when, row.sex, row.icd10))
            print(cur.lastrowid)


def save_to_database_2():
    src_dir = 'src'
    con = sqlite3.connect('..\..\db.sqlite3')
    with con:
        for entry in os.listdir(src_dir):
            if entry.endswith('.csv'):
                csv_filepath = os.path.join(src_dir, entry)
                diagnosis_df = pd.read_csv(csv_filepath)
                diagnosis_df = diagnosis_df.fillna('M')
                diagnosis_df.to_sql('Diagnosis_diagnosis', con, index=False, if_exists='append')


save_to_database_2()

# dostosowac kod z save_to_database by z kazdego pliku csv zczytywal, czyli ma robic to samo co save_to_database_2,
# bo teraz dziala tylko dla jednego dataframe, poczytac o