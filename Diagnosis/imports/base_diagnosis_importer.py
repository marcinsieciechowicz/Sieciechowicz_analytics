import os
import sqlite3
from abc import ABC, abstractmethod

import pandas as pd


class BaseCsvToTableImporter(ABC):
    def __init__(self, src_dir, db_path):
        self.src_dir = src_dir
        self.conn = sqlite3.connect(db_path)

    def do_import(self):
        with self.conn:
            for root, dirs, files in os.walk(self.src_dir):
                for file in files:
                    if file.endswith('.csv'):
                        csv_filepath = os.path.join(root, file)
                        self._save(self._get_dataframe(csv_filepath))
                        self.rename_source_file(csv_filepath)

    @staticmethod
    def _get_dataframe(csv_filepath: str) -> pd.DataFrame:
        diagnosis_df = pd.read_csv(csv_filepath)
        diagnosis_df = diagnosis_df.fillna('M')

        return diagnosis_df

    @abstractmethod
    def _save(self, diagnosis_df: pd.DataFrame):
        pass

    @staticmethod
    def rename_source_file(csv_filepath):
        new_filename = csv_filepath.replace('.csv', '.imported')
        try:
            os.rename(csv_filepath, new_filename)
        except OSError:
            print('unable to rename file:', csv_filepath)
            os.replace(csv_filepath, new_filename)


class CsvToTableWholesaleImporter(BaseCsvToTableImporter):

    def __init__(self, *, src_dir, db_path, target_table):
        super().__init__(src_dir, db_path)

        self.target_table = target_table

    def _save(self, diagnosis_df):
        diagnosis_df.to_sql(self.target_table, self.conn, index=False, if_exists='append')


class CsvToDiagnosis_DiagnosisTableIterativeImporter(BaseCsvToTableImporter):

    def _save(self, diagnosis_df):
        cur = self.conn.cursor()
        for index, row in diagnosis_df.iterrows():
            sql = "INSERT INTO Diagnosis_diagnosis ('when', sex, icd10) VALUES (?, ?, ?)"
            cur.execute(sql, (row.when, row.sex, row.icd10))


# iterative_importer = CsvToDiagnosis_DiagnosisTableIterativeImporter('src', '..\..\db.sqlite3')
# iterative_importer.do_import()
wholesale_importer = CsvToTableWholesaleImporter(src_dir='src', db_path='..\..\db.sqlite3',
                                                 target_table='Diagnosis_diagnosis')
wholesale_importer.do_import()
