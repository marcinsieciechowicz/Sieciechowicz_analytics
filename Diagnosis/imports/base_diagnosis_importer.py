import os
import sqlite3
from abc import ABC, abstractmethod

import pandas as pd


class BaseDiagnosisImporter(ABC):
    def __init__(self, src_dir, db_path):
        self.src_dir = src_dir
        self.db_path = db_path

    def do_import(self):
        for root, dirs, files in os.walk(self.src_dir):
            for file in files:
                if file.endswith('.csv'):
                    csv_filepath = os.path.join(root, file)
                    with self.conn:
                        diagnosis_df = pd.read_csv(csv_filepath)
                        diagnosis_df = diagnosis_df.fillna('M')
                        self._save(diagnosis_df)
                    self.rename_source_file(csv_filepath)

    @property
    def conn(self):
        return sqlite3.connect(self.db_path)

    @staticmethod
    def rename_source_file(csv_filepath):
        new_filename = csv_filepath.replace('.csv', '.imported')
        try:
            os.rename(csv_filepath, new_filename)
        except OSError:
            print('unable to rename file:', csv_filepath)
            os.replace(csv_filepath, new_filename)

    @abstractmethod
    def _save(self, diagnosis_df):
        pass


class DiagnosisDataFrameWholesaleImporter(BaseDiagnosisImporter):

    def _save(self, diagnosis_df):

        diagnosis_df.to_sql('Diagnosis_diagnosis', self.conn, index=False, if_exists='append')



class DiagnosisDataFrameIterativeImporter(BaseDiagnosisImporter):

    def _save(self, diagnosis_df):
        diagnosis_df.sql = "INSERT INTO Diagnosis_diagnosis ('when', sex, icd10) VALUES (?, ?, ?)"




diagnosis_importer_iterative = DiagnosisDataFrameIterativeImporter('src', '..\..\db.sqlite3')
diagnosis_importer_iterative.do_import()
diagnosis_importer_wholesale = DiagnosisDataFrameWholesaleImporter('src', '..\..\db.sqlite3')


