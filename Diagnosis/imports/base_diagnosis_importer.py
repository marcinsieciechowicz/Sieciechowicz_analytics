import os
import sqlite3
from abc import ABC, abstractmethod
import logging
import pandas as pd


class BaseCsvToTableImporter(ABC):
    def __init__(self, src_dir: str, db_path: str, target_table: str):

        self._init_log()

        self._check_src_dir(src_dir)
        self.src_dir = src_dir

        self._check_db_path(db_path)
        self.conn = self._init_conn(db_path)

        self._check_target_table(target_table)
        self.target_table = target_table

    def _init_log(self):
        self.Logger = logging.getLogger(BaseCsvToTableImporter.__name__)
        file = logging.FileHandler('errors.txt', mode='a')
        self.Logger.addHandler(file)
        console = logging.StreamHandler()
        self.Logger.addHandler(console)

    def _check_src_dir(self, src_dir):
        if not os.path.isdir(src_dir):
            error_message = f'no source directory {src_dir} found'
            self.Logger.error(error_message)
            exit()

    def _check_db_path(self, db_path):
        if not os.path.isfile(db_path):
            error_message = f'no database {db_path} found'
            self.Logger.error(error_message)
            exit()

    def _init_conn(self, db_path):
        conn = None
        try:
            return sqlite3.connect(db_path)
        except sqlite3.Error as error:
            self.Logger.error(error)
            exit()
        finally:
            if conn:
                conn.close()

    def _check_target_table(self, target_table):
        cur = self.conn.cursor()
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
        cur.execute(sql, (target_table,))
        if cur.fetchone() is None:
            error_message = f'No target {target_table} table found'
            self.Logger.error(error_message)
            exit()

    def do_import(self):
        try:
            with self.conn:
                for root, dirs, files in os.walk(self.src_dir):
                    for file in files:
                        if file.endswith('.csv'):
                            csv_filepath = os.path.join(root, file)
                            diagnosis_df = self._get_dataframe(csv_filepath)
                            self._validate(diagnosis_df)
                            self._save(diagnosis_df)
                            # self._rename_source_file(csv_filepath)
                            self._move_source_file(csv_filepath)
        finally:
            if self.conn:
                self.conn.close()

    @staticmethod
    def _get_dataframe(csv_filepath: str) -> pd.DataFrame:
        diagnosis_df = pd.read_csv(csv_filepath)
        diagnosis_df = diagnosis_df.fillna('M')

        return diagnosis_df

    def _save(self, diagnosis_df: pd.DataFrame):
        diagnosis_df.to_sql(self.target_table, self.conn, index=False, if_exists='append')

    def _rename_source_file(self, csv_filepath):
        new_filename = csv_filepath.replace('.csv', '.imported')
        try:
            os.rename(csv_filepath, new_filename)
        except OSError:
            error_message = f'unable to rename file {csv_filepath}'
            self.Logger.error(error_message)
            os.replace(csv_filepath, new_filename)

    def _move_source_file(self, csv_filepath: str):
        try:
            dst = csv_filepath.replace('src', 'res')
            os.renames(csv_filepath, dst)
        except FileExistsError:
            error_message = f'File {dst} already exists'
            self.Logger.error(error_message)
        except OSError as e:
            self.Logger.error(e)

    @staticmethod
    @abstractmethod
    def _validate(diagnosis_df):
        pass


class CsvToDiagnosisImporter(BaseCsvToTableImporter):

    def __init__(self, src_dir: str, db_path: str):
        target_table = 'Diagnosis_diagnosis'
        super().__init__(src_dir, db_path, target_table)

    @staticmethod
    def _validate(diagnosis_df):
        pass


diagnosis_importer = CsvToDiagnosisImporter(src_dir='src', db_path='..\..\db.sqlite3')
diagnosis_importer.do_import()

# gdzie zaczynamy kodowanie z pandas schema?