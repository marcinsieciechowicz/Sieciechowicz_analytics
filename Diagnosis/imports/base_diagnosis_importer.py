import os
import sqlite3
from abc import ABC, abstractmethod

from termcolor import cprint
import pandas as pd


class BaseCsvToTableImporter(ABC):
    def __init__(self, src_dir: str, db_path: str):
        self._check_src_dir(src_dir)
        self._check_db_path(db_path)

        self.src_dir = src_dir
        self.conn = self._init_conn(db_path)

    @staticmethod
    def _init_conn(db_path):
        conn = None
        try:
            return sqlite3.connect(db_path)

        except sqlite3.Error as error:
            cprint(f'ERR: {error}', 'grey', 'on_red')
        finally:
            if conn:
                conn.close()

    @staticmethod
    def _check_db_path(db_path):
        if not os.path.isfile(db_path):
            print(cprint('ERR: no database found', 'grey', 'on_red'))
            exit()

    @staticmethod
    def _check_src_dir(src_dir):
        if not os.path.isdir(src_dir):
            print(cprint('ERR: no source directory found', 'grey', 'on_red'))
            exit()

    def do_import(self):
        try:
            with self.conn:
                for root, dirs, files in os.walk(self.src_dir):
                    for file in files:
                        if file.endswith('.csv'):
                            csv_filepath = os.path.join(root, file)
                            self._save(self._get_dataframe(csv_filepath))
                            self.rename_source_file(csv_filepath)
        finally:
            self.conn.close()

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
            print(cprint('WAR: unable to rename file:', 'grey', 'on_yellow', csv_filepath))
            os.replace(csv_filepath, new_filename)


class CsvToTableWholesaleImporter(BaseCsvToTableImporter):

    def __init__(self, *, src_dir, db_path, target_table: str):
        super().__init__(src_dir, db_path)
        self._check_target_table(target_table)

        self.target_table = target_table

    def _check_target_table(self, target_table):
        cur = self.conn.cursor()
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
        cur.execute(sql, (target_table,))
        if cur.fetchone() is None:
            print(cprint(f'ERR: No target {target_table} table found', 'grey', 'on_red'))
            exit()

    def _save(self, diagnosis_df):
        diagnosis_df.to_sql(self.target_table, self.conn, index=False, if_exists='append')


wholesale_importer = CsvToTableWholesaleImporter(src_dir='src', db_path='..\..\db.sqlite3',
                                                 target_table='Diagnosis_diagnosis')
wholesale_importer.do_import()
