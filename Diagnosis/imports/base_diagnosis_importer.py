import os
import sqlite3
from abc import ABC, abstractmethod

from termcolor import cprint
import pandas as pd


class BaseCsvToTableImporter(ABC):
    def __init__(self, src_dir: str, db_path: str, target_table: str):
        self._check_src_dir(src_dir)
        self.src_dir = src_dir

        self._check_db_path(db_path)
        self.conn = self._init_conn(db_path)

        self._check_target_table(target_table)
        self.target_table = target_table

    @staticmethod
    def _check_src_dir(src_dir):
        if not os.path.isdir(src_dir):
            print(cprint('ERR: no source directory found', 'grey', 'on_red'))
            exit()

    @staticmethod
    def _check_db_path(db_path):
        if not os.path.isfile(db_path):
            print(cprint('ERR: no database found', 'grey', 'on_red'))
            exit()

    @staticmethod
    def _init_conn(db_path):
        conn = None
        try:
            return sqlite3.connect(db_path)
        except sqlite3.Error as error:
            cprint(f'ERR: {error}', 'grey', 'on_red')
            exit()
        finally:
            if conn:
                conn.close()

    def _check_target_table(self, target_table):
        cur = self.conn.cursor()
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
        cur.execute(sql, (target_table,))
        if cur.fetchone() is None:
            cprint(f'ERR: No target {target_table} table found', 'grey', 'on_red')
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
                            # self._move_source_file(csv_filepath)
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

    @staticmethod
    def _rename_source_file(csv_filepath):
        new_filename = csv_filepath.replace('.csv', '.imported')
        try:
            os.rename(csv_filepath, new_filename)
        except OSError:
            print(cprint('WAR: unable to rename file:', 'grey', 'on_yellow', csv_filepath))
            os.replace(csv_filepath, new_filename)

    # @staticmethod
    # def _move_source_file(csv_filepath):
    #     csv_dirpath = os.path.dirname(csv_filepath)
    #     dst_csv_dirpath = csv_dirpath.replace('src', 'res')
    #     if not os.path.exists(dst_csv_dirpath):
    #         os.makedirs(dst_csv_dirpath)
    #     dst_csv_filepath = csv_filepath.replace('src', 'res')
    #     os.rename(csv_filepath, dst_csv_filepath)
    @staticmethod
    def _move_source_file(csv_filepath: str):
        try:
            dst = csv_filepath.replace('src', 'res')
            dst_dirs, _ = os.path.split(dst)
            if not os.path.exists(dst_dirs):
                os.makedirs(dst_dirs)
            os.rename(csv_filepath, dst)
        except FileExistsError:
            cprint(f'File {dst} already exists', 'grey', 'on_red')


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

# read about pandas schema, connected with the code, which validator you have to use?

# make 'when' in Diagnosis_diagnosis unique, by migration
