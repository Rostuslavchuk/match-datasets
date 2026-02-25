import pandas as pd
from abc import ABC, abstractmethod
from logger_config import setup_logger
from pathlib import Path
from thefuzz import fuzz

logger = setup_logger(__name__)

class MatchAbstract(ABC):
    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass
    
    @abstractmethod
    def match(self):
        pass

    @abstractmethod
    def load(self):
        pass

class MatchProcess(MatchAbstract):
    def __init__(self, dataset_path1, dataset_path2, addr_cols1, addr_cols2, comp_col):
        self.dataset_path1 = dataset_path1
        self.dataset_path2 = dataset_path2
        self.addr_cols1 = addr_cols1
        self.addr_cols2 = addr_cols2
        self.comp_col = comp_col

    def extract(self):
        logger.info("extracting data...")
        self.ds1, self.ds2 = (pd.read_csv(self.dataset_path1), pd.read_csv(self.dataset_path2))
        logger.info("data extracted successfully")

    def _normalize_data(self, df, addr_cols, zip = None):
        logger.info("normalizing data...")
        df = df.copy()

        df['clean_name'] = df[self.comp_col].astype(str).str.lower()
        df['clean_name'] = df['clean_name'].str.replace(r'[^\w\s]', '', regex=True)

        suffixes = r'\b(inc|ltd|llc|corp|management|systems|lp|reit|holding|holdings|group|canada)\b'
        df['clean_name'] = df['clean_name'].str.replace(suffixes, '', regex=True).str.strip()

        if zip and zip in df.columns:
            df[zip] = df[zip].fillna('').astype(str).str.replace(r'\s+', '', regex=True)

        for col in addr_cols:
            df[col] = df[col].fillna('').astype(str).str.strip().str.lower()

        df['full_address'] = df[addr_cols].apply(
            lambda x: ', '.join([part for part in x if part]),
            axis=1
        )
        logger.info("data normalized successfully")

        return df
    
    def get_metrics(self):
        # Match Rate
        total_count = len(self.final_table)
        matched_count = self.final_table['custname_ds2'].notnull().sum()
        match_rate = (matched_count / total_count) * 100 if total_count > 0 else 0

        # Unmatched Records
        unmatched_count = self.final_table['custname_ds2'].isnull().sum()
        unmatched_rate = (unmatched_count / total_count) * 100 if total_count > 0 else 0
        
        # One-to-Many Matches
        one_to_many_count = (self.counts > 1).sum()
        one_to_many_rate = (one_to_many_count / total_count) * 100 if total_count > 0 else 0
        
        logger.info("--- Matching Metrics ---")
        logger.info(f"Match Rate: {match_rate:.2f}% ({matched_count} records)")
        logger.info(f"Unmatched Records: {unmatched_rate:.2f}% ({unmatched_count} records)")
        logger.info(f"One-to-Many Matches: {one_to_many_rate:.2f}% ({one_to_many_count} cases)")
        logger.info("------------------------")

    def get_fuzzy_overlap(self, list1, list2, threshold=85):
        if not isinstance(list1, list) or not isinstance(list2, list):
            return []
        
        overlap = []
        for addr1 in list1:
            for addr2 in list2:
                score = fuzz.token_sort_ratio(addr1, addr2)
                if score >= threshold:
                    overlap.append(addr1)
                    break 
        return list(set(overlap))

    def transform(self):
        logger.info("transforming data...")

        self.ds1 = self._normalize_data(self.ds1, self.addr_cols1)
        self.ds2 = self._normalize_data(self.ds2, self.addr_cols2, 'zip')

        self.ds1_grouped = self.ds1.groupby('clean_name').agg({
            'custname': 'first',
            'full_address': lambda x: list(set(x))
        }).reset_index().rename(columns={'full_address': 'locations_ds1'})
        
        self.ds2_grouped = self.ds2.groupby('clean_name').agg({
            'custname': 'first',
            'full_address': lambda x: list(set(x))
        }).reset_index().rename(columns={'full_address': 'locations_ds2', 'custname': 'custname_ds2'})
        
        logger.info("data transformed successfully...")

        # for metric (One-to-Many)
        self.counts = self.ds2.groupby('clean_name')['custname'].nunique()

    def match(self):
        logger.info('matching data...')
        self.final_table = self.ds1_grouped.merge(self.ds2_grouped, on='clean_name', how='left')

        self.final_table['overlapping_locations'] = self.final_table.apply(
            lambda row: self.get_fuzzy_overlap(row['locations_ds1'], row['locations_ds2']),
            axis=1
        )
        logger.info('data was matched successfully')
        self.get_metrics()

    def load(self, output_path="matching_results.csv"):
        logger.info('loading data...')

        df_to_save = self.final_table.copy()
        list_cols = ['locations_ds1', 'locations_ds2', 'overlapping_locations']
        
        for col in list_cols:
            df_to_save[col] = df_to_save[col].apply(lambda x: '; '.join(x) if isinstance(x, list) else "")
            
        df_to_save.to_csv(output_path, index=False)
        logger.info(f"data was loaded. file {Path(output_path).name} created")



ADR_COLS_1 = ['sStreet1', 'sStreet2', 'sCity', 'sProvState', 'sCountry', 'sPostalZip']
ADR_COLS_2 = ['address1', 'address2', 'address3', 'city', 'state', 'country', 'zip']
COMPANY_COL = 'custname'

dataProcess = MatchProcess(
    "./company_dataset_1.csv",
    "./company_dataset_2.csv",
    ADR_COLS_1,
    ADR_COLS_2,
    COMPANY_COL
)

dataProcess.extract()
dataProcess.transform()
dataProcess.match()
dataProcess.load()
