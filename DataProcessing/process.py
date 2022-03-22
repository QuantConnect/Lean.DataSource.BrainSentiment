import os
import boto3
import sqlalchemy

from pathlib import Path
from botocore.exceptions import ClientError

# CLRImports is required to handle Lean C# objects
from CLRImports import *

from universe import UniverseDataProcessing

S3_USER_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
S3_USER_KEY_ACCESS = os.environ['AWS_SECRET_ACCESS_KEY']
S3_BUCKET_NAME = os.environ['BRAIN_S3_BUCKET_NAME']

DATE_FORMAT = '%Y-%m-%d'
OUTPUT_DATE_FORMAT = '%Y%m%d'

OUTPUT_DATA_PATH = Path('/temp-output-directory') / 'alternative' / 'brain'
PROCESS_ALL = False if 'PROCESS_ALL' not in os.environ else os.environ['PROCESS_ALL'].lower() == 'true'
PROCESS_DATE = datetime(2010, 1, 1) if PROCESS_ALL else datetime.strptime(os.environ['QC_DATAFLEET_DEPLOYMENT_DATE'], '%Y%m%d')
PROCESS_DATE_STR = PROCESS_DATE.strftime(DATE_FORMAT)

LOCAL_FOLDER = Path('./output')

REPORT_KEY_PREFIX = 'BLMCF_V2'
SENTIMENT_KEY_PREFIX = 'BSI'
RANKINGS_KEY_PREFIX = 'BSR'

REPORT_10K_CATEGORY = f'{REPORT_KEY_PREFIX}_10K'
REPORT_ALL_CATEGORY = f'{REPORT_KEY_PREFIX}_ALL' # 2010-01-01
REPORT_DIFF_10K_CATEGORY = f'{REPORT_KEY_PREFIX}_DIFF_10K'
REPORT_DIFF_ALL_CATEGORY = f'{REPORT_KEY_PREFIX}_DIFF_ALL'
SENTIMENT_CATEGORY = SENTIMENT_KEY_PREFIX # 2016-08-01
RANKINGS_CATEGORY = RANKINGS_KEY_PREFIX # 2010-01-01

FILE_PREFIXES = [
    ('differences_10k', None, REPORT_DIFF_10K_CATEGORY),
    ('differences_all', None, REPORT_DIFF_ALL_CATEGORY),
    ('metrics_10k', None, REPORT_10K_CATEGORY),
    ('metrics_all', None, REPORT_ALL_CATEGORY),

    ('sentimentDays7', 7, SENTIMENT_CATEGORY),
    ('sentimentDays30', 30, SENTIMENT_CATEGORY),

    ('mlAlpha2Days', 2, RANKINGS_CATEGORY),
    ('mlAlpha3Days', 3, RANKINGS_CATEGORY),
    ('mlAlpha5Days', 5, RANKINGS_CATEGORY),
    ('mlAlpha10Days', 10, RANKINGS_CATEGORY),
    ('mlAlpha21Days', 21, RANKINGS_CATEGORY)
]

REPORT_CATEGORIES = [
    REPORT_10K_CATEGORY,
    REPORT_DIFF_10K_CATEGORY,
    REPORT_ALL_CATEGORY,
    REPORT_DIFF_ALL_CATEGORY
]

CATEGORY_KEY_PREFIXES = {
    REPORT_10K_CATEGORY: REPORT_KEY_PREFIX,
    REPORT_ALL_CATEGORY: REPORT_KEY_PREFIX,
    REPORT_DIFF_10K_CATEGORY: REPORT_KEY_PREFIX,
    REPORT_DIFF_ALL_CATEGORY: REPORT_KEY_PREFIX,
    SENTIMENT_CATEGORY: SENTIMENT_KEY_PREFIX,
    RANKINGS_CATEGORY: RANKINGS_KEY_PREFIX,
}

OUTPUT_DIRECTORY_NAMES = {
    RANKINGS_CATEGORY: 'rankings',
    SENTIMENT_CATEGORY: 'sentiment',
    REPORT_10K_CATEGORY: 'report_10k',
    REPORT_ALL_CATEGORY: 'report_all',
}


class BrainProcessor:
    def __init__(self, files):
        self.files = files
        self.figi_map = None
        self.map_file_provider = LocalZipMapFileProvider()
        self.map_file_provider.Initialize(DefaultDataProvider())
        self.map_file_resolver = self.map_file_provider.Get(Market.USA)

        self.category_parsing_columns = {
            RANKINGS_CATEGORY: ['ML_ALPHA'],
            SENTIMENT_CATEGORY: ['VOLUME', 'VOLUME_SENTIMENT', 'SENTIMENT_SCORE', 'BUZZ_VOLUME', 'BUZZ_VOLUME_SENTIMENT'],
            REPORT_ALL_CATEGORY: [
                'LAST_REPORT_DATE',
                'LAST_REPORT_CATEGORY',
                'N_SENTENCES',
                'MEAN_SENTENCE_LENGTH',
                'SENTIMENT',
                'SCORE_UNCERTAINTY',
                'SCORE_LITIGIOUS',
                'SCORE_CONSTRAINING',
                'SCORE_INTERESTING',
                'READABILITY',
                'LEXICAL_RICHNESS',
                'LEXICAL_DENSITY',
                'SPECIFIC_DENSITY',
                'RF_N_SENTENCES',
                'RF_MEAN_SENTENCE_LENGTH',
                'RF_SENTIMENT',
                'RF_SCORE_UNCERTAINTY',
                'RF_SCORE_LITIGIOUS',
                'RF_SCORE_CONSTRAINING',
                'RF_SCORE_INTERESTING',
                'RF_READABILITY',
                'RF_LEXICAL_RICHNESS',
                'RF_LEXICAL_DENSITY',
                'RF_SPECIFIC_DENSITY',
                'MD_N_SENTENCES',
                'MD_MEAN_SENTENCE_LENGTH',
                'MD_SENTIMENT',
                'MD_SCORE_UNCERTAINTY',
                'MD_SCORE_LITIGIOUS',
                'MD_SCORE_CONSTRAINING',
                'MD_SCORE_INTERESTING',
                'MD_READABILITY',
                'MD_LEXICAL_RICHNESS',
                'MD_LEXICAL_DENSITY',
                'MD_SPECIFIC_DENSITY'
            ],
            REPORT_DIFF_ALL_CATEGORY: [
                'LAST_REPORT_DATE',
                'LAST_REPORT_CATEGORY',
                'LAST_REPORT_PERIOD',
                'PREV_REPORT_DATE',
                'PREV_REPORT_CATEGORY',
                'PREV_REPORT_PERIOD',
                'SIMILARITY_ALL',
                'SIMILARITY_POSITIVE',
                'SIMILARITY_NEGATIVE',
                'SIMILARITY_UNCERTAINTY',
                'SIMILARITY_LITIGIOUS',
                'SIMILARITY_CONSTRAINING',
                'SIMILARITY_INTERESTING',
                'RF_SIMILARITY_ALL',
                'RF_SIMILARITY_POSITIVE',
                'RF_SIMILARITY_NEGATIVE',
                'MD_SIMILARITY_ALL',
                'MD_SIMILARITY_POSITIVE',
                'MD_SIMILARITY_NEGATIVE'
            ]
        }

        self.category_parsing_columns[REPORT_10K_CATEGORY] = list(self.category_parsing_columns[REPORT_ALL_CATEGORY])
        self.category_parsing_columns[REPORT_DIFF_10K_CATEGORY] = list(self.category_parsing_columns[REPORT_DIFF_ALL_CATEGORY])

        self.db_name = os.environ.get('DB_NAME')
        self.db_host = os.environ.get('DB_HOST')
        self.db_port = os.environ.get('DB_PORT', 3306)
        self.db_user = os.environ.get('DB_USER')
        self.db_pass = os.environ.get('DB_PASS')
        self.db_security_table = os.environ['DB_SECDEF_TABLE']

        self.has_db_connection = self.db_name is not None and self.db_host is not None and self.db_user is not None
        self.db_connection_info = None

        if self.has_db_connection:
            self.db_connection_info = f'mysql+pymysql://{self.db_user}'
            self.db_connection_info += f':{self.db_pass}' if self.db_pass is not None else ''
            self.db_connection_info += f'@{self.db_host}:{self.db_port}/{self.db_name}'

    def filter_files_by_category(self, category):
        return [(file_path, lookback_days, date) for (file_path, lookback_days, cat, date) in self.files if cat == category]

    def figi_to_mapped_ticker(self, ticker, figi, trading_date):
        sid = self.figi_to_sid(figi)
        if sid is not None:
            return self.map_sid(sid, trading_date)

        return self.map_ticker(ticker, trading_date)

    def map_ticker(self, ticker, trading_date):
        if ticker is None:
            return None

        map_file = self.map_file_resolver.ResolveMapFile(ticker, datetime.now())
        return map_file.GetMappedSymbol(trading_date, None)

    def map_sid(self, sid, trading_date):
        if sid is None:
            return None

        map_file = self.map_file_resolver.ResolveMapFile(sid.Symbol, sid.Date)
        return map_file.GetMappedSymbol(trading_date, None)

    def figi_to_sid(self, figi):
        if not self.has_db_connection:
            return None

        if (self.figi_map is None or not any(self.figi_map)) and not self.try_create_figi_map():
            print('Failed to create FIGI lookup table')
            return None

        return self.figi_map.get(figi)

    def try_create_figi_map(self):
        if not self.has_db_connection:
            print('No connection to the security definition database')
            return False

        self.figi_map = {}

        db_connection = sqlalchemy.create_engine(self.db_connection_info)
        df = pd.read_sql(f'SELECT figi, sid, ticker FROM {self.db_security_table} ORDER BY id DESC', con=db_connection)

        if df.empty:
            print('Database contains no FIGI/ticker entries')
            return False

        df = df.drop_duplicates(subset='figi')

        for _, secdef in df.iterrows():
            figi = secdef['figi']
            sid = secdef['sid']

            if not figi or figi.isspace():
                continue

            if not sid or sid.isspace():
                continue

            try:
                self.figi_map[figi] = SecurityIdentifier.Parse(sid)
            except:
                print(f'Failed to parse SID: {sid} for Security: {figi}')

        return any(self.figi_map)

    def parse_raw(self, file, category, date, lookback_days=None):
        columns = list(self.category_parsing_columns[category]) + ['COMPOSITE_FIGI']
        if lookback_days is not None:
            columns.append('lookback_days')

        if not file.exists():
            return self.create_empty_df(columns)

        sec_def_columns = ['COMPOSITE_FIGI']
        ticker_column = None

        df = pd.read_csv(file)
        if 'TICKER' in df:
            ticker_column = 'TICKER'
            sec_def_columns.insert(0, 'TICKER')
        elif 'PRIMARY_EXCHANGE_TICKER' in df:
            ticker_column = 'PRIMARY_EXCHANGE_TICKER'
            sec_def_columns.insert(0, 'PRIMARY_EXCHANGE_TICKER')

        df['date'] = date
        df['ticker'] = df[sec_def_columns].apply(lambda sec_def: self.figi_to_mapped_ticker(sec_def[ticker_column] if ticker_column is not None else None, sec_def['COMPOSITE_FIGI'], date), axis=1)
        df = df[~df['ticker'].isnull()]
        df = df.set_index('date', append=False).set_index('ticker', append=True)

        if lookback_days is not None:
            df['lookback_days'] = str(lookback_days)

        return df[columns]

    def process(self):
        category_files = {k: self.filter_files_by_category(k) for k in CATEGORY_KEY_PREFIXES.keys()}
        category_df_collection = {k: [] for k in CATEGORY_KEY_PREFIXES.keys()}
        category_dfs = {k: None for k in CATEGORY_KEY_PREFIXES.keys()}

        for category, files in category_files.items():
            for file_path, lookback_days, date in files:
                print(f'Parsing {file_path}')
                df = self.parse_raw(file_path, category, date, lookback_days)
                category_df_collection[category].append(df)

        for category, dfs in category_df_collection.items():
            if len(dfs) == 0:
                print(f'No DataFrame created for category: {category}')
                continue

            df = pd.concat(dfs)
            if df.empty:
                print(f'No data in DataFrame for category: {category}')
                continue

            category_dfs[category] = df.sort_index(level=[1, 0])

        df_report_10k_merged = self.merge_reports(category_dfs[REPORT_10K_CATEGORY], category_dfs[REPORT_DIFF_10K_CATEGORY])
        df_report_all_merged = self.merge_reports(category_dfs[REPORT_ALL_CATEGORY], category_dfs[REPORT_DIFF_ALL_CATEGORY])

        category_dfs[REPORT_10K_CATEGORY] = df_report_10k_merged
        category_dfs[REPORT_ALL_CATEGORY] = df_report_all_merged
        del category_dfs[REPORT_DIFF_10K_CATEGORY]
        del category_dfs[REPORT_DIFF_ALL_CATEGORY]

        month_start = PROCESS_DATE - timedelta(days=PROCESS_DATE.day - 1)
        category_dfs = {
            k: df.loc[df.index.get_level_values('date') >= month_start].drop(columns=['COMPOSITE_FIGI']) for k, df in category_dfs.items() if df is not None and not df.empty
        }

        self.write(category_dfs)

    def write(self, categories_data):
        for category, df in categories_data.items():
            if df is None or df.empty:
                print(f'Skipping category: {category}')
                continue

            print(f'Begin writing data for category: {category}')

            has_lookback = 'lookback_days' in df
            groupby_columns = ['ticker']
            if has_lookback:
                groupby_columns.append('lookback_days')

            for index, df_ticker in df.groupby(groupby_columns):
                ticker = index[0] if has_lookback else index
                directory_name = OUTPUT_DIRECTORY_NAMES[category]
                output_path = OUTPUT_DATA_PATH / directory_name
                lookback_days = None

                if has_lookback:
                    lookback_days = index[1]
                    output_path = output_path / lookback_days
                    df_ticker = df_ticker.drop(columns=['lookback_days'])

                output_path = output_path / PROCESS_DATE.strftime('%Y%m')
                output_path.mkdir(parents=True, exist_ok=True)
                output_path = output_path / f'{ticker.lower()}.csv'

                df_ticker = df_ticker.reset_index(level=1, drop=True)
                df_ticker.to_csv(output_path, header=False, index=True, date_format=OUTPUT_DATE_FORMAT, float_format='%f')
                print(f'Finished writing {category}/{directory_name}/{lookback_days} data :: {ticker}')

    def create_empty_df(self, columns):
        return pd.DataFrame(columns=columns, index=[['date'], ['ticker']], dtype=object).dropna()

    def merge_reports(self, df_report, df_report_diff):
        if df_report is None:
            df_report = self.create_empty_df(self.category_parsing_columns[REPORT_ALL_CATEGORY] + ['COMPOSITE_FIGI'])
        if df_report_diff is None:
            df_report_diff = self.create_empty_df(self.category_parsing_columns[REPORT_DIFF_ALL_CATEGORY] + ['COMPOSITE_FIGI'])

        df_report = df_report.set_index('COMPOSITE_FIGI', append=True)
        df_report_diff = df_report_diff.set_index('COMPOSITE_FIGI', append=True)

        # These columns appear in both data sets, and won't play nicely if we
        # try to join both DataFrames with the same columns
        df_report_diff = df_report_diff.drop(columns=['LAST_REPORT_DATE', 'LAST_REPORT_CATEGORY'])

        return df_report.join(df_report_diff)\
            .reset_index(level=2)\
            .drop_duplicates()

def get_business_dates(date_start, date_end, date_format='dt'):
    """ Get business dates """
    dates = pd.date_range(date_start, date_end, freq='B')
    dts = [date.to_pydatetime() for date in dates]
    if date_format == 'dt':
        return dts
    else:
        return [dt.strftime(DATE_FORMAT) for dt in dts]

def download_file_s3(s3, file_prefix, category, date):
    file_name = f'{file_prefix}_{date.strftime(OUTPUT_DATE_FORMAT)}.csv'
    file_path = LOCAL_FOLDER / file_name

    if file_path.exists():
        print(f'File already exists: {file_name}')
        return file_path

    key_prefix = CATEGORY_KEY_PREFIXES[category]
    remote_key = f'{key_prefix}/{file_name}'

    try:
        s3.Bucket(S3_BUCKET_NAME).download_file(remote_key, str(file_path))
    except ClientError as e:
        print(f'{str(e)} - Failed to download {file_name}')
        return None

    print(f'Finished downloading: {file_name}')
    return file_path

def download():
    LOCAL_FOLDER.mkdir(parents=True, exist_ok=True)

    # -- Get file names until current date
    if PROCESS_ALL:
        date_start = PROCESS_DATE
        date_end = datetime.now()
    else:
        date_start = (PROCESS_DATE - timedelta(days=PROCESS_DATE.day - 1))
        date_end = pd.date_range(start=date_start, periods=1, freq='M')[0].to_pydatetime()

    dates = get_business_dates(date_start, date_end)

    file_names = [(*prefix_data, date) for date in dates for prefix_data in FILE_PREFIXES]
    n_files = len(file_names)

    # -- Connect to S3
    session = boto3.Session(
            aws_access_key_id=S3_USER_KEY_ID,
            aws_secret_access_key=S3_USER_KEY_ACCESS,
        )
    s3 = session.resource('s3')

    downloaded_files = []

    # -- Download files
    for file_key, lookback_days, category, date in file_names:
        if date == date_start and category in REPORT_CATEGORIES:
            previous_file_date = date_start
            oldest_file_date = previous_file_date - timedelta(days=14)

            while previous_file_date > oldest_file_date:
                previous_file_date = previous_file_date - timedelta(days=1)
                if previous_file_date.weekday() >= 5:
                    continue

                file_path = download_file_s3(s3, file_key, category, previous_file_date)
                if file_path is not None:
                    downloaded_files.append((file_path, lookback_days, category, previous_file_date))
                    break

            continue

        file_path = download_file_s3(s3, file_key, category, date)
        if file_path is not None:
            downloaded_files.append((file_path, lookback_days, category, date))

    return downloaded_files


def main():
    files = download()
    processor = BrainProcessor(files)
    processor.process()

    universe_processor = UniverseDataProcessing(processor.map_file_provider, OUTPUT_DATA_PATH)
    universe_processor.report_10k_universe_creation()
    universe_processor.report_all_universe_creation()
    universe_processor.rank_universe_creation()
    universe_processor.sentiment_universe_creation()

if __name__ == '__main__':
    main()