import pandas as pd
import hashlib
import logging
import os
import re

# logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger()

# cleaning
def data_cleaning(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text)
    text = text.lower()                            # konsistensi huruf kecil
    text = re.sub(r'http\S+|www\S+', ' ', text)    # hapus url
    text = re.sub(r'\d+', ' ', text)               # hapus angka
    text = re.sub(r'[^\w\s]', ' ', text)           # hapus tanda baca
    text = re.sub(r'[^\x00-\x7f]', r' ', text)     # hapus non-ASCII
    text = re.sub(r'\s+', ' ', text).strip()       # hapus spasi berlebih
    return text

def convert_bulan_ke_angka(series: pd.Series) -> pd.Series:
    bulan_map = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'Mei': '05', 'Jun': '06', 'Jul': '07', 'Agu': '08',
        'Sep': '09', 'Okt': '10', 'Nov': '11', 'Des': '12'
    }
    for bulan, angka in bulan_map.items():
        series = series.str.replace(bulan, angka, regex=False)
    return series

def transform(filename='../transform/data_baru.csv'):

    if not os.path.exists(filename):
        logger.error(f"File {filename} tidak ditemukan. Transform dibatalkan.")
        return None

    df = pd.read_csv(filename)
    
    df['id'] = df['link'].astype(str).apply(lambda url: hashlib.md5(url.encode()).hexdigest())
    df.set_index('id', inplace=True)
    
    df = df.drop_duplicates(subset=['link'])
    
    df['title'] = df['title'].apply(data_cleaning)
    df['content'] = df['content'].apply(data_cleaning)
    df['location'] = df['location'].apply(data_cleaning)

    df = df[~df['link'].fillna("").str.startswith('No')]
    df = df[~df['title'].fillna("").str.startswith('No')]
    df = df[~df['location'].fillna("").str.startswith('No')]
    df = df[~df['datetime'].fillna("").str.startswith('No')]
    df = df[~df['content'].fillna("").str.startswith('No')]

    if 'datetime' in df.columns:
        df['datetime'] = (
            df['datetime'].astype(str)
            .str.replace(r'^\w+, ', '', regex=True)
            .str.replace('WIB', '', regex=False)
            .str.strip())
        df['datetime'] = convert_bulan_ke_angka(df['datetime'])

        df['date'] = pd.to_datetime(
            df['datetime'],
            format="%d %m %Y %H:%M",
            errors='coerce')
        
        df['date'] = df['date'].dt.date

    df = df.dropna()
    
    os.makedirs('../load', exist_ok=True)
    clean_file = '../load/data_bersih.csv'
    df.to_csv(clean_file, index=True)

    logger.info(f'Transform Data Selesai. Disimpan ke {clean_file}')
    return df

# main function
try:
    transform()
except Exception as e:
    logger.exception("Terjadi error saat transform")
