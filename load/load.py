import pandas as pd
from sqlalchemy import create_engine, inspect
import logging
import os

# log
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger()

# connect to database
DB_URI = 'postgresql+psycopg2://postgres:XXXXXX@localhost:5432/artikel_berita'
TABLE_NAME = 'data_artikel'

def main(csv_file='../load/data_bersih.csv'):
    logger.info('Tunggu...')
    if not os.path.exists(csv_file):
        logger.warning(f"File {csv_file} tidak ditemukan. Load dibatalkan.")
        return

    df = pd.read_csv(csv_file)

    if df.empty:
        logger.warning("CSV kosong. Tidak ada data untuk di-load.")
        return

    columns_order = ['id','link','date','location','title','content']

    columns_order = [col for col in columns_order if col in df.columns]
    df = df[columns_order]

    # load ke database
    try:
        engine = create_engine(DB_URI)
        inspector = inspect(engine)

        if TABLE_NAME in inspector.get_table_names():
            df_existing = pd.read_sql_table(TABLE_NAME, engine) # ambil data existing
            df_new = df[~df['id'].isin(df_existing['id'])] # filter baris baru berdasarkan id

            if df_new.empty:
                logger.info("Tidak ada data baru untuk ditambahkan. Semua baris duplikat.")
                return

            df_new.to_sql(TABLE_NAME, engine, index=False, if_exists='append')
            logger.info(f"{len(df_new)} rows ditambahkan ke tabel '{TABLE_NAME}'")

        else:
            df.to_sql(TABLE_NAME, engine, index=False, if_exists='replace') # jika tabel belum ada, buat tabel baru dengan semua data
            logger.info(f"Tabel '{TABLE_NAME}' belum ada, dibuat baru dengan {len(df)} rows")

    except Exception as e:
        logger.error(f"Gagal load ke database: {e}")


# main function
try:
    main()
except Exception as e:
    logger.exception("Terjadi error saat load")