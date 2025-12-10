import csv
import os
import time
import random
import hashlib
import traceback
import logging
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

DB_URI = 'postgresql+psycopg2://postgres:XXXXXX@localhost:5432/artikel_berita'
TABLE_NAME = 'data_artikel'

# --- Setup koneksi SQLAlchemy ---
engine = create_engine(DB_URI)

def is_id_exists(id_to_check):
    query = text(f"SELECT EXISTS(SELECT 1 FROM {TABLE_NAME} WHERE id=:id)")
    with engine.connect() as conn:
        result = conn.execute(query, {"id": id_to_check})
        return result.scalar()

def scrape_data(start_page=1, pages=1):
    logger.info('Tunggu...')
    artikel = []

    # setup session dengan retry
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    for page in range(start_page, start_page + pages):
        try:
            url = f'https://www.detik.com/search/searchnews?query=Kriminal&page={page}&result_type=latest'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            content = soup.find('div', class_='list-content')
            if not content:
                continue
            articles = content.find_all('article')

            for idx, article in enumerate(articles, start=1):
                link = article.find('a')['href']
                id_hash = hashlib.md5(link.encode()).hexdigest()

                # cek apakah id sudah ada di DB
                if is_id_exists(id_hash):
                    logger.info(f"Artikel sudah ada di DB, skip: {link}")
                    continue

                title = article.find('h3').text.strip() if article.find('h3') else 'No title'
                datetime_tag = article.find(class_='media__date')
                datetime = datetime_tag.find('span')['title'] if datetime_tag and datetime_tag.find('span') else 'No datetime'

                time.sleep(random.uniform(1, 3))

                # detail content
                response_content = session.get(link, headers=headers, timeout=10)
                response_content.raise_for_status()
                soup_content = BeautifulSoup(response_content.text, 'html.parser')
                content_detail = soup_content.find('div', class_='detail__body-text itp_bodycontent')

                if content_detail:
                    paragraphs = content_detail.find_all('p')
                    content_text = ' '.join([p.text.strip() for p in paragraphs])
                    content_text = content_text.replace('ADVERTISEMENT','').replace('SCROLL TO CONTINUE WITH CONTENT','')
                    content_text = ' '.join(content_text.split())

                    strong_tag = content_detail.find('strong')
                    location_text = strong_tag.text.strip() if strong_tag else 'No location'
                else:
                    content_text = 'No detailed content'
                    location_text = 'No location'

                # add ke list
                artikel.append([id_hash, link, title, datetime, location_text, content_text])
                logger.info(f"Halaman {page} - Artikel {idx}/{len(articles)} berhasil di-scrape")

            time.sleep(random.uniform(2, 5))

        except Exception as e_page:
            logger.error(f"Error scraping page {page}: {e_page}")
            logger.error(traceback.format_exc())

    # save
    os.makedirs('../transform', exist_ok=True)
    output_file = '../transform/data_baru.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id','link','title','datetime','location','content'])
        writer.writerows(artikel)

    logger.info(f'Scraping Selesai. Di save ke {output_file}')

# main function
try:
    scrape_data(start_page=400, pages=100)
except Exception as e:
    logger.exception("Terjadi error saat Extract")

