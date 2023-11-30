from os import mkdir
from os.path import exists, isdir
from sys import stderr
from typing import Union

from cfscrape import create_scraper
from httpx import Client
from loguru import logger
from pathvalidate import sanitize_filename
from requests import exceptions
from urllib3.exceptions import ProtocolError

from config import FANBOX_CONFIG
from pixiv_download_bookmark import img_db

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{function}()</cyan>::<cyan>L{line}</cyan> | "
    "<level>{message}</level>"
)
logger.remove()
logger.add(stderr, format=logger_format)

WHITE_LIST_DRAWER = FANBOX_CONFIG['WHITE_LIST_DRAWER']
BLACKLIST_WORDS = FANBOX_CONFIG['BLACKLIST_WORDS']
LIMIT = FANBOX_CONFIG['LIMIT_TO_FETCH']
DATE = FANBOX_CONFIG['MAX_DATE']

CREATOR = FANBOX_CONFIG['PIXIV_CREATOR_CONFIG']

HEADERS = {
    'cookie': FANBOX_CONFIG['SESSION_ID'],
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/92.0.4515.159 Safari/537.36',
    "authority": 'api.fanbox.cc',
    "path": f'/post.listCreator?creatorId={CREATOR}&maxPublishedDatetime={DATE}%2004%3A16%3A39'
            f'&maxId={"9" * 12}&limit={LIMIT}',
    "accept": 'application/json, text/plain, */*',
    "origin": 'https://www.fanbox.cc',
    "referer": 'https://www.fanbox.cc/'
}


def _init_database():
    img_db.execute("""
        create table if not exists fanbox (
            post_id varchar(50) unique on conflict ignore,
            creator_name varchar(200),
            title varchar(255),
            no_access boolean
        )
    """)
    img_db.commit()


def _check_if_post_id_exists(post_id: Union[int, str]):
    post_id = str(post_id)
    result = img_db.execute(
        """
        select * from fanbox where post_id = ?
        """, (post_id,)
    ).fetchone()

    return result is not None


def _insert_to_db(post_id: str, creator_name: str, title: str, no_access=False):
    img_db.execute(
        """
        insert or replace into fanbox values (?, ?, ?, ?)
        """, (post_id, creator_name, title, no_access)
    )
    img_db.commit()


def _file_name_from_url(file_name: str):
    return sanitize_filename(file_name)


def image_download(scraper, original_image_url, file_name):
    try:
        page = scraper.get(original_image_url, headers=HEADERS, stream=True)
    except ConnectionError or exceptions.ConnectionError:
        logger.warning('Connection aborted, retrying...')
        image_download(scraper, original_image_url, file_name)
        return

    data_downloaded = 0
    with open(f'./data/image/{CREATOR}/{file_name}', 'wb+') as file:
        if 'Content-Length' in page.headers:
            total = int(page.headers["Content-Length"])
        else:
            total = -1
        for chunk in page.iter_content(chunk_size=1024 ** 2):
            if total > 0:
                data_downloaded += len(chunk)
                print(
                    f'Downloaded "{file_name}": {data_downloaded / total * 100:.2f}%',
                    flush=True,
                    end='\r' if data_downloaded < total else '\n'
                )
            if chunk:
                file.write(chunk)

    logger.success(f'Downloading of image {file_name} completed.')


def _download_fanbox_files(scraper, data_body):
    data_body = data_body['files'] if 'files' in data_body else data_body['fileMap']
    for count, file in enumerate(data_body):
        if isinstance(file, str):
            file = data_body[file]
        title = file['name']
        original_image_url = file['url']
        file_name = title + '.' + original_image_url.split('.')[-1]

        file_name = _file_name_from_url(file_name)

        if exists(f'data/image/{CREATOR}/{file_name}'):
            logger.info(f'File: {file_name} exists. Skipping...')
            continue

        image_download(scraper, original_image_url, file_name)


def pixivfanbox_crawler():
    _init_database()
    session = Client()
    session.headers = HEADERS
    scraper = create_scraper(sess=session)
    page = scraper.get(
        f'https://api.fanbox.cc/post.listCreator?creatorId={CREATOR}&maxPublishedDatetime='
        f'{DATE}%2005%3A13%3A17&maxId=2514720&limit={LIMIT}', headers=HEADERS
    )

    json_data = page.json()
    json_data = json_data['body']['items']
    logger.debug(f'data count: {len(json_data)}')

    if not isdir(f'data/image'):
        mkdir('data/image')

    if not isdir(f'data/image/{CREATOR}'):
        mkdir(f'data/image/{CREATOR}')

    for data in json_data:
        post_id = data['id']
        if _check_if_post_id_exists(post_id):
            logger.info(f'post id: {post_id} already exists, skipping the fetch.')
            continue

        try:
            page = scraper.get(f'https://api.fanbox.cc/post.info?postId={post_id}', headers=HEADERS).json()
        except ProtocolError or ConnectionError:
            logger.error('Protocol error encountered, retrying the whole process now.')
            pixivfanbox_crawler()
            return

        data_body = page['body']

        title = data["title"] if "title" in data else "?"
        for word in BLACKLIST_WORDS:
            title = title.replace(word, '').strip()

        if data_body is None:
            _insert_to_db(post_id, CREATOR, title)
            continue

        logger.info(f'title: {title}')
        logger.info(f'Link: https://www.fanbox.cc/@{CREATOR}/posts/{data["id"]}')
        logger.info(f'Published time: {data["publishedDatetime"]}')

        data_body = data_body['body']

        if data_body is None:
            _insert_to_db(post_id, CREATOR, title, True)
            continue

        if ('images' not in data_body or not data_body['images']) \
                and ('imageMap' not in data_body or not data_body['imageMap']):
            if 'files' in data_body or 'fileMap' in data_body:
                _download_fanbox_files(scraper, data_body)

            _insert_to_db(post_id, CREATOR, title)
            continue

        if 'files' in data_body or 'fileMap' in data_body:
            _download_fanbox_files(scraper, data_body)

        if 'images' not in data_body:
            images_enable = False
        else:
            images_enable = True

        images = data_body['images'] if 'images' in data_body else data_body['imageMap']
        count = 0
        for image in images:
            if not images_enable:
                image = images[image]

            original_image = image['originalUrl']
            original_image_url = original_image.replace('\\', '')

            count += 1
            if CREATOR not in WHITE_LIST_DRAWER:
                file_name = title + f'{count:04d}' + '.' + original_image_url.split('.')[-1]
            else:
                file_name = title + data['id'] + f'{count:04d}' + '.' + original_image_url.split('.')[-1]

            file_name = _file_name_from_url(file_name)

            if exists(f'data/image/{CREATOR}/{file_name}'):
                logger.info(f'File: {file_name} exists. Skipping...')
                continue

            image_download(scraper, original_image_url, file_name)

        _insert_to_db(post_id, CREATOR, title)

    logger.success('All tasks completed without problem.')


if __name__ == '__main__':
    pixivfanbox_crawler()
