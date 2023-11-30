from os import getcwd, mkdir, rmdir, remove, walk
from os.path import exists, abspath, join
from random import uniform
from re import match
from sqlite3 import connect
from time import sleep
from zipfile import ZipFile

import requests
from PIL import Image
from loguru import logger
from pathvalidate import sanitize_filename
from pixivpy3 import AppPixivAPI

from config import REFRESH_TOKEN

USER_ID = 13839440
ROOT_PATH = f'{getcwd()}/data/pixivPic'

img_db = connect('./pixiv_id_db.db')
MAX_ITER_COUNT = 10


def save_img_id(img_id, img_type, illuster):
    img_id = str(img_id)
    logger.debug(f'Saving image id {img_id}')
    img_db.execute(
        """
        insert or replace into downloaded_illusts values (?, ?, ?)
        """, (img_id, img_type, illuster)
    )
    img_db.commit()


def check_database_already_downloaded(img_id):
    img_id = str(img_id)
    info = img_db.execute(
        """
        select illust_id from downloaded_illusts where illust_id = ?
        """, (img_id,)
    ).fetchone()

    return info


def download(url, path, title):
    with requests.get(url, stream=True, headers={'Referer': 'https://app-api.pixiv.net/'}) as r:
        total_size = len(r.content) if r.headers.get('content-length') is None else int(r.headers.get('content-length'))
        written_size = 0
        r.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                written_size += len(chunk)
                f.write(chunk)
                print(f'Downloading {title}:  {written_size / total_size * 100:.1f}%',
                      end='\r' if written_size < total_size or written_size / total_size > 1.1 else '\n',
                      flush=True)

    logger.success(f'Download done {path}.')
    return path


def download_image(image_url, title):
    title = sanitize_filename(title)

    if image_url is None:
        logger.info('Image url is none somehow!!')
    else:
        path = f'{ROOT_PATH}/{title}'.replace('\\\\', '/')
        if not match(r'.*?\.[jpgnif]{3,4}$', path):
            path += '.jpg'
        if not exists(path):
            download(image_url, path, title)


def get_absolute_file_paths(directory):
    file_paths = []
    for folder, subs, files in walk(directory):
        for filename in files:
            file_paths.append(abspath(join(folder, filename)))

    return file_paths


def download_gif(ugoira_url, title, duration):
    if ugoira_url is None:
        logger.warning('Zip ugoira is None!!!')
        return

    title = sanitize_filename(title)

    file_name = ugoira_url.split('/')[-1]
    gif_zip_path = f'{ROOT_PATH}/{file_name}'.replace('\\\\', '/')
    gif_path = gif_zip_path.replace('zip', 'gif')

    download_gif_processor(gif_path, ugoira_url, gif_zip_path, title, duration)


def download_gif_processor(gif_path, ugoira_url, gif_zip_path, title, duration):
    if not exists(gif_path):
        path = download(ugoira_url, gif_zip_path, title)

        gif_zip_path_path = gif_zip_path
        gif_zip_path = gif_zip_path.split('.')[0]
        with ZipFile(path, 'r') as zipref:
            if not exists(gif_zip_path):
                mkdir(gif_zip_path)
            zipref.extractall(gif_zip_path)
            im = Image.open(get_absolute_file_paths(gif_zip_path)[0])
            logger.info('Making the gif...')
            im.save(f'{gif_path}',
                    save_all=True,
                    append_images=[Image.open(file) for file in get_absolute_file_paths(gif_zip_path)],
                    duration=duration,
                    loop=0)

        logger.info('Removing gif making single img cache...')
        remove(gif_zip_path_path)
        for file in get_absolute_file_paths(gif_zip_path):
            remove(file)

        logger.info('Removing zip cache.')
        rmdir(gif_zip_path)


def _init_database():
    img_db.execute(
        """
        create table if not exists downloaded_illusts (
            illust_id varchar(20) unique on conflict ignore,
            type varchar(20),
            illustrator varchar(100)
        )
        """
    )
    img_db.commit()


def main():
    _init_database()
    iter_count = 0
    api = AppPixivAPI()
    api.auth(refresh_token=REFRESH_TOKEN)

    json_result = api.user_bookmarks_illust(user_id=USER_ID, req_auth=True, filter=None)
    next_query = api.parse_qs(json_result.next_url)
    while next_query is not None and MAX_ITER_COUNT > iter_count:
        logger.info(f'Fetching data: {next_query["max_bookmark_id"]}')
        for result in json_result.illusts:
            illust_data = []
            title = result.title
            author = result.user.name
            if check_database_already_downloaded(result.id):
                logger.debug(f'Database check is already downloaded {result.id}')
                continue
            if result.type != 'ugoira':
                if result.meta_pages:
                    illust_data = result.meta_pages
                else:
                    illust_data.append(result.meta_single_page)

                for illust in illust_data:
                    image_url = None
                    if 'image_urls' in illust:
                        illust = illust['image_urls']
                        if 'original' in illust:
                            image_url = illust['original']
                        elif 'large' in illust:
                            image_url = illust['large']
                        elif 'medium' in illust:
                            image_url = illust['medium']
                        elif 'square_medium' in illust:
                            image_url = illust['square_medium']
                    else:
                        image_url = illust['original_image_url']

                    download_image(image_url, author + '_' + title + '_' + image_url.split('_')[-1])
            else:
                ugoira_data = api.ugoira_metadata(result.id)
                url_list = ugoira_data.ugoira_metadata.zip_urls.medium
                download_gif(
                    url_list,
                    author + '_' + title,
                    ugoira_data.ugoira_metadata.frames[0].delay)

            save_img_id(result.id, result.type, result.user.name)

        json_result = api.user_bookmarks_illust(**next_query)
        next_query = api.parse_qs(json_result.next_url)
        rand_sleep_time = uniform(2.0, 4.0)
        logger.info(f'Sleeping for {rand_sleep_time:.1f}s')
        sleep(rand_sleep_time)
        iter_count += 1


if __name__ == '__main__':
    main()
