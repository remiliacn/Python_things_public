from os import getcwd, mkdir, walk, remove
from os.path import exists, abspath, join
from random import uniform
from re import match
from sqlite3 import connect
from time import sleep
from traceback import print_exc
from zipfile import ZipFile

import requests
from loguru import logger
from pathvalidate import sanitize_filename
from pixivpy3 import AppPixivAPI
from win10toast import ToastNotifier

from config import REFRESH_TOKEN

USER_ID = 5657723

MAX_ITER_COUNT = 50

img_db = connect('./pixiv_id_db.db')


def _get_root_path():
    supposed_path = f'{getcwd()}/data/pixivPic/{USER_ID}'
    if not exists(supposed_path):
        mkdir(supposed_path)

    return supposed_path


ROOT_PATH = _get_root_path()


def save_img_id(img_id, img_type, illuster):
    img_id = str(img_id)
    logger.debug(f'Saving image id {img_id}')
    img_db.execute(
        """
        insert or replace into downloaded_by values (?, ?, ?)
        """, (img_id, img_type, illuster)
    )
    img_db.commit()


def check_database_already_downloaded(img_id):
    img_id = str(img_id)
    info = img_db.execute(
        """
        select illust_id from downloaded_by where illust_id = ?
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
        file_name = image_url.split('/')[-1]

        path = f'{ROOT_PATH}/{file_name}'.replace('\\\\', '/')
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


def download_gif(ugoira_url, title, result_id):
    if ugoira_url is None:
        logger.warning('Zip ugoira is None!!!')
        return

    title = sanitize_filename(title)

    file_name = ugoira_url.split('/')[-1]
    gif_zip_path = f'{ROOT_PATH}/{file_name}'.replace('\\\\', '/')

    download_gif_processor(gif_zip_path, ugoira_url, title, result_id)


def download_gif_processor(gif_zip_path, ugoira_url, title, result_id):
    if not exists(gif_zip_path):
        path = download(ugoira_url, gif_zip_path, title)
        gif_zip_path_path = gif_zip_path.split('.')[0]
        with ZipFile(path, 'r') as zipref:
            if not exists(gif_zip_path_path):
                mkdir(gif_zip_path_path)
                for idx, zipinfo in enumerate(zipref.infolist()):
                    zipinfo.filename = f'{result_id}_p{idx}.jpg'
                    zipref.extract(zipinfo, path=gif_zip_path_path)

        remove(gif_zip_path)


def _init_database():
    img_db.execute(
        """
        create table if not exists downloaded_by (
            illust_id varchar(20) unique on conflict ignore,
            type varchar(20),
            illustrator varchar(100)
        )
        """
    )


def main():
    iter_count = 0
    api = AppPixivAPI()
    api.auth(refresh_token=REFRESH_TOKEN)

    json_result = api.user_illusts(user_id=USER_ID)
    # json_result = api.user_bookmarks_illust(user_id=USER_ID, req_auth=True, filter=None)
    next_query = {}
    while next_query is not None and MAX_ITER_COUNT > iter_count:
        logger.info(
            f'Fetching data: {next_query["offset"] if "offset" in next_query else "current_page"}')
        for result in json_result.illusts:
            illust_data = []
            title = result.title
            if check_database_already_downloaded(result.id):
                logger.debug(f'Database check is already downloaded {result.id}')
                continue

            if result.type != 'ugoira':
                if result.meta_pages:
                    illust_data = result.meta_pages
                else:
                    illust_data.append(result.meta_single_page)

                if len(illust_data) > 50:
                    illust_data = illust_data[:50]
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
                        if 'original_image_url' in illust:
                            image_url = illust['original_image_url']
                        else:
                            continue

                    download_image(image_url, title + '_' + image_url.split('_')[-1])
            else:
                ugoira_data = api.ugoira_metadata(result.id)
                url_list = ugoira_data.ugoira_metadata.zip_urls.medium
                download_gif(url_list, title, result.id)

            save_img_id(result.id, result.type, result.user.name)

        next_query = api.parse_qs(json_result.next_url)
        if next_query:
            json_result = api.user_illusts(**next_query)
            rand_sleep_time = uniform(2.0, 4.0)
            logger.info(f'Sleeping for {rand_sleep_time:.1f}s')
            sleep(rand_sleep_time)
            iter_count += 1
        else:
            break


def wrapping_up(text: str):
    toast = ToastNotifier()
    toast.show_toast("PixivDownloadBy APP", text, duration=1)


if __name__ == '__main__':
    _init_database()
    try:
        main()
        wrapping_up('Download done with no issue!!')
    except Exception as err:
        wrapping_up(f'Something happened, see error for details! {err.__traceback__}')
        print_exc()
        exit(-1)
