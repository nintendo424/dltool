import asyncio
import logging
import multiprocessing
import os
import re
import signal
import argparse
import platform
import textwrap
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, before_sleep_log
from tqdm.asyncio import tqdm
import aiofiles

# Define constants
# Myrient HTTP-server addresses
MYRIENT_HTTP_ADDR = 'https://myrient.erista.me/files/'
# Catalog URLs, to parse out the catalog in use from DAT
CATALOG_URLS = {
    'https://www.no-intro.org': 'No-Intro',
    'http://redump.org/': 'Redump'
}
# Postfixes in DATs to strip away
DAT_POSTFIXES = [
    ' (Retool)'
]
# Headers to use in HTTP-requests
REQ_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
}

async def main():

    # Exit handler function
    def exit_handler(signum, frame):
        logger.info('Exiting script!')
        exit()
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    # Generate argument parser
    parser = argparse.ArgumentParser(
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent('''\
            \033[92mTool to automatically download ROMs of a DAT-file from Myrient.

            Generate a DAT-file with the tool of your choice to include ROMs that you
            want from a No-Intro/Redump/etc catalog, then use this tool to download
            the matching files from Myrient.\033[00m
        '''))

    # Add required arguments
    required_args = parser.add_argument_group('\033[91mRequired arguments\033[00m')
    required_args.add_argument('-i', dest='inp', metavar='nointro.dat', help='Input DAT-file containing wanted ROMs', required=True)
    required_args.add_argument('-o', dest='out', metavar='/data/roms', help='Output path for ROM files to be downloaded', required=True)

    # Add optional arguments
    optional_args = parser.add_argument_group('\033[96mOptional arguments\033[00m')
    optional_args.add_argument('-c', dest='catalog', action='store_true', help='Choose catalog manually, even if automatically found')
    optional_args.add_argument('-s', dest='system', action='store_true', help='Choose system collection manually, even if automatically found')
    optional_args.add_argument('-l', dest='list', action='store_true', help='List only ROMs that are not found in server (if any)')
    optional_args.add_argument('-d', dest='enabledebug', action='store_true', help='Enable debug logs to a file')
    optional_args.add_argument('-h', '--help', dest='help', action='help', help='Show this help message')
    optional_args.add_argument('-t', '--task-count', dest='taskcount', action='store', default=multiprocessing.cpu_count(), help='Number of simultaneous tasks', type=int)
    optional_args.add_argument('--chunk-size', dest='chunksize', action='store', help='Chunk size in bytes', type=int)
    optional_args.add_argument('-f', '--filter', dest='filter', action='store', help='Filter ROMs to download', default=None, type=str)
    optional_args.add_argument('--log', default='warning',
        choices=['debug', 'info', 'warning', 'error'],
        help='logging level (defaults to \'warning\')')

    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, args.log.upper()))
    formatter = logging.Formatter(
        '{asctime} - {levelname} - {message}',
        style='{',
        datefmt='%Y-%m-%d %H:%M'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if args.enabledebug:
        file_handler = logging.FileHandler('debug.log', mode='w')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Init variables
    catalog = None
    collection = None
    wanted_roms = []
    wanted_files = []
    missing_roms = []
    collection_dir = []
    available_roms = {}
    found_collections = []

    # Validate arguments
    if not os.path.isfile(args.inp):
        logger.error('Invalid input DAT-file!')
        exit()
    if not os.path.isdir(args.out):
        logger.error('Invalid output ROM path!')
        exit()
    if platform.system() == 'Linux' and args.out[-1] == '/':
        args.out = args.out[:-1]
    elif platform.system() == 'Windows' and args.out[-1] == '\\':
        args.out = args.out[:-1]

    # Open input DAT-file
    logger.info('Opening input DAT-file...')
    dat_xml = ElementTree.parse(args.inp)
    dat_root = dat_xml.getroot()

    transport = httpx.AsyncHTTPTransport(http2=True, retries=10)
    async with httpx.AsyncClient(follow_redirects=True, http2=True, headers=REQ_HEADERS, timeout=httpx.Timeout(30), transport=transport) as client:

        # Loop through ROMs in input DAT-file
        for dat_child in dat_root:
            # Print out system information
            if dat_child.tag == 'header':
                system = dat_child.find('name').text
                for fix in DAT_POSTFIXES:
                    system = system.replace(fix, '')
                catalog_url = dat_child.find('url').text
                if catalog_url in CATALOG_URLS:
                    catalog = CATALOG_URLS[catalog_url]
                    logger.info(f'Processing {catalog}: {system}...')
                else:
                    logger.info(f'Processing {system}...')
            # Add found ROMs to wanted list
            elif dat_child.tag == 'game':
                rom = dat_child.find('rom')
                file_name = rom.attrib['name']
                file_name = re.sub(r'\.[(a-zA-Z0-9)]{1,3}\Z', '', file_name)
                if file_name not in wanted_roms:
                    wanted_roms.append(file_name)

        # Get HTTP base and select wanted catalog
        catalog_url = None
        resp = (await client.get(MYRIENT_HTTP_ADDR)).text
        resp = BeautifulSoup(resp, 'html.parser')
        main_dir = resp.find('table', id='list').tbody.find_all('tr')
        for directory in main_dir[1:]:
            cell = directory.find('td')
            if catalog in cell.a['title']:
                catalog_url = cell.a['href']

        if not catalog_url or args.catalog:
            logger.warning('Catalog for DAT not automatically found, please select from the following:')
            dir_nbr = 1
            catalogtemp = {}
            for directory in main_dir[1:]:
                cell = directory.find('td')
                logger.warning(f'{str(dir_nbr).ljust(2)}: {cell.a['title']}')
                catalogtemp[dir_nbr] = {'name': cell.a['title'], 'url': cell.a['href']}
                dir_nbr += 1
            while True:
                sel = input('Input selected catalog number: ')
                try:
                    sel = int(sel)
                    if 0 < sel < dir_nbr:
                        catalog = catalogtemp[sel]['name']
                        catalog_url = catalogtemp[sel]['url']
                        break
                    else:
                        logger.error('Input number out of range!')
                except ValueError:
                    logger.error('Invalid number!')

        # Get catalog directory and select wanted collection
        collection_url = None
        resp = (await client.get(f'{MYRIENT_HTTP_ADDR}{catalog_url}')).text
        resp = BeautifulSoup(resp, 'html.parser')
        content_dir = resp.find('table', id='list').tbody.find_all('tr')
        for directory in content_dir[1:]:
            cell = directory.find('td')
            if cell.a['title'].startswith(system):
                found_collections.append({'name': cell.a['title'], 'url': cell.a['href']})
        if len(found_collections) == 1:
            collection = found_collections[0]['name']
            collection_url = found_collections[0]['url']
        if not collection or args.system:
            logger.warning('Collection for DAT not automatically found, please select from the following:')
            dir_nbr = 1
            if len(found_collections) > 1 and not args.system:
                for found_collection in found_collections:
                    logger.warning(f'{str(dir_nbr).ljust(2)}: {found_collection['name']}')
                    dir_nbr += 1
            else:
                collection_temp = {}
                for directory in content_dir[1:]:
                    cell = directory.find('td')
                    logger.info(f'{str(dir_nbr).ljust(2)}: {cell.a['title']}')
                    collection_temp[dir_nbr] = {'name': cell.a['title'], 'url': cell.a['href']}
                    dir_nbr += 1
            while True:
                sel = input('Input selected collection number: ')
                try:
                    sel = int(sel)
                    if 0 < sel < dir_nbr:
                        if len(found_collections) > 1 and not args.system:
                            collection = found_collections[sel-1]['name']
                            collection_url = found_collections[sel-1]['url']
                        else:
                            collection = collection_temp[sel]['name']
                            collection_url = collection_temp[sel]['url']
                        break
                    else:
                        logger.error('Input number out of range!')
                except ValueError:
                    logger.error('Invalid number!')

        # Get collection directory contents and list contents to available ROMs
        resp = (await client.get(f'{MYRIENT_HTTP_ADDR}{catalog_url}{collection_url}')).text
        resp = BeautifulSoup(resp, 'html.parser')
        collection_dir = resp.find('table', id='list').tbody.find_all('tr')
        for rom in collection_dir[1:]:
            cell = rom.find('a')
            file_name = cell['title']
            rom_name = re.sub(r'\.[(a-zA-Z0-9)]{1,3}\Z', '', file_name)
            url = f'{MYRIENT_HTTP_ADDR}{catalog_url}{collection_url}{cell['href']}'
            available_roms[rom_name] = {'name': rom_name, 'file': file_name, 'url': url}

        if args.filter:
            wanted_roms = [rom for rom in wanted_roms if args.filter.lower() in rom.lower()]

        # Compare wanted ROMs and contents of the collection, parsing out only wanted files
        for wanted_rom in wanted_roms:
            if wanted_rom in available_roms:
                wanted_files.append(available_roms[wanted_rom])
            else:
                missing_roms.append(wanted_rom)

        # Print out information about wanted/found/missing ROMs
        logger.info(f'Amount of wanted ROMs in DAT-file   : {len(wanted_roms)}')
        logger.info(f'Amount of found ROMs at server      : {len(wanted_files)}')

        downloaded_roms = []

        @retry(
            wait=wait_exponential(multiplier=1, min=1, max=8),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )
        async def file_download(sem, wanted_file):
            local_path = os.path.join(args.out, wanted_file['file'])
            local_size = os.path.getsize(local_path) if os.path.isfile(local_path) else 0

            logger.debug(f'Preparing to fetch {wanted_file['name']}')

            async with sem:
                file_size_response = await client.head(wanted_file['url'])
                if not file_size_response.is_error:
                    remote_file_size = int(file_size_response.headers['content-length'])
                else:
                    logger.error('Error getting filesize')
                    raise Exception()

                logger.debug(f'{wanted_file['name']} sizes: local: {local_size}, remote: {remote_file_size}')
                if local_size < remote_file_size:
                    headers = REQ_HEADERS
                    headers['Range'] = f'bytes={local_size}-'
                    async with client.stream('GET', wanted_file['url'], headers=headers) as filestream:
                        if not filestream.is_error:
                            logger.debug(f'Need to download {wanted_file['name']}, {'downloading' if local_size == 0 else 'resuming'}')

                            async with aiofiles.open(local_path, 'wb' if local_size == 0 else 'ab') as file:
                                with tqdm(desc=wanted_file['file'], total=remote_file_size, initial=local_size, unit='B', unit_scale=True, leave=False) as pbar:
                                    async for chunk in filestream.aiter_bytes(args.chunksize):
                                        pbar.update(len(chunk))
                                        await file.write(chunk)

                            if os.path.getsize(local_path) != remote_file_size:
                                os.remove(local_path)
                                logger.error(f'Wrong file size after downloading {wanted_file['name']}, will redownload')
                                raise Exception()
                        else:
                            logger.error(f'Error downloading {wanted_file['name']}, will redownload')
                            raise Exception()
                elif local_size > remote_file_size:
                    logger.error(f'{wanted_file['file']} local size larger than remote, need to redownload...')
                    os.remove(local_path)
                    raise Exception()

                logger.debug(f'Successfully downloaded {wanted_file['name']}')
                downloaded_roms.append(wanted_file)

        # Download wanted files
        if not args.list:
            try:
                semaphore = asyncio.Semaphore(args.taskcount)
                await tqdm.gather(*[file_download(semaphore, file) for file in wanted_files], desc='ROM Fetch Progress')
                logger.info('Downloading complete!')
            except asyncio.CancelledError:
                logger.error('Download cancelled!')

    # Output missing ROMs, if any
    if missing_roms:
        logger.info(f'Following {len(missing_roms)} ROMs in DAT not automatically found from server, grab these manually:')
        for missing_rom in missing_roms:
            logger.info(missing_rom)
    else:
        logger.info('All ROMs in DAT found from bserver!')

    not_downloaded = [rom for rom in wanted_files if rom not in downloaded_roms]
    if len(not_downloaded) > 0:
        logger.error(f'Couldn\'t download some roms, retry:')
        for rom in not_downloaded:
            logger.error(rom['name'])

if __name__ == '__main__':
    asyncio.run(main())
