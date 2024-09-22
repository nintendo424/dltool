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
from tenacity import retry, after_log, before_log
from tqdm.asyncio import tqdm
import aiofiles

# Define constants
# Myrient HTTP-server addresses
MYRIENTHTTPADDR = 'https://myrient.erista.me/files/'
# Catalog URLs, to parse out the catalog in use from DAT
CATALOGURLS = {
    'https://www.no-intro.org': 'No-Intro',
    'http://redump.org/': 'Redump'
}
# Postfixes in DATs to strip away
DATPOSTFIXES = [
    ' (Retool)'
]
# Headers to use in HTTP-requests
REQHEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
}

async def main():

    # Exit handler function
    def exithandler(signum, frame):
        logger.info('Exiting script!')
        exit()
    signal.signal(signal.SIGINT, exithandler)
    signal.signal(signal.SIGTERM, exithandler)

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
    requiredargs = parser.add_argument_group('\033[91mRequired arguments\033[00m')
    requiredargs.add_argument('-i', dest='inp', metavar='nointro.dat', help='Input DAT-file containing wanted ROMs', required=True)
    requiredargs.add_argument('-o', dest='out', metavar='/data/roms', help='Output path for ROM files to be downloaded', required=True)

    # Add optional arguments
    optionalargs = parser.add_argument_group('\033[96mOptional arguments\033[00m')
    optionalargs.add_argument('-c', dest='catalog', action='store_true', help='Choose catalog manually, even if automatically found')
    optionalargs.add_argument('-s', dest='system', action='store_true', help='Choose system collection manually, even if automatically found')
    optionalargs.add_argument('-l', dest='list', action='store_true', help='List only ROMs that are not found in server (if any)')
    optionalargs.add_argument('-d', dest='enabledebug', action='store_true', help='Enable debug logs to a file')
    optionalargs.add_argument('-h', '--help', dest='help', action='help', help='Show this help message')
    optionalargs.add_argument('-t', '--task-count', dest='taskcount', action='store', default=multiprocessing.cpu_count(), help='Number of simultaneous tasks', type=int)
    optionalargs.add_argument('--chunk-size', dest='chunksize', action='store', help='Chunk size in bytes', type=int)
    optionalargs.add_argument('-f', '--filter', dest='filter', action='store', help='Filter ROMs to download', default=None, type=str)
    optionalargs.add_argument('--log', default='warning',
        choices=['debug', 'info', 'warning', 'error'],
        help="logging level (defaults to 'warning')")

    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, args.log.upper()))
    formatter = logging.Formatter(
        "{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M"
    )
    console_handler.setFormatter(formatter)

    if args.enabledebug:
        file_handler = logging.FileHandler("debug.log", mode="w")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.addHandler(console_handler)

    # Init variables
    catalog = None
    collection = None
    wantedroms = []
    wantedfiles = []
    missingroms = []
    collectiondir = []
    availableroms = {}
    foundcollections = []

    # Validate arguments
    if not os.path.isfile(args.inp):
        logger.info('Invalid input DAT-file!')
        exit()
    if not os.path.isdir(args.out):
        logger.info('Invalid output ROM path!')
        exit()
    if platform.system() == 'Linux' and args.out[-1] == '/':
        args.out = args.out[:-1]
    elif platform.system() == 'Windows' and args.out[-1] == '\\':
        args.out = args.out[:-1]

    # Open input DAT-file
    logger.info('Opening input DAT-file...')
    datxml = ElementTree.parse(args.inp)
    datroot = datxml.getroot()

    transport = httpx.AsyncHTTPTransport(http2=True, retries=10)
    async with httpx.AsyncClient(follow_redirects=True, http2=True, headers=REQHEADERS, timeout=httpx.Timeout(30), transport=transport) as client:

        # Loop through ROMs in input DAT-file
        for datchild in datroot:
            # Print out system information
            if datchild.tag == 'header':
                system = datchild.find('name').text
                for fix in DATPOSTFIXES:
                    system = system.replace(fix, '')
                catalogurl = datchild.find('url').text
                if catalogurl in CATALOGURLS:
                    catalog = CATALOGURLS[catalogurl]
                    logger.info(f'Processing {catalog}: {system}...')
                else:
                    logger.info(f'Processing {system}...')
            # Add found ROMs to wanted list
            elif datchild.tag == 'game':
                rom = datchild.find('rom')
                filename = rom.attrib['name']
                filename = re.sub(r'\.[(a-zA-Z0-9)]{1,3}\Z', '', filename)
                if filename not in wantedroms:
                    wantedroms.append(filename)

        # Get HTTP base and select wanted catalog
        catalogurl = None
        resp = (await client.get(MYRIENTHTTPADDR)).text
        resp = BeautifulSoup(resp, 'html.parser')
        maindir = resp.find('table', id='list').tbody.find_all('tr')
        for directory in maindir[1:]:
            cell = directory.find('td')
            if catalog in cell.a['title']:
                catalogurl = cell.a['href']

        if not catalogurl or args.catalog:
            logger.info('Catalog for DAT not automatically found, please select from the following:')
            dirnbr = 1
            catalogtemp = {}
            for directory in maindir[1:]:
                cell = directory.find('td')
                logger.info(f'{str(dirnbr).ljust(2)}: {cell.a["title"]}')
                catalogtemp[dirnbr] = {'name': cell.a['title'], 'url': cell.a['href']}
                dirnbr += 1
            while True:
                sel = input('Input selected catalog number: ')
                try:
                    sel = int(sel)
                    if 0 < sel < dirnbr:
                        catalog = catalogtemp[sel]['name']
                        catalogurl = catalogtemp[sel]['url']
                        break
                    else:
                        logger.info('Input number out of range!')
                except ValueError:
                    logger.info('Invalid number!')

        # Get catalog directory and select wanted collection
        collectionurl = None
        resp = (await client.get(f'{MYRIENTHTTPADDR}{catalogurl}')).text
        resp = BeautifulSoup(resp, 'html.parser')
        contentdir = resp.find('table', id='list').tbody.find_all('tr')
        for directory in contentdir[1:]:
            cell = directory.find('td')
            if cell.a['title'].startswith(system):
                foundcollections.append({'name': cell.a['title'], 'url': cell.a['href']})
        if len(foundcollections) == 1:
            collection = foundcollections[0]['name']
            collectionurl = foundcollections[0]['url']
        if not collection or args.system:
            logger.info('Collection for DAT not automatically found, please select from the following:')
            dirnbr = 1
            if len(foundcollections) > 1 and not args.system:
                for foundcollection in foundcollections:
                    logger.info(f'{str(dirnbr).ljust(2)}: {foundcollection["name"]}')
                    dirnbr += 1
            else:
                collectiontemp = {}
                for directory in contentdir[1:]:
                    cell = directory.find('td')
                    logger.info(f'{str(dirnbr).ljust(2)}: {cell.a["title"]}')
                    collectiontemp[dirnbr] = {'name': cell.a['title'], 'url': cell.a['href']}
                    dirnbr += 1
            while True:
                sel = input('Input selected collection number: ')
                try:
                    sel = int(sel)
                    if 0 < sel < dirnbr:
                        if len(foundcollections) > 1 and not args.system:
                            collection = foundcollections[sel-1]['name']
                            collectionurl = foundcollections[sel-1]['url']
                        else:
                            collection = collectiontemp[sel]['name']
                            collectionurl = collectiontemp[sel]['url']
                        break
                    else:
                        logger.info('Input number out of range!')
                except ValueError:
                    logger.info('Invalid number!')

        # Get collection directory contents and list contents to available ROMs
        resp = (await client.get(f'{MYRIENTHTTPADDR}{catalogurl}{collectionurl}')).text
        resp = BeautifulSoup(resp, 'html.parser')
        collectiondir = resp.find('table', id='list').tbody.find_all('tr')
        for rom in collectiondir[1:]:
            cell = rom.find('a')
            filename = cell['title']
            romname = re.sub(r'\.[(a-zA-Z0-9)]{1,3}\Z', '', filename)
            url = f'{MYRIENTHTTPADDR}{catalogurl}{collectionurl}{cell["href"]}'
            availableroms[romname] = {'name': romname, 'file': filename, 'url': url}

        if args.filter:
            wantedroms = [rom for rom in wantedroms if args.filter.lower() in rom.lower()]

        # Compare wanted ROMs and contents of the collection, parsing out only wanted files
        for wantedrom in wantedroms:
            if wantedrom in availableroms:
                wantedfiles.append(availableroms[wantedrom])
            else:
                missingroms.append(wantedrom)

        # Print out information about wanted/found/missing ROMs
        logger.info(f'Amount of wanted ROMs in DAT-file   : {len(wantedroms)}')
        logger.info(f'Amount of found ROMs at server      : {len(wantedfiles)}')
        if missingroms:
            logger.info(f'Amount of missing ROMs at server    : {len(missingroms)}')

        @retry
        async def file_download(sem, wantedfile):
            localpath = os.path.join(args.out, wantedfile["file"])
            localsize = os.path.getsize(localpath) if os.path.isfile(localpath) else 0

            logger.debug(f'Preparing to fetch {wantedfile["name"]}')

            async with sem:
                filesizeresponse = await client.head(wantedfile['url'])
                if not filesizeresponse.is_error:
                    remotefilesize = int(filesizeresponse.headers['content-length'])
                else:
                    logger.debug('Error getting filesize')
                    raise Exception()

                logger.debug(f'{wantedfile["name"]} sizes: local: {localsize}, remote: {remotefilesize}')
                if localsize != remotefilesize:
                    headers = REQHEADERS
                    headers['Range'] = f'bytes={localsize}-'
                    async with client.stream('GET', wantedfile['url'], headers=headers) as filestream:
                        if not filestream.is_error:
                            logger.debug(f'Need to download {wantedfile["name"]}, {'downloading' if localsize == 0 else 'resuming'}')

                            async with aiofiles.open(localpath, 'wb' if localsize == 0 else 'ab') as file:
                                with tqdm(desc=wantedfile['file'], total=remotefilesize, initial=localsize, unit='B', unit_scale=True, leave=False) as pbar:
                                    async for chunk in filestream.aiter_bytes(args.chunksize):
                                        pbar.update(len(chunk))
                                        await file.write(chunk)

                            if os.path.getsize(localpath) != remotefilesize:
                                os.remove(localpath)
                                logger.error(f'Wrong file size after downloading {wantedfile["name"]}, will redownload')
                                raise Exception()
                            logger.debug(f'Successfully downloaded {wantedfile["name"]}')
                        else:
                            logger.error(f'Error downloading {wantedfile["name"]}, will redownload')
                            raise Exception()
                else:
                    logger.debug(f'Already have {wantedfile["name"]}, do not need to download')

        # Download wanted files
        if not args.list:
            try:
                semaphore = asyncio.Semaphore(args.taskcount)
                await tqdm.gather(*[file_download(semaphore, file) for file in wantedfiles], desc='ROM Fetch Progress')
                logger.info('Downloading complete!')
            except asyncio.CancelledError:
                logger.error('Download cancelled!')

    # Output missing ROMs, if any
    if missingroms:
        logger.info(f'Following {len(missingroms)} ROMs in DAT not automatically found from server, grab these manually:')
        for missingrom in missingroms:
            logger.info(missingrom)
    else:
        logger.info('All ROMs in DAT found from server!')

asyncio.run(main())
