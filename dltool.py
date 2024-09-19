import multiprocessing
import os
import re
import math
import signal
import argparse
import datetime
import platform
import requests
import textwrap
import xml.etree.ElementTree as ET
from multiprocessing import Pool, Value
from bs4 import BeautifulSoup
from progressbar import MultiBar, Bar, ETA, FileTransferSpeed, Percentage, DataSize

#Define constants
#Myrient HTTP-server addresses
MYRIENTHTTPADDR = 'https://myrient.erista.me/files/'
#Catalog URLs, to parse out the catalog in use from DAT
CATALOGURLS = {
    'https://www.no-intro.org': 'No-Intro',
    'http://redump.org/': 'Redump'
}
#Postfixes in DATs to strip away
DATPOSTFIXES = [
    ' (Retool)'
]
#Chunk sizes to download
CHUNKSIZE = 8192
#Headers to use in HTTP-requests
REQHEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
}

#Print output function
def logger(str, color=None, rewrite=False):
    colors = {'red': '\033[91m', 'green': '\033[92m', 'yellow': '\033[93m', 'cyan': '\033[96m'}
    if rewrite:
        print('\033[1A', end='\x1b[2K')
    if color:
        print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {colors[color]}{str}\033[00m')
    else:
        print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {str}')

#Input request function
def inputter(str, color=None):
    colors = {'red': '\033[91m', 'green': '\033[92m', 'yellow': '\033[93m', 'cyan': '\033[96m'}
    if color:
        val = input(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {colors[color]}{str}\033[00m')
    else:
        val = input(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {str}')
    return val

#Scale file size
def scale1024(val):
    prefixes=['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
    if val <= 0:
        power = 0
    else:
        power = min(int(math.log(val, 2) / 10), len(prefixes) - 1)
    scaled = float(val) / (2 ** (10 * power))
    unit = prefixes[power]
    return scaled, unit

#Exit handler function
def exithandler(signum, frame):
    logger('Exiting script!', 'red')
    exit()
signal.signal(signal.SIGINT, exithandler)

#Generate argument parser
parser = argparse.ArgumentParser(
    add_help=False,
    formatter_class=argparse.RawTextHelpFormatter,
    description=textwrap.dedent('''\
        \033[92mTool to automatically download ROMs of a DAT-file from Myrient.
        
        Generate a DAT-file with the tool of your choice to include ROMs that you
        want from a No-Intro/Redump/etc catalog, then use this tool to download
        the matching files from Myrient.\033[00m
    '''))

#Add required arguments
requiredargs = parser.add_argument_group('\033[91mRequired arguments\033[00m')
requiredargs.add_argument('-i', dest='inp', metavar='nointro.dat', help='Input DAT-file containing wanted ROMs', required=True)
requiredargs.add_argument('-o', dest='out', metavar='/data/roms', help='Output path for ROM files to be downloaded', required=True)
#Add optional arguments
optionalargs = parser.add_argument_group('\033[96mOptional arguments\033[00m')
optionalargs.add_argument('-c', dest='catalog', action='store_true', help='Choose catalog manually, even if automatically found')
optionalargs.add_argument('-s', dest='system', action='store_true', help='Choose system collection manually, even if automatically found')
optionalargs.add_argument('-l', dest='list', action='store_true', help='List only ROMs that are not found in server (if any)')
optionalargs.add_argument('-h', '--help', dest='help', action='help', help='Show this help message')
optionalargs.add_argument('-t', '--threads', dest='threads', default=multiprocessing.cpu_count(), help='Thread count', type=int)
args = parser.parse_args()

#Init variables
catalog = None
collection = None
wantedroms = []
wantedfiles = []
missingroms = []
collectiondir = []
availableroms = {}
foundcollections = []

#Validate arguments
if not os.path.isfile(args.inp):
    logger('Invalid input DAT-file!', 'red')
    exit()
if not os.path.isdir(args.out):
    logger('Invalid output ROM path!', 'red')
    exit()
if platform.system() == 'Linux' and args.out[-1] == '/':
    args.out = args.out[:-1]
elif platform.system() == 'Windows' and args.out[-1] == '\\':
    args.out = args.out[:-1]

#Open input DAT-file
logger('Opening input DAT-file...', 'green')
datxml = ET.parse(args.inp)
datroot = datxml.getroot()

#Loop through ROMs in input DAT-file
for datchild in datroot:
    #Print out system information
    if datchild.tag == 'header':
        system = datchild.find('name').text
        for fix in DATPOSTFIXES:
            system = system.replace(fix, '')
        catalogurl = datchild.find('url').text
        if catalogurl in CATALOGURLS:
            catalog = CATALOGURLS[catalogurl]
            logger(f'Processing {catalog}: {system}...', 'green')
        else:
            logger(f'Processing {system}...', 'green')
    #Add found ROMs to wanted list
    elif datchild.tag == 'game':
        rom = datchild.find('rom')
        filename = rom.attrib['name']
        filename = re.sub(r'\.[(a-zA-Z0-9)]{1,3}\Z', '', filename)
        if filename not in wantedroms:
            wantedroms.append(filename)

#Get HTTP base and select wanted catalog
catalogurl = None
resp = requests.get(MYRIENTHTTPADDR, headers=REQHEADERS).text
resp = BeautifulSoup(resp, 'html.parser')
maindir = resp.find('table', id='list').tbody.find_all('tr')
for dir in maindir[1:]:
    cell = dir.find('td')
    if catalog in cell.a['title']:
        catalogurl = cell.a['href']

if not catalogurl or args.catalog:
    logger('Catalog for DAT not automatically found, please select from the following:', 'yellow')
    dirnbr = 1
    catalogtemp = {}
    for dir in maindir[1:]:
        cell = dir.find('td')
        logger(f'{str(dirnbr).ljust(2)}: {cell.a["title"]}', 'yellow')
        catalogtemp[dirnbr] = {'name': cell.a['title'], 'url': cell.a['href']}
        dirnbr += 1
    while True:
        sel = inputter('Input selected catalog number: ', 'cyan')
        try:
            sel = int(sel)
            if sel > 0 and sel < dirnbr:
                catalog = catalogtemp[sel]['name']
                catalogurl = catalogtemp[sel]['url']
                break
            else:
                logger('Input number out of range!', 'red')
        except:
            logger('Invalid number!', 'red')

#Get catalog directory and select wanted collection
collectionurl = None
resp = requests.get(f'{MYRIENTHTTPADDR}{catalogurl}', headers=REQHEADERS).text
resp = BeautifulSoup(resp, 'html.parser')
contentdir = resp.find('table', id='list').tbody.find_all('tr')
for dir in contentdir[1:]:
    cell = dir.find('td')
    if cell.a['title'].startswith(system):
        foundcollections.append({'name': cell.a['title'], 'url': cell.a['href']})
if len(foundcollections) == 1:
    collection = foundcollections[0]['name']
    collectionurl = foundcollections[0]['url']
if not collection or args.system:
    logger('Collection for DAT not automatically found, please select from the following:', 'yellow')
    dirnbr = 1
    if len(foundcollections) > 1 and not args.system:
        for foundcollection in foundcollections:
            logger(f'{str(dirnbr).ljust(2)}: {foundcollection["name"]}', 'yellow')
            dirnbr += 1
    else:
        collectiontemp = {}
        for dir in contentdir[1:]:
            cell = dir.find('td')
            logger(f'{str(dirnbr).ljust(2)}: {cell.a["title"]}', 'yellow')
            collectiontemp[dirnbr] = {'name': cell.a['title'], 'url': cell.a['href']}
            dirnbr += 1
    while True:
        sel = inputter('Input selected collection number: ', 'cyan')
        try:
            sel = int(sel)
            if sel > 0 and sel < dirnbr:
                if len(foundcollections) > 1 and not args.system:
                    collection = foundcollections[sel-1]['name']
                    collectionurl = foundcollections[sel-1]['url']
                else:
                    collection = collectiontemp[sel]['name']
                    collectionurl = collectiontemp[sel]['url']
                break
            else:
                logger('Input number out of range!', 'red')
        except:
            logger('Invalid number!', 'red')
    
#Get collection directory contents and list contents to available ROMs
resp = requests.get(f'{MYRIENTHTTPADDR}{catalogurl}{collectionurl}', headers=REQHEADERS).text
resp = BeautifulSoup(resp, 'html.parser')
collectiondir = resp.find('table', id='list').tbody.find_all('tr')
for rom in collectiondir[1:]:
    cell = rom.find('a')
    filename = cell['title']
    romname = re.sub(r'\.[(a-zA-Z0-9)]{1,3}\Z', '', filename)
    url = f'{MYRIENTHTTPADDR}{catalogurl}{collectionurl}{cell["href"]}'
    availableroms[romname] = {'name': romname, 'file': filename, 'url': url}

#Compare wanted ROMs and contents of the collection, parsing out only wanted files
for wantedrom in wantedroms:
    if wantedrom in availableroms:
        wantedfiles.append(availableroms[wantedrom])
    else:
        missingroms.append(wantedrom)

#Print out information about wanted/found/missing ROMs
logger(f'Amount of wanted ROMs in DAT-file   : {len(wantedroms)}', 'green')
logger(f'Amount of found ROMs at server      : {len(wantedfiles)}', 'green')
if missingroms:
    logger(f'Amount of missing ROMs at server    : {len(missingroms)}', 'yellow')

dlcounter = Value('i', 1)
progressbars = MultiBar()

def file_download(wantedfile):
    global progressbars
    global dlcounter

    resumedl = False
    proceeddl = True

    counter = dlcounter.value
    with dlcounter.get_lock():
        dlcounter.value += 1

    if platform.system() == 'Linux':
        localpath = f'{args.out}/{wantedfile["file"]}'
    elif platform.system() == 'Windows':
        localpath = f'{args.out}\\{wantedfile["file"]}'

    resp = requests.get(wantedfile['url'], headers=REQHEADERS, stream=True)
    remotefilesize = int(resp.headers.get('content-length'))

    if os.path.isfile(localpath):
        localfilesize = int(os.path.getsize(localpath))
        if localfilesize != remotefilesize:
            resumedl = True
        else:
            proceeddl = False

    if proceeddl:
        file = open(localpath, 'ab')

        size, unit = scale1024(remotefilesize)
        pbar = progressbars[counter]
        pbar.widgets = ['\033[96m', Percentage(), ' | ', DataSize(), f' / {round(size, 1)} {unit}', ' ', Bar(marker='#'), ' ', ETA(), ' | ', FileTransferSpeed(), '\033[00m']
        pbar.redirect_stdout = True
        pbar.start(max_value=remotefilesize)

        if resumedl:
            logger(f'Resuming    {str(dlcounter).zfill(len(str(len(wantedfiles))))}/{len(wantedfiles)}: {wantedfile["name"]}', 'cyan')
            pbar.increment(localfilesize)
            headers = REQHEADERS
            headers.update({'Range': f'bytes={localfilesize}-'})
            resp = requests.get(wantedfile['url'], headers=headers, stream=True)
            for data in resp.iter_content(chunk_size=CHUNKSIZE):
                file.write(data)
                pbar.increment(len(data))
        else:
            logger(f'Downloading {str(counter).zfill(len(str(len(wantedfiles))))}/{len(wantedfiles)}: {wantedfile["name"]}', 'cyan')
            for data in resp.iter_content(chunk_size=CHUNKSIZE):
                file.write(data)
                pbar.increment(len(data))

        file.close()
        pbar.finish()
        print('\033[1A', end='\x1b[2K')
        logger(f'Downloaded  {str(counter).zfill(len(str(len(wantedfiles))))}/{len(wantedfiles)}: {wantedfile["name"]}', 'green', True)
    else:
        logger(f'Already DLd {str(counter).zfill(len(str(len(wantedfiles))))}/{len(wantedfiles)}: {wantedfile["name"]}', 'green')

pool = multiprocessing.Pool(args.threads)

#Download wanted files
if not args.list:
    pool.map(file_download, wantedfiles)
    pool.close()
    pool.join()
    logger('Downloading complete!', 'green', False)

#Output missing ROMs, if any
if missingroms:
    logger(f'Following {len(missingroms)} ROMs in DAT not automatically found from server, grab these manually:', 'red')
    for missingrom in missingroms:
        logger(missingrom, 'yellow')
else:
    logger('All ROMs in DAT found from server!', 'green')