#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Explanation:

This builds a specific lookup file, based on a CSV file and lookup file definitions.

Usage:
    $ python  sumologic-createlookup [ options ]

Style:
    Google Python Style Guide:
    http://google.github.io/styleguide/pyguide.html

    @name           sumologic-createlookup
    @version        3.0.0
    @author-name    Wayne Schmidt
    @author-email   wschmidt@sumologic.com
    @license-name   GNU GPL
    @license-url    http://www.gnu.org/licenses/gpl.html
"""

__version__ = '3.0.0'
__author__ = "Wayne Schmidt (wschmidt@sumologic.com)"

import argparse
import configparser
import datetime
import os
import sys
import json
import http
import glob
import requests
from filesplit.split import Split

sys.dont_write_bytecode = 1

PARSER = argparse.ArgumentParser(description="""
Tool to build a Sumo Logic lookup file
""")

PARSER.add_argument('-k', metavar='<apikey>', dest='APIKEY', \
                    help='specify API key')

PARSER.add_argument('-d', metavar='<cachedir>', dest='CACHED', \
                    help='specify cache directory')

PARSER.add_argument('-j', metavar='<lookupjson>', dest='LOOKUPJSON', \
                    required=True, help='specify lookup file definition')

PARSER.add_argument('-s', metavar='<lookupfile>', dest='LOOKUPFILE', \
                    required=True, help='specify lookup file source')

PARSER.add_argument('-c', metavar='<cfgfile>', dest='CONFIG', \
                    help='specify config file')

PARSER.add_argument("-v", type=int, default=0, metavar='<verbose>', \
                    dest='verbose', help="specify level of verbose output")

ARGS = PARSER.parse_args(args=None if sys.argv[1:] else ['--help'])

DEFAULTMAP = []
DEFAULTMAP.append('ip')
MAPLIST = DEFAULTMAP

FILE = {}
HTTP = {}
SUMO = {}

CURRENT = datetime.datetime.now()

DSTAMP = CURRENT.strftime("%Y%m%d")
TSTAMP = CURRENT.strftime("%H%M%S")

LSTAMP = f'{DSTAMP}.{TSTAMP}'

if os.name == 'nt':
    VARTMPDIR = os.path.join ( "C:", "Windows", "Temp" )
else:
    VARTMPDIR = os.path.join ( "/", "var", "tmp" )

SRCTAG = 'sumolookups'
CACHED = os.path.join(VARTMPDIR, SRCTAG, DSTAMP)

FILELIMIT = 50 * 1024 * 1024
LINELIMIT = 30000

if ARGS.APIKEY:
    SUMOUID, SUMOKEY = ARGS.APIKEY.split(":")
    os.environ['SUMOUID'] = SUMOUID
    os.environ['SUMOKEY'] = SUMOKEY

if ARGS.CONFIG:
    CFGFILE = os.path.abspath(ARGS.CONFIG)
    CONFIG = configparser.ConfigParser()
    CONFIG.optionxform = str
    CONFIG.read(CFGFILE)
    if ARGS.verbose > 8:
        print(dict(CONFIG.items('Default')))

    if CONFIG.has_option("Default", "SUMOUID"):
        SUMOUID = CONFIG.get("Default", "SUMOUID")
        os.environ['SUMOUID'] = SUMOUID

    if CONFIG.has_option("Default", "SUMOKEY"):
        SUMOKEY = CONFIG.get("Default", "SUMOKEY")
        os.environ['SUMOKEY'] = SUMOKEY

    if CONFIG.has_option("Default", "SUMOEND"):
        SUMOEND = CONFIG.get("Default", "SUMOEND")
        os.environ['SUMOEND'] = SUMOEND

    if CONFIG.has_option("Default", "CACHED"):
        CACHED = os.path.abspath(CONFIG.get("Default", "CACHED"))

if ARGS.APIKEY:
    os.environ['APIKEY'] = ARGS.APIKEY
    ( os.environ['SUMOUID'], os.environ['SUMOKEY'] ) = ARGS.APIKEY.split(":")

if ARGS.CACHED:
    CACHED = os.path.abspath(ARGS.CACHED)

try:

    SUMOUID = os.environ['SUMOUID']
    SUMOKEY = os.environ['SUMOKEY']

except KeyError as myerror:

    print(f'Environment Variable Not Set :: {myerror.args[0]}')

class SumoApiClient():
    """
    General Sumo Logic API Client Class
    """

    def __init__(self, access_id, access_key, region, cookie_file='cookies.txt'):
        """
        Initializes the Sumo Logic object
        """
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers = {'content-type': 'application/json', \
            'accept': 'application/json'}
        self.apipoint = 'https://api.' + region + '.sumologic.com/api'
        cookiejar = http.cookiejar.FileCookieJar(cookie_file)
        self.session.cookies = cookiejar

    def delete(self, method, params=None, headers=None, data=None):
        """
        Defines a Sumo Logic Delete operation
        """
        response = self.session.delete(self.apipoint + method, \
            params=params, headers=headers, data=data)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def get(self, method, params=None, headers=None):
        """
        Defines a Sumo Logic Get operation
        """
        response = self.session.get(self.apipoint + method, \
            params=params, headers=headers)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def upload(self, method, headers=None, files=None):
        """
        Defines a Sumo Logic Post operation
        """
        response = self.session.post(self.apipoint + method, \
            headers=headers, files=files)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def post(self, method, data, headers=None, params=None):
        """
        Defines a Sumo Logic Post operation
        """
        response = self.session.post(self.apipoint + method, \
            data=json.dumps(data), headers=headers, params=params)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def put(self, method, data, headers=None, params=None):
        """
        Defines a Sumo Logic Put operation
        """
        response = self.session.put(self.apipoint + method, \
            data=json.dumps(data), headers=headers, params=params)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def create_folder(self, folder_name, parent_id, adminmode=False):
        """
        creates a named folder
        """
        headers = {'isAdminMode': str(adminmode)}
        jsonpayload = {
            'name': folder_name,
            'parentId': str(parent_id)
        }

        url = '/v2/content/folders'
        body = self.post(url, jsonpayload, headers=headers).text
        results = json.loads(body)
        return results

    def create_lookup(self, parent_id, jsonfile, lookupname, adminmode=False):
        """
        creates a lookup file stub
        """
        headers = {'isAdminMode': str(adminmode)}

        with open (jsonfile, "r", encoding='utf8') as jsonobject:
            jsonpayload = json.load(jsonobject)
            jsonpayload['parentFolderId'] = parent_id
            jsonpayload['name'] = lookupname

        url = '/v1/lookupTables'
        body = self.post(url, jsonpayload, headers=headers).text
        results = json.loads(body)
        return results

    def populate_lookup_merge(self, parent_id, csvfile):
        """
        populates a lookup file stub
        """

        with open(csvfile, "r", encoding='utf8') as fileobject:
            csvpayload = fileobject.read()

        files = { 'file' : ( csvfile, csvpayload ) }
        headers = {'merge': 'true' }

        url = '/v1/lookupTables/' + parent_id + '/upload'
        body = self.upload(url, headers=headers, files=files).text
        results = json.loads(body)
        return results


    def upload_lookup_data(self,lookupfileid, targetfile):
        """
        Upload the lookup file data
        """
        self.session.headers = None

        filesize = os.path.getsize(targetfile)
        if filesize <= FILELIMIT:
            if ARGS.verbose > 6:
                print(f'UPLOAD id: {lookupfileid} file: {targetfile}')
            _result = self.populate_lookup(lookupfileid, targetfile)
        else:
            split_dir = os.path.splitext(targetfile)[0]
            os.makedirs(split_dir, exist_ok=True)
            filesplit = Split(targetfile, split_dir )
            filesplit.bylinecount(linecount=LINELIMIT, includeheader=True)
            for csv_file in glob.glob(glob.escape(split_dir) + "/*.csv"):
                if ARGS.verbose > 6:
                    print(f'UPLOAD id: {lookupfileid} file: {csv_file}')
                _result = self.populate_lookup_merge(lookupfileid, csv_file)

    def populate_lookup(self, parent_id, csvfile):
        """
        populates a lookup file stub
        """

        with open(csvfile, "r", encoding='utf8') as fileobject:
            csvpayload = fileobject.read()

        files = { 'file' : ( csvfile, csvpayload ) }

        url = '/v1/lookupTables/' + parent_id + '/upload'
        body = self.upload(url, files=files).text
        results = json.loads(body)
        return results

    def get_folder(self, folder_id, adminmode=False):
        """
        queries folders
        """
        headers = {'isAdminMode': str(adminmode).lower()}
        url = '/v2/content/folders/' + str(folder_id)
        body = self.get(url, headers=headers).text
        results = json.loads(body)
        return results

    def get_personal_folder(self):
        """
        get personal base folder
        """
        url = '/v2/content/folders/personal'
        body = self.get(url).text
        results = json.loads(body)
        return results

def main():
    """
    General Logic
    """

    source = SumoApiClient(SUMOUID, SUMOKEY, SUMOEND)

    personal_folder_results = source.get_personal_folder()
    personal_folder_id = personal_folder_results['id']

    lookupjson = ARGS.LOOKUPJSON
    lookupfile = ARGS.LOOKUPFILE

    build_lookup_dir = 'yes'

    for child in personal_folder_results['children']:
        if child['name'] == SRCTAG:
            build_lookup_dir = 'no'
            lookupdir_id = child['id']

    if build_lookup_dir == 'yes':
        create_folder_results = source.create_folder(SRCTAG, personal_folder_id)
        lookupdir_id = create_folder_results['id']
        if ARGS.verbose > 2:
            print(f'Created ParentDir: {lookupdir_id}')

    lookup_name = os.path.splitext(os.path.basename(lookupjson))[0]
    create_lookup_file_results = source.create_lookup(lookupdir_id, lookupjson, lookup_name)
    lookup_file_id = create_lookup_file_results['id']

    if ARGS.verbose > 2:
        print(f'Created Lookup: {lookup_file_id}')

    source.session.headers = None

    upload_lookup_file_results = source.populate_lookup(lookup_file_id, lookupfile)
    upload_lookup_file_id = upload_lookup_file_results['id']

    if ARGS.verbose > 2:
        print(f'Uploaded Lookup: {upload_lookup_file_id}')

if __name__ == '__main__':
    main()
