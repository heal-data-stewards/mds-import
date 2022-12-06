#!/usr/bin/env python
import json
import os
import re
import shutil
import urllib.parse
import logging

import requests

#
# This Python script downloads all data dictionaries from the HEAL Platform MDS service.
#

# The maximum number of data dictionaries to download from MDS.
MDS_DEFAULT_LIMIT = 1000

# MDS Endpoint.
MDS_ENDPOINT = 'https://preprod.healdata.org/mds/'

# Data dictionary output path.
DD_OUTPUT_DIR = 'data/dictionaries'

# Method to retrieve a data dictionary.
def retrieve_dd(drs_uri: str):
    assert drs_uri.startswith('dg.H34L/')
    MDS_DD_ENDPOINT = urllib.parse.urljoin(MDS_ENDPOINT, f'metadata/{drs_uri}')

    result = requests.get(MDS_DD_ENDPOINT)
    if not result.ok:
        raise RuntimeError(f'Could not retrieve data dictionary {drs_uri}: {result}')

    return result.json()


# Method to retrieve a list of all data dictionaries.
def retrieve_dd_list(limit = MDS_DEFAULT_LIMIT):
    DD_LIST_ENDPOINT = urllib.parse.urljoin(MDS_ENDPOINT, f'metadata?_guid_type=data_dictionary&limit={limit}')

    result = requests.get(DD_LIST_ENDPOINT)
    if not result.ok:
        raise RuntimeError(f'Could not retrieve data dictionary list: {result}')

    return result.json()


# Get all data dictionaries and store them in data/dictionaries
def download_dds():
    shutil.rmtree(DD_OUTPUT_DIR, ignore_errors=True)
    os.makedirs(DD_OUTPUT_DIR, exist_ok=True)

    logging.info(f"Retrieving data dictionary list from {MDS_ENDPOINT} ...")
    dds = retrieve_dd_list()
    logging.info(f"Found {len(dds)} data dictionaries.")

    count_dds = 0
    count_fields = 0
    for dd_uri in dds:
        logging.info(f"Retrieving data dictionary {dd_uri} ...")
        dd_uri_pathname = re.sub(r"\W", '_', dd_uri)
        filename = os.path.join(DD_OUTPUT_DIR, f'{dd_uri_pathname}.json')

        dd = retrieve_dd(dd_uri)
        with open(filename, 'w') as f:
            json.dump(dd, f, sort_keys=True, indent=2)

        logging.info(f"Downloaded a data dictionary containing {len(dd['data_dictionary'])} fields.")
        count_fields += 1
        count_dds += 1

    logging.info(f"Downloaded {count_fields} fields from {count_dds} data dictionaries.")