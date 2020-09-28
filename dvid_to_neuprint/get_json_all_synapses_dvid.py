#!/bin/env

import os
import sys
import logging
import json
import requests
from tqdm import tqdm_notebook
tqdm = tqdm_notebook

import numpy as np
import pandas as pd
from neuclease.dvid import find_master, fetch_synapses_in_batches

# use python 3.6 or greater
# python get_json_all_synapses_dvid.py emdata4:8900 ff278 synapses

server = sys.argv[1]
uuid = sys.argv[2]

master = (server, uuid)

#Dimension of your volume to get all 
box_zyx = [(0,0,0), (41408,39552,34432)]

annotation = sys.argv[3]
synapses_json = fetch_synapses_in_batches(*master, annotation, box_zyx, format='json')
#json_string = json.dumps(synapses_json)

outputfilename = annotation + "-dvid-" + uuid + ".json"
with open(outputfilename, 'wt') as f:
    json.dump(synapses_json, f)

# get as pandas dataframe
#synapses_df = fetch_synapses_in_batches(*master, 'synapses', box_zyx, format='pandas')
#synapses_df.to_csv(f'synapses-{uuid}-SUBVOLUME.csv', header=True, index=False)
#synapses_df.head()
