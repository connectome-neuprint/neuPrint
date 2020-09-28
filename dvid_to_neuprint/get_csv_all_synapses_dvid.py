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
# python get_csv_all_synapses_dvid.py emdata4:8900 ff278 synapses

server = sys.argv[1]
uuid = sys.argv[2]
annotation = sys.argv[3]

master = (server, uuid)

#Dimensions of your volume to get all the synapses
box_zyx = [(0,0,0), (41408,39552,34432)]

# get as pandas dataframe
synapses_df, partners_df = fetch_synapses_in_batches(*master, annotation, box_zyx, format='pandas')
synapses_df = synapses_df[['x', 'y', 'z', 'kind', 'conf', 'user']]
synapses_df.to_csv(f'synapses-{uuid}-dvid.csv', header=True, index=False)
synapses_df.head()
