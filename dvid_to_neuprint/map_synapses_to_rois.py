#!/bin/env

import os
import sys
import logging
import requests
#from tqdm import tqdm_notebook
#tqdm = tqdm_notebook
import numpy as np
import pandas as pd
from neuclease import configure_default_logging
from neuclease.dvid import fetch_repo_instances, fetch_combined_roi_volume, load_synapses_csv, extract_labels_from_volume
#from neuclease.dvid import fetch_combined_roi_volume
#from neuclease.dvid import load_synapses_csv
#from neuclease.util import extract_labels_from_volume

configure_default_logging()

server = sys.argv[1]
uuid = sys.argv[2]
roi_file_list = sys.argv[3]
synapse_csv = sys.argv[4]
outfile = sys.argv[5]

master = (server, uuid)

# Get the list of all ROIs
rois = []
allRoisList = open(roi_file_list,'r')
for line in allRoisList:
    roi_name = line.rstrip('\n')
    rois.append(roi_name)

# Fetch and combine into one volume
rois = { roi_name: label for label, roi_name in enumerate(rois, start=1) }

print(rois)

roi_vol, box, overlaps = fetch_combined_roi_volume(*master, rois, box_zyx=[(0,0,0), None])

synapses_df = load_synapses_csv(synapse_csv)
print(f"Loaded {len(synapses_df)} points")

extract_labels_from_volume(synapses_df, roi_vol, vol_scale=5, label_names=rois)

print("Writing csv results...")
synapses_df.to_csv(outfile, header=True, index=False)
synapses_df.head()
