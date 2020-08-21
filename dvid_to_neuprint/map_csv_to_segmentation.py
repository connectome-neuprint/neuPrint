#!/bin/env

# Use python 3.6 or greater
# python map_csv_to_segmentation.py emdata4:8900 6f2cb segmentation clean-synapses-6f2cb-sub3-roi-added.csv synIDs_synapses-6f2cb-rois-bodyIDs.csv

import os
import sys
import logging
import requests
import numpy as np
import pandas as pd
from neuclease.dvid import fetch_labels_batched

dvid_server = sys.argv[1]
dvid_uuid = sys.argv[2]
segmentation_inst = sys.argv[3] 
infile = sys.argv[4]
outfile = sys.argv[5]

master_seg = (dvid_server, dvid_uuid, segmentation_inst)

df = pd.read_csv(infile)

labels = fetch_labels_batched(*master_seg, df[['z', 'y', 'x']].values, threads=32)

df['body'] = labels

df.to_csv(outfile, index=False)
