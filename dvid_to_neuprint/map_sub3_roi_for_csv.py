#!/bin/env

# Use python 3.6 or greater
# python map_sub3_roi_for_csv.py emdata4:8900 6f2cb clean-synapses-6f2cb-sub2-roi-added.csv synapses-6f2cb-sub3-rois-added.csv

import os
import sys
import logging
import requests
#from tqdm import tqdm_notebook
#tqdm = tqdm_notebook
import numpy as np
import pandas as pd
from neuclease import configure_default_logging
configure_default_logging()

#server = 'emdata1:8900'
#uuid = 'ef1da'
server = sys.argv[1]
uuid = sys.argv[2]

master = (server, uuid)
from neuclease.dvid import fetch_repo_instances
# Get the list of all ROIs
rois = [
"CA(R)",
"PED(R)",
"a'L(R)",
"aL(R)",
"gL(R)",
"b'L(R)",
"bL(R)",
"dACA(R)",
"lACA(R)",
"vACA(R)",
"FB-column3",
"NO1(R)",
"NO1(L)",
"NO2(R)",
"NO2(L)",
"NO3(R)",
"NO3(L)"
]

from neuclease.dvid import fetch_combined_roi_volume

# Fetch and combine into one volume
rois = { roi_name: label for label, roi_name in enumerate(rois, start=1) }

consolidated_rois ={}

#MB
#MB(R)
mbACA_R_label = rois["CA(R)"]
rois["PED(R)"] = mbACA_R_label
rois["a'L(R)"] = mbACA_R_label
rois["aL(R)"] = mbACA_R_label
rois["gL(R)"] = mbACA_R_label
rois["b'L(R)"] = mbACA_R_label
rois["bL(R)"] = mbACA_R_label
rois["dACA(R)"] = mbACA_R_label
rois["lACA(R)"] = mbACA_R_label
rois["vACA(R)"] = mbACA_R_label
consolidated_rois['MB(+ACA)(R)'] = mbACA_R_label

consolidated_rois["FB-column3"] = rois['FB-column3']

consolidated_rois["NO1(L)"] = rois["NO1(L)"]
consolidated_rois["NO2(L)"] = rois["NO2(L)"]
consolidated_rois["NO3(L)"] = rois["NO3(L)"]

consolidated_rois['NO1(R)'] = rois["NO1(R)"]
consolidated_rois['NO2(R)'] = rois["NO2(R)"]
consolidated_rois['NO3(R)'] = rois["NO3(R)"]

print(rois)

roi_vol, box, overlaps = fetch_combined_roi_volume(*master, rois, box_zyx=[(0,0,0), None])

from neuclease.dvid import load_synapses_csv

#Test
#synapse_csv = '/groups/flyem/home/flyem/bin/neuclease_get_all_synapses/synapses-9e0d2-SUBVOLUME.csv'
#Prod
#synapse_csv = '/nrs/flyem/data/hemi_brain_synapses/syn_base_dvid_ef1da/synapses-ef1da.csv'

synapse_csv = sys.argv[3]
synapses_df = load_synapses_csv(synapse_csv)
print(f"Loaded {len(synapses_df)} points")
from neuclease.util import extract_labels_from_volume

#consolidated_rois = dict(rois)

print("MAP ROIs:", consolidated_rois)


extract_labels_from_volume(synapses_df, roi_vol, vol_scale=5, label_names=consolidated_rois)

#print(synapses_df.head())
#print(synapses_df['label_name'].value_counts())
print("Writing csv results...")
outfile = sys.argv[4]
synapses_df.to_csv(outfile, header=True, index=False)
synapses_df.head()
