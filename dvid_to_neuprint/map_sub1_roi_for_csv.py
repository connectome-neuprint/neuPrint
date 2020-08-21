#!/bin/env

# Use python 3.6 or greater
# python map_sub1_roi_for_csv.py emdata4:8900 6f2cb clean-synapses-6f2cb-superlevel-rois.csv synapses-6f2cb-sub1-rois-added.csv

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
    "ME(R)",
    "AME(R)",
    "LO(R)",
    "LOP(R)",
    "CA(R)",
    "CA(L)",
    "PED(R)",
    "a'L(R)",
    "a'L(L)",
    "aL(R)",
    "aL(L)",
    "gL(R)",
    "gL(L)",
    "b'L(R)",
    "b'L(L)",
    "bL(R)",
    "bL(L)",
    "FB",
    "EB",
    "AB(R)",
    "AB(L)",
    "PB",
    "NO",
    "BU(R)",
    "BU(L)",
    "LAL(R)",
    "LAL(L)",
    "AOTU(R)",
    "AVLP(R)",
    "PVLP(R)",
    "PLP(R)",
    "WED(R)",
    "SLP(R)",
    "SIP(R)",
    "SIP(L)",
    "SMP(R)",
    "SMP(L)",
    "CRE(R)",
    "CRE(L)",
    "SCL(R)",
    "SCL(L)",
    "ICL(R)",
    "ICL(L)",
    "IB",
    "ATL(R)",
    "ATL(L)",
    "AL-DA1(R)",
    "AL-DA2(R)",
    "AL-DA3(R)",
    "AL-DA4l(R)",
    "AL-DA4m(R)",
    "AL-DC1(R)",
    "AL-DC2(R)",
    "AL-DC3(R)",
    "AL-DC4(R)",
    "AL-DL1(R)",
    "AL-DL2d(R)",
    "AL-DL2v(R)",
    "AL-DL3(R)",
    "AL-DL4(R)",
    "AL-DL5(R)",
    "AL-DM1(R)",
    "AL-DM2(R)",
    "AL-DM3(R)",
    "AL-DM4(R)",
    "AL-DM5(R)",
    "AL-DM6(R)",
    "AL-DP1l(R)",
    "AL-DP1m(R)",
    "AL-D(R)",
    "AL-VA1d(R)",
    "AL-VA1v(R)",
    "AL-VA2(R)",
    "AL-VA3(R)",
    "AL-VA4(R)",
    "AL-VA5(R)",
    "AL-VA6(R)",
    "AL-VA7l(R)",
    "AL-VA7m(R)",
    "AL-VC1(R)",
    "AL-VC2(R)",
    "AL-VC3l(R)",
    "AL-VC3m(R)",
    "AL-VC4(R)",
    "AL-VC5(R)",
    "AL-VL1(R)",
    "AL-VL2a(R)",
    "AL-VL2p(R)",
    "AL-VM1(R)",
    "AL-VM2(R)",
    "AL-VM3(R)",
    "AL-VM4(R)",
    "AL-VM5d(R)",
    "AL-VM5v(R)",
    "AL-VM7d(R)",
    "AL-VM7v(R)",
    "AL-V(R)",
    "AL-DA2(L)",
    "AL-DA3(L)",
    "AL-DA4m(L)",
    "AL-DC1(L)",
    "AL-DC2(L)",
    "AL-DC4(L)",
    "AL-DL4(L)",
    "AL-DL5(L)",
    "AL-D(L)",
    "AL-DM1(L)",
    "AL-DM2(L)",
    "AL-DM3(L)",
    "AL-DM4(L)",
    "AL-DM5(L)",
    "AL-DM6(L)",
    "AL-DP1m(L)",
    "AL-VA6(L)",
    "AL-VM7d(L)",
    "AL-VM7v(L)",
    "AL-VP5(R)",
    "AL-VP4(R)",
    "AL-VP3(R)",
    "AL-VP2(R)",
    "AL-VP1m(R)",
    "AL-VP1l(R)",
    "AL-VP1d(R)",
    "VES(R)",
    "VES(L)",
    "EPA(R)",
    "EPA(L)",
    "GOR(R)",
    "GOR(L)",
    "SPS(R)",
    "SPS(L)",
    "IPS(R)",
    "SAD",
    "FLA(R)",
    "CAN(R)",
    "PRW"
]
#print(rois)

from neuclease.dvid import fetch_combined_roi_volume

# Fetch and combine into one volume
rois = { roi_name: label for label, roi_name in enumerate(rois, start=1) }

print(rois)

roi_vol, box, overlaps = fetch_combined_roi_volume(*master, rois, box_zyx=[(0,0,0), None])

from neuclease.dvid import load_synapses_csv

synapse_csv = sys.argv[3]
synapses_df = load_synapses_csv(synapse_csv)
print(f"Loaded {len(synapses_df)} points")
from neuclease.util import extract_labels_from_volume

#consolidated_rois = dict(rois)

print("MAP ROIs:", rois)


extract_labels_from_volume(synapses_df, roi_vol, vol_scale=5, label_names=rois)

#print(synapses_df.head())
#print(synapses_df['label_name'].value_counts())
print("Writing csv results...")
outfile = sys.argv[4]
synapses_df.to_csv(outfile, header=True, index=False)
synapses_df.head()
