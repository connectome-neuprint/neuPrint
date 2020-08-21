#!/bin/env

# use python 3.6 or greater
# python map_toplevel_roi_for_csv.py emdata4:8900 6f2cb synapses-ff278-dvid.csv synapses-6f2cb-superlevel-rois.csv

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
#    "MB(R)",
#    "MB(L)",
#

rois = [
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
    "ME(R)",
    "AME(R)",
    "LO(R)",
    "LOP(R)",
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
    "LH(R)",
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
    "AL(R)",
    "AL(L)",
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
    "PRW",
    "GNG",
    "AOT(R)",
    "GC",
    "GF(R)",
    "mALT(R)",
    "mALT(L)",
    "POC"
]


from neuclease.dvid import fetch_combined_roi_volume

# Fetch and combine into one volume
rois = { roi_name: label for label, roi_name in enumerate(rois, start=1) }

#MB(L)
mbL_label = rois["a'L(L)"]
rois["aL(L)"] = mbL_label
rois["b'L(L)"] = mbL_label
rois["bL(L)"] = mbL_label
rois["CA(L)"] = mbL_label
rois["gL(L)"] = mbL_label

#MB(R)
mbR_label = rois["a'L(R)"]
rois["aL(R)"] = mbR_label
rois["b'L(R)"] = mbR_label
rois["bL(R)"] = mbR_label
rois["CA(R)"] = mbR_label
rois["gL(R)"] = mbR_label
rois["PED(R)"] = mbR_label


#OL
OL_label = rois["ME(R)"]
rois["AME(R)"] = OL_label
rois["LO(R)"] = OL_label
rois["LOP(R)"] = OL_label

CX_label = rois["FB"]
rois["AB(R)"] = CX_label
rois["AB(L)"] = CX_label
rois["EB"] = CX_label
rois["PB"] = CX_label
rois["NO"] = CX_label

#LX_R
LX_R_label = rois["BU(R)"]
rois["LAL(R)"] = LX_R_label
rois["GA(R)"] = LX_R_label

#LX L
LX_L_label = rois["BU(L)"]
rois["LAL(L)"] = LX_L_label



#VLNP
VLNP_label = rois['AOTU(R)']
rois['AVLP(R)'] = VLNP_label
rois['PVLP(R)'] = VLNP_label
rois['PLP(R)'] = VLNP_label
rois['WED(R)'] = VLNP_label

#SNP
SNP_R_label = rois['SLP(R)']
rois['SIP(R)'] = SNP_R_label
rois['SMP(R)'] = SNP_R_label

SNP_L_label = rois['SIP(L)']
rois['SMP(L)'] = SNP_L_label

#INP R
INP_label = rois['CRE(R)']
rois['SCL(R)'] = INP_label
rois['ICL(R)'] = INP_label
rois['ATL(R)'] = INP_label
#INP L
rois['CRE(L)'] = INP_label
rois['SCL(L)'] = INP_label
rois['ICL(L)'] = INP_label
rois['ATL(L)'] = INP_label
rois['IB'] = INP_label

#VMNP
VMNP_label = rois['VES(R)']
rois['VES(L)'] = VMNP_label
rois['EPA(R)'] = VMNP_label
rois['EPA(L)'] = VMNP_label
rois['GOR(R)'] = VMNP_label
rois['GOR(L)'] = VMNP_label
rois['SPS(R)'] = VMNP_label
rois['SPS(L)'] = VMNP_label
rois['IPS(R)'] = VMNP_label

#PENP
PENP_label = rois['SAD']
rois['AMMC'] = PENP_label
rois['FLA(R)'] = PENP_label
rois['CAN(R)'] = PENP_label
rois['PRW'] = PENP_label

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
consolidated_rois ={}
consolidated_rois['GNG'] = rois['GNG']
consolidated_rois['PENP'] = PENP_label
consolidated_rois['VMNP'] = VMNP_label
consolidated_rois['AL(R)'] = rois['AL(R)']
consolidated_rois['AL(L)'] = rois['AL(L)']
consolidated_rois['INP'] = INP_label
consolidated_rois['SNP(R)'] = SNP_R_label
consolidated_rois['SNP(L)'] = SNP_L_label
consolidated_rois['LH(R)'] = rois['LH(R)']
consolidated_rois['VLNP(R)'] = VLNP_label
consolidated_rois['LX(R)'] = LX_R_label
consolidated_rois['LX(L)'] = LX_L_label
consolidated_rois['CX'] = CX_label
consolidated_rois['OL(R)'] = OL_label
#consolidated_rois['MB(L)'] = rois["MB(L)"]
#consolidated_rois['MB(R)'] = rois["MB(R)"]
consolidated_rois['MB(L)'] = mbL_label
consolidated_rois['MB(R)'] = mbR_label


#major fiber bundles
consolidated_rois['AOT(R)'] = rois['AOT(R)']
consolidated_rois['GC'] = rois['GC']
consolidated_rois['GF(R)'] = rois['GF(R)']
consolidated_rois['mALT(R)'] = rois['mALT(R)']
consolidated_rois['mALT(L)'] = rois['mALT(L)']
consolidated_rois['POC'] = rois['POC']

print("MAP ROIs:", consolidated_rois)


extract_labels_from_volume(synapses_df, roi_vol, vol_scale=5, label_names=consolidated_rois)

outfile = sys.argv[4]
#print(synapses_df.head())
#print(synapses_df['label_name'].value_counts())
print("Writing csv results...")
synapses_df.to_csv(outfile, header=True, index=False)
synapses_df.head()
