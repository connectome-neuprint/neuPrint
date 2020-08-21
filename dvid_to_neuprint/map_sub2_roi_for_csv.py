#!/bin/env

# Use python 3.6 or greater
# python map_sub2_roi_for_csv.py emdata4:8900 6f2cb clean-synapses-6f2cb-sub1-roi-added.csv synapses-6f2cb-sub2-rois-added.csv

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


server = sys.argv[1]
uuid = sys.argv[2]

master = (server, uuid)
from neuclease.dvid import fetch_repo_instances
# Get the list of all ROIs
rois = [
"dACA(R)",
"vACA(R)",
"lACA(R)",
"a'1(R)",
"a'2(R)",
"a'3(R)",
"a1(R)",
"a2(R)",
"a3(R)",
"g1(R)",
"g2(R)",
"g3(R)",
"g4(R)",
"g5(R)",
"b'1(R)",
"b'2(R)",
"b1(R)",
"b2(R)",
"FBl1",
"FBl2",
"FBl3",
"FBl4",
"FBl5",
"FBl6",
"FBl7",
"FBl8",
"FBl9",
"EBr1",
"EBr2r4",
"EBr3am",
"EBr3d",
"EBr3pw",
"EBr5",
"EBr6",
"PB(R1)",
"PB(R2)",
"PB(R3)",
"PB(R4)",
"PB(R5)",
"PB(R6)",
"PB(R7)",
"PB(R8)",
"PB(R9)",
"PB(L1)",
"PB(L2)",
"PB(L3)",
"PB(L4)",
"PB(L5)",
"PB(L6)",
"PB(L7)",
"PB(L8)",
"PB(L9)",
"NO(R)",
"NO(L)",
"LAL(-GA)(R)",
"GA(R)",
"CRE(-ROB,-RUB)(R)",
"RUB(R)",
"CRE(-RUB)(L)",
"RUB(L)",
"ROB(R)",
"SAD(-AMMC)",
"AMMC"
]

print(rois)

from neuclease.dvid import fetch_combined_roi_volume

# Fetch and combine into one volume
rois = { roi_name: label for label, roi_name in enumerate(rois, start=1) }

consolidated_rois ={}

#OL
#consolidated_rois['ME(R)'] = rois['ME']
#consolidated_rois['AME(R)'] = rois['AME']
#consolidated_rois['LO(R)'] = rois["LO(R)"]
#consolidated_rois['LOP(R)'] = rois["LOP(R)"]
#consolidated_rois['LOX'] = LOX_label


#MB
#consolidated_rois['CA(R)'] = rois["CA"]
#consolidated_rois['CA(L)'] = rois["(L)CA"] 
consolidated_rois['dACA(R)'] = rois["dACA(R)"] 
consolidated_rois['lACA(R)'] = rois["lACA(R)"]
consolidated_rois['vACA(R)'] = rois["vACA(R)"]
#consolidated_rois['PED(R)'] = rois["PED"]
#consolidated_rois["a'L(L)"] = rois["a'L(L)"]
#consolidated_rois["a'L(R)"] = rois["a'L(R)"]
consolidated_rois["a'1(R)"] = rois["a'1(R)"]
consolidated_rois["a'2(R)"] =rois["a'2(R)"]
consolidated_rois["a'3(R)"] =rois["a'3(R)"]
#consolidated_rois["aL(R)"] = rois["aL(R)"]
consolidated_rois["a1(R)"] =rois["a1(R)"]
consolidated_rois["a2(R)"] =rois["a2(R)"]
consolidated_rois["a3(R)"] =rois["a3(R)"]
#consolidated_rois["aL(L)"] = rois["aL(L)"]
#consolidated_rois['VL'] = VL_label
#consolidated_rois["gL(R)"] = rois["gL(R)"]
consolidated_rois["g1(R)"] =rois["g1(R)"]
consolidated_rois["g2(R)"] =rois["g2(R)"]
consolidated_rois["g3(R)"] =rois["g3(R)"]
consolidated_rois["g4(R)"] =rois["g4(R)"]
consolidated_rois["g5(R)"] =rois["g5(R)"]
#consolidated_rois["gL(L)"] = rois["gL(L)"]
#consolidated_rois["b'L(R)"] = rois["b'L(R)"]
consolidated_rois["b'1(R)"] =rois["b'1(R)"]
consolidated_rois["b'2(R)"] =rois["b'2(R)"]
#consolidated_rois["b'L(L)"] = rois["b'L(L)"]
#consolidated_rois["bL(R)"] = rois["bL(R)"]
consolidated_rois["b1(R)"] =rois["b1(R)"]
consolidated_rois["b2(R)"] =rois["b2(R)"]
#consolidated_rois["bL(L)"] = rois["bL(L)"]
#consolidated_rois['ML'] = ML_label

#CX
#CB
#consolidated_rois["FB"] = rois["FB"]
consolidated_rois["FBl1"] =rois["FBl1"]
consolidated_rois["FBl2"] =rois["FBl2"]
consolidated_rois["FBl3"] =rois["FBl3"]
consolidated_rois["FBl4"] =rois["FBl4"]
consolidated_rois["FBl5"] =rois["FBl5"]
consolidated_rois["FBl6"] =rois["FBl6"]
consolidated_rois["FBl7"] =rois["FBl7"]
consolidated_rois["FBl8"] =rois["FBl8"]
consolidated_rois["FBl9"] =rois["FBl9"]
#consolidated_rois["AB(R)"] = rois['AB'] 
#consolidated_rois["EB"] = rois["EB"]
consolidated_rois["EBr1"] =rois["EBr1"]
consolidated_rois["EBr2r4"] =rois["EBr2r4"]
consolidated_rois["EBr3am"] =rois["EBr3am"]
consolidated_rois["EBr3d"] =rois["EBr3d"]
consolidated_rois["EBr3pw"] =rois["EBr3pw"]
consolidated_rois["EBr5"] =rois["EBr5"]
consolidated_rois["EBr6"] =rois["EBr6"]

# PB
for name in rois.keys():
    if name.startswith('PB'):
        consolidated_rois[name] = rois[name]

# NO
consolidated_rois["NO(L)"] = rois['NO(L)']
consolidated_rois['NO(R)'] = rois['NO(R)']

#LX
#consolidated_rois['BU(R)'] = rois["BU"]
#consolidated_rois['BU(L)'] = rois["(L)BU"]
#consolidated_rois['LAL(R)'] = rois["LAL"] 
#consolidated_rois['LAL(L)'] = rois["(L)LAL"] 
consolidated_rois['LAL(-GA)(R)'] = rois['LAL(-GA)(R)']
consolidated_rois['GA(R)'] = rois['GA(R)']

#VLNP
#consolidated_rois['AOTU(R)'] = rois['AOTU']
#consolidated_rois['AVLP(R)'] = rois['AVLP(R)'] 
#consolidated_rois['PVLP(R)'] = rois['PVLP(R)']
#consolidated_rois['VLP'] = VLP_label
#consolidated_rois['PLP(R)'] = rois['PLP']
#consolidated_rois['WED(R)'] = rois['WED']

#SNP
#consolidated_rois['SLP(R)'] = rois['SLP']
#consolidated_rois['SIP(R)'] = rois['SIP']
#consolidated_rois['SIP(L)'] = rois['(L)SIP']
#consolidated_rois['SMP(R)'] = rois['SMP']
#consolidated_rois['SMP(L)'] = rois['(L)SMP']

#INP
#consolidated_rois['CRE(R)'] = rois['CRE']
#consolidated_rois['CRE(L)'] = rois['(L)CRE']
consolidated_rois['CRE(-ROB|-RUB)(R)'] = rois['CRE(-ROB,-RUB)(R)']
consolidated_rois['RUB(R)'] = rois['RUB(R)']
consolidated_rois['CRE(-RUB)(L)'] = rois['CRE(-RUB)(L)']
consolidated_rois['RUB(L)'] = rois['RUB(L)']
consolidated_rois['ROB(R)'] = rois['ROB(R)']

#consolidated_rois['SCL(R)'] = rois['SCL(R)']
#consolidated_rois['SCL(L)'] = rois['SCL(L)']
#consolidated_rois['ICL(R)'] = rois['ICL(R)']
#consolidated_rois['ICL(L)'] = rois['ICL(L)']
#consolidated_rois['CL'] = CL_label
#consolidated_rois['IB'] = rois['IB']
#consolidated_rois['ATL(R)'] = rois['ATL']
#consolidated_rois['ATL(L)'] = rois['(L)ATL']

#AL
#consolidated_rois['AL-DC3(R)'] = rois['AL-DC3']

#VMNP
#consolidated_rois['VES(R)'] = rois['VES(R)']
#consolidated_rois['VES(L)'] = rois['VES(L)']
#consolidated_rois['EPA(R)'] = rois['EPA(R)']
#consolidated_rois['EPA(L)'] = rois['EPA(L)']
#consolidated_rois['GOR(R)'] = rois['GOR(R)']
#consolidated_rois['GOR(L)'] = rois['GOR(L)']
#consolidated_rois['VX'] = VX_label
#consolidated_rois['SPS(R)'] = rois['SPS(R)'] 
#consolidated_rois['SPS(L)'] = rois['SPS(L)']
#consolidated_rois['IPS'] = rois['IPS']
#consolidated_rois['PS'] = VX_label

#PENP
consolidated_rois['SAD(-AMMC)'] = rois['SAD(-AMMC)']
consolidated_rois['AMMC'] = rois['AMMC']
#consolidated_rois['FLA(R)'] = rois['FLA']
#consolidated_rois['CAN(R)'] =  rois['CAN']
#consolidated_rois['PRW'] = rois['PRW'] 

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
