#!/bin/env

# python generate_Neuprint_Synapses_csv.py synIDs_synapses-6f2cb-rois-bodyIDs.csv hemibrain > Neuprint_Synapses_6f2cb.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time

if __name__ == '__main__':
    synapses_csv = sys.argv[1]
    dataset = sys.argv[2]

    all_rois_csv = "all_ROIs.txt"
    all_rois = []
    allRoisList = open(all_rois_csv,'r')
    for line in allRoisList:
        roi_name = line.rstrip('\n')
        #print(roi_name)
        all_rois.append(roi_name)

    header = '":ID(Syn-ID)","type:string","confidence:float","location:point{srid:9157}",":Label"'

    for roi in all_rois:
        header = header + ',"' + roi + ':boolean"'

    print(header)
    id_count = 0
    synapseList = open(synapses_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            id_count += 1
            data_str = line.rstrip('\n')
            synData = data_str.split(',')
            #x,y,z,type,confidence,super_roi,sub_roi
            syn_id = synData[0]
            x = synData[1]
            y = synData[2]
            z = synData[3]
            syn_type_dvid = synData[4]
            isPre_bool = "false"
            isPost_bool = "false"
            if syn_type_dvid == "PostSyn":
                isPost_bool = "true"
                syn_type = "post"
            elif syn_type_dvid == "PreSyn":
                isPre_bool = "true"
                syn_type = "pre"

            confidence = synData[5]
            super_roi = synData[6]
            sub1_roi = synData[7]
            sub2_roi = synData[8].replace("|",",")
            sub3_roi = synData[9]
            #syn_id = 1000000000 + id_count
            location = "\"{x:" + x + ", y:" + y + ", z:" + z + "}\""
            syn_line = str(syn_id) + "," + syn_type + "," + confidence + "," + location + ",Synapse;" + dataset + "_Synapse"
            for roi_name in all_rois:
                is_roi = ""
                if roi_name == super_roi:
                    is_roi = "true"
                if roi_name == sub1_roi:
                    is_roi = "true"
                if roi_name == sub2_roi:
                    is_roi = "true"
                if roi_name == sub3_roi:
                    is_roi = "true"
                syn_line = syn_line + "," + is_roi

            print(syn_line)
