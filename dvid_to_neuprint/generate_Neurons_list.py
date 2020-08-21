#!/bin/env

# python generate_Neuron_list.py synIDs_synapses-6f2cb-rois-bodyIDs.csv > synapse_bodies_6f2cb.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import numpy as np
from tqdm import trange
from neuclease.dvid import *
from libdvid import DVIDNodeService, ConnectionMethod

if __name__ == '__main__':
    synapses_csv = sys.argv[1]
    #dvid_uuid = find_master(dvid_server,"28841") 
    
    all_bodies_syn = {}
    bodies_pre = {}
    bodies_post = {}
    all_neuron_roi_keys = {}
    neuron_roi_pre = {}
    neuron_roi_post = {}

    #superLevelrois = json.loads(open("superLevelROIs.json", 'rt').read())

    synapseList = open(synapses_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            data = data_str.split(",")
            bodyID = data[10]
            synType = data[4]
            roi = data[6]
            sub1_roi = data[7]
            sub2_roi = data[8]
            sub3_roi = data[9]
            #print("roi",roi)
            roiDetected = 0
            if len(roi) > 0:
                roiDetected += 1
                neuron_roi_key = bodyID + "_" + roi
                all_neuron_roi_keys[neuron_roi_key] = 1

            if len(sub1_roi) > 0:
                roiDetected += 1
                neuron_sub1roi_key = bodyID + "_" + sub1_roi
                all_neuron_roi_keys[neuron_sub1roi_key] = 1

            if len(sub2_roi) > 0:
                roiDetected += 1
                neuron_sub2roi_key = bodyID + "_" + sub2_roi
                all_neuron_roi_keys[neuron_sub2roi_key] = 1

            if len(sub3_roi) > 0:
                roiDetected += 1
                neuron_sub3roi_key = bodyID + "_" + sub3_roi
                all_neuron_roi_keys[neuron_sub3roi_key] = 1

            
            #print(bodyID,synType)
            all_bodies_syn[bodyID] = 1
            
            if synType == "PreSyn":
                if bodyID in bodies_pre:
                    bodies_pre[bodyID] += 1
                else:
                    bodies_pre[bodyID] = 1
                found_pre = 0
                if len(roi) > 0:
                    found_pre += 1
                    if neuron_roi_key in neuron_roi_pre:
                        neuron_roi_pre[neuron_roi_key] += 1                        
                    else:
                        neuron_roi_pre[neuron_roi_key] = 1
                if len(sub1_roi) > 0:
                    found_pre += 1
                    if neuron_sub1roi_key in neuron_roi_pre:
                        neuron_roi_pre[neuron_sub1roi_key] += 1
                    else:
                        neuron_roi_pre[neuron_sub1roi_key] = 1
                if len(sub2_roi) > 0:
                    found_pre += 1
                    if neuron_sub2roi_key in neuron_roi_pre:
                        neuron_roi_pre[neuron_sub2roi_key] += 1
                    else:
                        neuron_roi_pre[neuron_sub2roi_key] = 1
                if len(sub3_roi) > 0:
                    found_pre += 1
                    if neuron_sub3roi_key in neuron_roi_pre:
                        neuron_roi_pre[neuron_sub3roi_key] += 1
                    else:
                        neuron_roi_pre[neuron_sub3roi_key] = 1

            if synType == "PostSyn":
                if bodyID in bodies_post:
                    bodies_post[bodyID] += 1
                else:
                    bodies_post[bodyID] = 1
                found_post = 0
                if len(roi) > 0:
                    #print("roi",roi)
                    found_post += 1
                    if neuron_roi_key in neuron_roi_post:
                        neuron_roi_post[neuron_roi_key] += 1
                    else:
                        neuron_roi_post[neuron_roi_key] = 1
                if len(sub1_roi) > 0:
                    found_post += 1
                    if neuron_sub1roi_key in neuron_roi_post:
                        neuron_roi_post[neuron_sub1roi_key] += 1
                    else:
                        neuron_roi_post[neuron_sub1roi_key] = 1
                if len(sub2_roi) > 0:
                    found_post += 1
                    if neuron_sub2roi_key in neuron_roi_post:
                        neuron_roi_post[neuron_sub2roi_key] += 1
                    else:
                        neuron_roi_post[neuron_sub2roi_key] = 1
                if len(sub3_roi) > 0:
                    found_post += 1
                    if neuron_sub3roi_key in neuron_roi_post:
                        neuron_roi_post[neuron_sub3roi_key] += 1
                    else:
                        neuron_roi_post[neuron_sub3roi_key] = 1



    roiInfo_lookup = {}
    for neuron_roi_key in all_neuron_roi_keys:
        nr_data = neuron_roi_key.split("_")
        bodyID = nr_data[0]
        roiName = nr_data[1]
        if roiName != "":
            neuron_roiInfo_counts = {}
            neuron_roi_pre_count = 0
            neuron_roi_post_count = 0
            if neuron_roi_key in neuron_roi_pre:
                neuron_roi_pre_count = neuron_roi_pre[neuron_roi_key]
                neuron_roiInfo_counts["pre"] = int(neuron_roi_pre_count)

            if neuron_roi_key in neuron_roi_post:
                neuron_roi_post_count = neuron_roi_post[neuron_roi_key]
                neuron_roiInfo_counts["post"] = neuron_roi_post_count

            if bodyID in roiInfo_lookup:
                neuron_roiInfo_dict = roiInfo_lookup[bodyID]
                neuron_roiInfo_dict[roiName] = neuron_roiInfo_counts                
            else:
                neuron_roiInfo_dict = {}
                neuron_roiInfo_dict[roiName] = neuron_roiInfo_counts
                roiInfo_lookup[bodyID] = neuron_roiInfo_dict

    
    for bodyID in all_bodies_syn:
        pre_count = 0
        post_count = 0
        body_roiInfo_str = ""
        
        if bodyID in bodies_pre:
            pre_count = bodies_pre[bodyID]
        if bodyID in bodies_post:
            post_count = bodies_post[bodyID]

        if bodyID in roiInfo_lookup:
            body_roiInfo = roiInfo_lookup[bodyID]
            body_roiInfo_str_tmp = json.dumps(body_roiInfo)
            body_roiInfo_str = body_roiInfo_str_tmp.replace("|",",")

        print(bodyID + ";" + str(pre_count) + ";" + str(post_count) + ";" + body_roiInfo_str)

