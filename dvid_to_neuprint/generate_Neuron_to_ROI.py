#!/bin/env

# python generate_Neuron_to_ROI.py synIDs_synapses-6f2cb-rois-bodyIDs.csv > Neuprint_Neuron_ROI_6f2cb.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import numpy as np
import pandas as pd

if __name__ == '__main__':
    synapses_csv = sys.argv[1]
    
    all_body_rois = {}
    body_roi_pre = {}
    body_roi_post = {}

    synapseList = open(synapses_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synData = data_str.split(',')
            synID = synData[0]
            x = synData[1]
            y = synData[2]
            z = synData[3]
            neuron_type = synData[4]
            confidence = synData[5]
            super_roi = synData[6]
            sub1_roi = synData[7]
            sub2_roi = synData[8]
            sub3_roi = synData[9]
            bodyID = synData[10]
            
            if len(super_roi) > 0:
                body_roi_key = bodyID + "_" + super_roi
                if body_roi_key not in all_body_rois:
                    all_body_rois[body_roi_key] = 1
                if neuron_type == "PreSyn":
                    if body_roi_key in body_roi_pre:
                        body_roi_pre[body_roi_key] += 1
                    else:
                        body_roi_pre[body_roi_key] = 1
                if neuron_type == "PostSyn":
                    if body_roi_key in body_roi_post:
                        body_roi_post[body_roi_key] += 1
                    else:
                        body_roi_post[body_roi_key] = 1

            if len(sub1_roi) > 0:
                body_roi_key = bodyID + "_" + sub1_roi
                if body_roi_key not in all_body_rois:
                    all_body_rois[body_roi_key] = 1
                if neuron_type == "PreSyn":
                    if body_roi_key in body_roi_pre:
                        body_roi_pre[body_roi_key] += 1
                    else:
                        body_roi_pre[body_roi_key] = 1
                if neuron_type == "PostSyn":
                    if body_roi_key in body_roi_post:
                        body_roi_post[body_roi_key] += 1
                    else:
                        body_roi_post[body_roi_key] = 1

            if len(sub2_roi) > 0:
                body_roi_key = bodyID + "_" + sub2_roi
                if body_roi_key not in all_body_rois:
                    all_body_rois[body_roi_key] = 1
                if neuron_type == "PreSyn":
                    if body_roi_key in body_roi_pre:
                        body_roi_pre[body_roi_key] += 1
                    else:
                        body_roi_pre[body_roi_key] = 1
                if neuron_type == "PostSyn":
                    if body_roi_key in body_roi_post:
                        body_roi_post[body_roi_key] += 1
                    else:
                        body_roi_post[body_roi_key] = 1

            if len(sub3_roi) > 0:
                body_roi_key = bodyID + "_" + sub3_roi
                if body_roi_key not in all_body_rois:
                    all_body_rois[body_roi_key] = 1
                if neuron_type == "PreSyn":
                    if body_roi_key in body_roi_pre:
                        body_roi_pre[body_roi_key] += 1
                    else:
                        body_roi_pre[body_roi_key] = 1
                if neuron_type == "PostSyn":
                    if body_roi_key in body_roi_post:
                        body_roi_post[body_roi_key] += 1
                    else:
                        body_roi_post[body_roi_key] = 1

    print(":START_ID(Body-ID),pre:int,post:int,:END_ID")
    for body_roi_key in all_body_rois:
        pre_count = 0
        if body_roi_key in body_roi_pre:
            pre_count = body_roi_pre[body_roi_key]
        post_count = 0
        if body_roi_key in body_roi_post:
            post_count = body_roi_post[body_roi_key]
    
        key_data = body_roi_key.split('_')
        bodyID = key_data[0]
        ROI_name = key_data[1]
        print(bodyID + "," + str(pre_count) + "," + str(post_count) + "," + ROI_name)
        
