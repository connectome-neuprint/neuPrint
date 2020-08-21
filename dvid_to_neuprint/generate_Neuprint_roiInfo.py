#!/bin/env

# Use python 3.6 or greater
# Script that counts the number of pre and post synapses in each mapped ROI

# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import re

if __name__ == '__main__':
    synapses_csv = sys.argv[1]
    
    #header = ":START_ID,pre:int,post:int"
    #print(header)

    all_rois = {}
    rois_pre_count = {}
    rois_post_count = {}
    tot_pre_count = 0
    tot_post_count = 0

    #id_count = 0
    synapseList = open(synapses_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synData = data_str.split(',')
            #x,y,z,type,confidence,super_roi,sub_roi
            syn_id = synData[0]
            x = synData[1]
            y = synData[2]
            z = synData[3]
            syn_type_dvid = synData[4]
            confidence = synData[5]
            super_roi = synData[6]
            sub1_roi = synData[7]
            sub2_roi = synData[8].replace("|",",")
            sub3_roi = synData[9]

            all_rois[super_roi] = 1
            all_rois[sub1_roi] = 1
            all_rois[sub2_roi] = 1
            all_rois[sub3_roi] = 1

            if syn_type_dvid == "PostSyn":
                syn_type = "post"
                tot_post_count += 1
                if super_roi in rois_post_count:
                    rois_post_count[super_roi] += 1
                else:
                    rois_post_count[super_roi] = 1

                if sub1_roi in rois_post_count:
                    rois_post_count[sub1_roi] += 1
                else:
                    rois_post_count[sub1_roi] = 1
                
                if sub2_roi in rois_post_count:
                    rois_post_count[sub2_roi] += 1
                else:
                    rois_post_count[sub2_roi] = 1

                if sub3_roi in rois_post_count:
                    rois_post_count[sub3_roi] += 1
                else:
                    rois_post_count[sub3_roi] = 1

            elif syn_type_dvid == "PreSyn":
                syn_type = "pre"
                tot_pre_count += 1
                if super_roi in rois_pre_count:
                    rois_pre_count[super_roi] += 1
                else:
                    rois_pre_count[super_roi] = 1

                if sub1_roi in rois_pre_count:
                    rois_pre_count[sub1_roi] += 1
                else:
                    rois_pre_count[sub1_roi] = 1

                if sub2_roi in rois_pre_count:
                    rois_pre_count[sub2_roi] += 1
                else:
                    rois_pre_count[sub2_roi] = 1

                if sub3_roi in rois_pre_count:
                    rois_pre_count[sub3_roi] += 1
                else:
                    rois_pre_count[sub3_roi] = 1
    
    roiInfo = {}
    for roi in sorted (all_rois.keys()):
        pre_count = 0
        post_count = 0
        if roi in rois_pre_count:
            pre_count = rois_pre_count[roi]
        if roi in rois_post_count:
            post_count = rois_post_count[roi]
        #print(roi + "," + str(pre_count) + "," + str(post_count))
        if roi != "":
            syn_counts = {}
            syn_counts["pre"] = pre_count
            syn_counts["post"] = post_count 
            roiInfo[roi] = syn_counts

    roiInfo_str = json.dumps(roiInfo)

    print (roiInfo_str)

    #roi_desc = json.loads(open("/groups/flyem/home/flyem/bin/create_neuprint_imports/np_roiInfo.json", 'rt').read())

    #for roiName in roi_desc:
    #    roi_desc_data = roi_desc[roiName]
    #    if roiName in roiInfo:
    #        syn_counts = roiInfo[roiName]
    #        if "pre" in syn_counts:
    #            roi_desc_data["pre"] = syn_counts["pre"]
    #        if "post" in syn_counts:
    #            roi_desc_data["post"] = syn_counts["post"]
    #    if roiName == "hemibrain":
    #        roi_desc_data["pre"] = tot_pre_count
    #        roi_desc_data["post"] = tot_post_count

    #roiInfo_tmp = json.dumps(roi_desc)
    #prbint(roiInfo_tmp)
    sys.exit()
