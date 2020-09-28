#!/bin/env

# python generate_Neuprint_Meta_csv.py synIDs_synapses-6f2cb-rois-bodyIDs.csv 6f2cb9f1d4514d64bf4bd788106ad8ab 1009257516 "2020-08-07 23:37:41" > Neuprint_Meta_6f2cb.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import re

if __name__ == '__main__':
    synapses_csv = sys.argv[1]
    uuid = sys.argv[2]
    dataset = sys.argv[3]

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

    #roi descriptions
    roi_desc = json.loads(open("np_roiInfo.json", 'rt').read())

    for roiName in roi_desc:
        roi_desc_data = roi_desc[roiName]
        if roiName in roiInfo:
            syn_counts = roiInfo[roiName]
            if "pre" in syn_counts:
                roi_desc_data["pre"] = syn_counts["pre"]
            if "post" in syn_counts:
                roi_desc_data["post"] = syn_counts["post"]
        if roiName == "hemibrain":
            roi_desc_data["pre"] = tot_pre_count
            roi_desc_data["post"] = tot_post_count

    roiInfo_tmp = json.dumps(roi_desc)

    #print(roiInfo_tmp)
    #sys.exit()

    roiInfo = roiInfo_tmp.replace('"','""')
    #roiInfo_tmp = json.dumps(roiInfo)
    #roiInfo = roiInfo_tmp.replace('"','""')
    
    #load roiHierarchy
    roi_hier = json.loads(open("np_roiHierarchy.json", 'rt').read())
    roi_hier_tmp = json.dumps(roi_hier)
    roiHier_str = roi_hier_tmp.replace('"','""')

    #load all the superlevel/toplevel ROIs
    superLevelrois = json.loads(open("superLevelROIs.json", 'rt').read())    
    superLevelrois_str = ""
    for slROI in superLevelrois:
        superLevelrois_str = superLevelrois_str + ';' + slROI    
    superLevelrois_str = re.sub(r'^;','',superLevelrois_str)

    #load all misc ROIs
    nonHierarchicalROIs = json.loads(open("nonHierarchicalROIs.json", 'rt').read())
    nonHierarchicalROIs_str = ""
    for nhROI in nonHierarchicalROIs:
        nonHierarchicalROIs_str = nonHierarchicalROIs_str + ';' + nhROI
    nonHierarchicalROIs_str = re.sub(r'^;','',nonHierarchicalROIs_str)

    postHighAccuracyThreshold = 0.5
    preHPThreshold = 0.0
    postHPThreshold = 0.7

    meshHost = "http://example.meshhost.org"

    #uuid = "a89eb3af216a46cdba81204d8f954786"

    neuroglancerInfo_tmp = '{"segmentation":{"host":"http://http://wasptrace.flatironinstitute.org/","uuid":"' + uuid + '","dataType":"labels"},"grayscale":{"host":"http://wasptrace.flatironinstitute.org/","uuid":"bfeeeb2b98bb4b2aa9b5e38256c6f1f1","dataType":"grayscalejpeg"}}'
    neuroglancerInfo = neuroglancerInfo_tmp.replace('"','""')

    NGloaded = json.loads(open("neuroglancer_meta_updated.json", 'rt').read())
    #NGloaded = json.loads(NGtmp)
    NGMeta = json.dumps(NGloaded)
    
    neuroglancerMeta = NGMeta.replace('"','""')

    statusDefinitions_tmp = '{"Roughly traced":"neuron high-level shape correct and validated by biological expert", "Prelim Roughly traced": "neuron high-level shape most likely correct or nearly complete, not yet validated by biological expert", "Anchor":"Big segment that has not been roughly traced", "0.5assign":"Segment fragment that is within the set required for a 0.5 connectome"}'
    statusDefinitions = statusDefinitions_tmp.replace('"','""')

    latestMutationId = sys.argv[4]

    totalPreCount = tot_pre_count
    totalPostCount = tot_post_count

    lastDatabaseEdit = sys.argv[5]
    
    logo = "https://www.janelia.org/sites/default/files/Project%20Teams/Fly%20EM/hemibrain_logo.png"
    info = "https://www.janelia.org/project-team/flyem/hemibrain"

    header = "voxelSize:float[],voxelUnits:string,info:string,logo:string,postHighAccuracyThreshold:float,preHPThreshold:float,postHPThreshold:float,meshHost:string,uuid:string,neuroglancerInfo:string,neuroglancerMeta:string,statusDefinitions:string,latestMutationId:int,totalPreCount:int,totalPostCount:int,lastDatabaseEdit:string,superLevelRois:string[],primaryRois:string[],nonHierarchicalROIs:string[],roiInfo:string,roiHierarchy:string,dataset:string,:LABEL"
    #print(roiInfo)
    print(header)
    print( '"8.0;8.0;8.0","nanometers",' + '"' + str(info) + '","' + str(logo) + '",' + str(postHighAccuracyThreshold) + ',' + str(preHPThreshold) + ',' + str(postHPThreshold) + ',"' + meshHost + '","' + uuid + '","' + neuroglancerInfo + '","' + neuroglancerMeta + '","' + statusDefinitions + '",' + str(latestMutationId) + ',' + str(totalPreCount) + ',' + str(totalPostCount) + ',"' + lastDatabaseEdit + '","' + superLevelrois_str + '","' + superLevelrois_str + '","' + nonHierarchicalROIs_str + '","' + roiInfo + '","' + roiHier_str + '",' + dataset + ',Meta;' + dataset + '_Meta' )

#    header = "tag:string,info:string,logo:string,postHighAccuracyThreshold:float,preHPThreshold:float,postHPThreshold:float,meshHost:string,uuid:string,neuroglancerInfo:string,neuroglancerMeta:string,statusDefinitions:string,latestMutationId:int,totalPreCount:int,totalPostCount:int,lastDatabaseEdit:string,superLevelRois:string[],primaryRois:string[],nonHierarchicalRois:string[],roiInfo:string,roiHierarchy:string,dataset:string,:LABEL"
#    print(header)
#    print( '"v1.0.1","' + str(info) + '","' + str(logo) + '",' + str(postHighAccuracyThreshold) + ',' + str(preHPThreshold) + ',' + str(postHPThreshold) + ',"' + meshHost + '","' + uuid + '","' + neuroglancerInfo + '","' + neuroglancerMeta + '","' + statusDefinitions + '",' + str(latestMutationId) + ',' + str(totalPreCount) + ',' + str(totalPostCount) + ',"' + lastDatabaseEdit + '","' + superLevelrois_str + '","' + superLevelrois_str + '","' + nonHierarchicalROIs_str +'","' + roiInfo + '","' + roiHier_str + '",hemibrain,Meta;hemibrain_Meta' )

#nonHierarchicalROIs_str
    
