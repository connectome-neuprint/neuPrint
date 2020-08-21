#!/bin/env

# python detect_downstream_roiInfo.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > downstream_synapses_roiInfo.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
#import numpy as np
#import pandas as pd

if __name__ == '__main__':
    synapses_connect_csv = sys.argv[1]


    neuron_downstream_roi = {}
    neuron_upstream_roi = {}

    synapseList = open(synapses_connect_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synConnectData = data_str.split(',')
            #from_synId,from_x,from_y,from_z,from_conf,from_super_roi,from_sub1_roi,from_sub2_roi,from_sub3_roi,from_bodyId,connection,to_synId,to_x,to_y,to_z,to_conf,to_super_roi,to_sub1_roi,to_sub2_roi,to_sub3_roi,to_bodyId
            from_synID = synConnectData[0]
            from_conf =  float(synConnectData[4])
            from_super_roi = synConnectData[5]
            from_sub1_roi = synConnectData[6]
            from_sub2_roi = synConnectData[7]
            from_sub3_roi = synConnectData[8]
            from_bodyId = synConnectData[9]
            connect_type = synConnectData[10]
            to_conf = float(synConnectData[15])
            to_super_roi = synConnectData[16]
            to_sub1_roi = synConnectData[17]
            to_sub2_roi = synConnectData[18]
            to_sub3_roi =synConnectData[19]
            to_bodyId = synConnectData[20]


            if connect_type =="PreSynTo":
                if len(from_super_roi) > 0:
                    neuron_roi_0 = from_bodyId + "_" + from_super_roi
                    if neuron_roi_0 in neuron_downstream_roi:
                        neuron_downstream_roi[neuron_roi_0] += 1
                    else:
                        neuron_downstream_roi[neuron_roi_0] = 1
                if len(from_sub1_roi) > 0:
                    neuron_roi_1 = from_bodyId + "_" + from_sub1_roi
                    if neuron_roi_1 in neuron_downstream_roi:
                        neuron_downstream_roi[neuron_roi_1] += 1
                    else:
                        neuron_downstream_roi[neuron_roi_1] = 1 
                if len(from_sub2_roi) > 0:
                    neuron_roi_2 = from_bodyId + "_" + from_sub2_roi
                    if neuron_roi_2 in neuron_downstream_roi:
                        neuron_downstream_roi[neuron_roi_2] += 1
                    else:
                        neuron_downstream_roi[neuron_roi_2] = 1
                if len(from_sub3_roi) > 0:
                    neuron_roi_3 = from_bodyId + "_" + from_sub3_roi
                    if neuron_roi_3 in neuron_downstream_roi:
                        neuron_downstream_roi[neuron_roi_3] += 1
                    else:
                        neuron_downstream_roi[neuron_roi_3] = 1

 #           elif connect_type =="PostSynTo":
 #               if len(from_super_roi) > 0:
 #                   neuron_roi_0 = from_bodyId + "_" + from_super_roi
 #                   if neuron_roi_0 in neuron_upstream_roi:
 #                       neuron_upstream_roi[neuron_roi_0] += 1
 #                   else:
 #                       neuron_upstream_roi[neuron_roi_0] = 1
 #               if len(from_sub1_roi) > 0:
 #                   neuron_roi_1 = from_bodyId + "_" + from_sub1_roi
 #                   if neuron_roi_1 in neuron_upstream_roi:
 #                       neuron_upstream_roi[neuron_roi_1] += 1
 #                   else:
 #                       neuron_upstream_roi[neuron_roi_1] = 1
 #               if len(from_sub2_roi) > 0:
 #                   neuron_roi_2 = from_bodyId + "_" + from_sub2_roi
 #                   if neuron_roi_2 in neuron_upstream_roi:
 #                       neuron_upstream_roi[neuron_roi_2] += 1
 #                   else:
 #                       neuron_upstream_roi[neuron_roi_2] = 1
 #               if len(from_sub3_roi) > 0:
 #                   neuron_roi_3 = from_bodyId + "_" + from_sub3_roi
 #                   if neuron_roi_3 in neuron_upstream_roi:
 #                       neuron_upstream_roi[neuron_roi_3] += 1
 #                   else:
 #                       neuron_upstream_roi[neuron_roi_3] = 1

    neuron_ups_dns_roi = {}

    for neuron_roi_key in neuron_downstream_roi:
        downstream_num = neuron_downstream_roi[neuron_roi_key]
        neuron_roi_data = neuron_roi_key.split("_")
        bodyId = neuron_roi_data[0]
        roi = neuron_roi_data[1].replace("|",",")
        if bodyId in neuron_ups_dns_roi:
            ups_dns_roiInfo = neuron_ups_dns_roi[bodyId]
            if roi in ups_dns_roiInfo:
                ups_dns_dict = ups_dns_roiInfo[roi]
                ups_dns_dict["downstream"] = int(downstream_num)
            else:
                ups_dns_dict = {}
                ups_dns_dict["downstream"] = int(downstream_num)
                ups_dns_roiInfo[roi] = ups_dns_dict
        else:
            ups_dns_roiInfo = {}
            ups_dns_dict = {}
            ups_dns_dict["downstream"] = int(downstream_num)
            ups_dns_roiInfo[roi] = ups_dns_dict
            neuron_ups_dns_roi[bodyId] = ups_dns_roiInfo

#    for neuron_roi_key in neuron_upstream:
#        upstream_num = neuron_upstream[neuron_roi_key]
#        neuron_roi_data = neuron_roi_key.split("_")
#        bodyId = neuron_roi_data[0]
#        roi = neuron_roi_data[1]
#        if bodyId in neuron_ups_dns_roi:
#            ups_dns_roiInfo = neuron_ups_dns_roi[bodyId]
#            if roi in ups_dns_roiInfo:
#                ups_dns_dict = ups_dns_roiInfo[roi]
#                ups_dns_dict["upstream"] = int(upstream_num)
#            else:
#                ups_dns_dict = {}
#                ups_dns_dict["upstream"] = int(upstream_num)
#                ups_dns_roiInfo[roi] = ups_dns_dict
#        else:
#            ups_dns_roiInfo = {}
#            ups_dns_dict = {}
#            ups_dns_dict["upstream"] = int(upstream_num)
#            ups_dns_roiInfo[roi] = ups_dns_dict
#            neuron_ups_dns_roi[bodyId] = ups_dns_roiInfo


    for bodyId in neuron_ups_dns_roi:
        ups_dns_roiInfo = neuron_ups_dns_roi[bodyId]
        ups_dns_roiInfo_str = json.dumps(ups_dns_roiInfo)
        print(bodyId + ";" + ups_dns_roiInfo_str)
