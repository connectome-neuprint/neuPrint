#!/bin/env

# python generate_SynapseSet_to_ROI.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > Neuprint_SynapseSet_ROI_6f2cb.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import numpy as np
import pandas as pd

if __name__ == '__main__':
    synapses_connect_csv = sys.argv[1]

    #synapse_connect = {}
    #synapse_connect_hp = {}

    synapse_sets = {}

    HP_cuttoff = 0.5

    all_synSet_ROI = {}
    synSet_ROI_weight = {}
    synSet_ROI_weightHP = {}

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
                synSet_ID_super_pre = from_bodyId + "_" + to_bodyId + "_pre-" + from_super_roi
                synSet_ID_super_post = to_bodyId + "_" + from_bodyId + "_post-" + to_super_roi
                synSet_ID_sub1_pre = from_bodyId + "_" + to_bodyId + "_pre-" + from_sub1_roi
                synSet_ID_sub1_post = to_bodyId + "_" + from_bodyId + "_post-" + to_sub1_roi
                synSet_ID_sub2_pre = from_bodyId + "_" + to_bodyId + "_pre-" + from_sub1_roi
                synSet_ID_sub2_post = to_bodyId + "_" + from_bodyId + "_post-" + to_sub1_roi
                synSet_ID_sub3_pre = from_bodyId + "_" + to_bodyId + "_pre-" + from_sub3_roi
                synSet_ID_sub3_post = to_bodyId + "_" + from_bodyId + "_post-" + to_sub3_roi
                

                all_synSet_ROI[synSet_ID_super_pre] = 1
                all_synSet_ROI[synSet_ID_super_post] = 1
                all_synSet_ROI[synSet_ID_sub1_pre] = 1
                all_synSet_ROI[synSet_ID_sub1_post] = 1
                all_synSet_ROI[synSet_ID_sub2_pre] = 1
                all_synSet_ROI[synSet_ID_sub2_post] = 1
                all_synSet_ROI[synSet_ID_sub3_pre] = 1
                all_synSet_ROI[synSet_ID_sub3_post] = 1

                if synSet_ID_super_pre in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_super_pre] += 1
                else:
                    synSet_ROI_weight[synSet_ID_super_pre] = 1
                if synSet_ID_super_post in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_super_post] += 1
                else:
                    synSet_ROI_weight[synSet_ID_super_post] = 1

                if synSet_ID_sub1_pre in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_sub1_pre] += 1
                else:
                    synSet_ROI_weight[synSet_ID_sub1_pre] = 1
                if synSet_ID_sub1_post in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_sub1_post] += 1
                else:
                    synSet_ROI_weight[synSet_ID_sub1_post] = 1

                if synSet_ID_sub2_pre in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_sub2_pre] += 1
                else:
                    synSet_ROI_weight[synSet_ID_sub2_pre] = 1
                if synSet_ID_sub2_post in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_sub2_post] += 1
                else:
                    synSet_ROI_weight[synSet_ID_sub2_post] = 1

                if synSet_ID_sub3_pre in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_sub3_pre] += 1
                else:
                    synSet_ROI_weight[synSet_ID_sub3_pre] = 1
                if synSet_ID_sub3_post in synSet_ROI_weight:
                    synSet_ROI_weight[synSet_ID_sub3_post] += 1
                else:
                    synSet_ROI_weight[synSet_ID_sub3_post] = 1



                if from_conf > 0.5:
                    if to_conf > 0.5:
                        if synSet_ID_super_pre in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_super_pre] += 1                            
                        else:
                            synSet_ROI_weightHP[synSet_ID_super_pre] = 1
                        if synSet_ID_super_post in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_super_post] += 1
                        else:
                            synSet_ROI_weightHP[synSet_ID_super_post] = 1

                        if synSet_ID_sub1_pre in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_sub1_pre] += 1
                        else:
                            synSet_ROI_weightHP[synSet_ID_sub1_pre] = 1
                        if synSet_ID_sub1_post in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_sub1_post] += 1
                        else:
                            synSet_ROI_weightHP[synSet_ID_sub1_post] = 1
                            
                        if synSet_ID_sub2_pre in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_sub2_pre] += 1
                        else:
                            synSet_ROI_weightHP[synSet_ID_sub2_pre] = 1
                        if synSet_ID_sub2_post in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_sub2_post] += 1
                        else:
                            synSet_ROI_weightHP[synSet_ID_sub2_post] = 1

                        if synSet_ID_sub3_pre in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_sub3_pre] += 1
                        else:
                            synSet_ROI_weightHP[synSet_ID_sub3_pre] = 1
                        if synSet_ID_sub3_post in synSet_ROI_weightHP:
                            synSet_ROI_weightHP[synSet_ID_sub3_post] += 1
                        else:
                            synSet_ROI_weightHP[synSet_ID_sub3_post] = 1


    print(":START_ID,weight:int,weightHP:int,:END_ID")
    for synSet_ROI in all_synSet_ROI:
        weight = 0
        weightHP = 0
        if synSet_ROI in synSet_ROI_weight:
            weight = synSet_ROI_weight[synSet_ROI]

        if synSet_ROI in synSet_ROI_weightHP:
            weightHP = synSet_ROI_weightHP[synSet_ROI]

        set_data = synSet_ROI.split('-')

        setSetID = set_data[0]
        roi_name = set_data[1]
        if len(roi_name) > 0:
            print(setSetID + "," + str(weight) + "," + str(weightHP) + "," + roi_name)
