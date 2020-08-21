#!/bin/env

# python generate_Neuron_connections.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > Neuprint_Neuron_Connections_6f2cb.csv
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
    
    synapse_connect = {}
    synapse_connect_hp = {}
    syn_connect_pre = []
    syn_connect_post = []
    neuron_conn_roi_pre = {}
    neuron_conn_roi_post = {}
    HP_cuttoff = 0.7
    all_neuron_conn_roi_keys = {}
    
    synapseList = open(synapses_connect_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synConnectData = data_str.split(',')            
            #from_synId,from_x,from_y,from_z,from_conf,from_super_roi,from_sub1_roi,from_sub2_roi,from_sub3_roi,from_bodyId,connection,to_synId,to_x,to_y,to_z,to_conf,to_super_roi,to_sub1_roi,to_sub2_roi,to_sub3_roi,to_bodyId
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

                        
            if connect_type == "PreSynTo":
                dict_key = from_bodyId + ":" + to_bodyId
                dict_key_from_super_roi = dict_key + "_" + from_super_roi
                dict_key_from_sub1_roi = dict_key + "_" + from_sub1_roi
                dict_key_from_sub2_roi = dict_key + "_" + from_sub2_roi
                dict_key_from_sub3_roi = dict_key + "_" + from_sub3_roi
                dict_key_to_super_roi = dict_key + "_" + to_super_roi
                dict_key_to_sub1_roi = dict_key + "_" + to_sub1_roi
                dict_key_to_sub2_roi = dict_key + "_" + to_sub2_roi
                dict_key_to_sub3_roi = dict_key + "_" + to_sub3_roi

                all_neuron_conn_roi_keys[dict_key_from_super_roi] = 1
                all_neuron_conn_roi_keys[dict_key_from_sub1_roi] = 1
                all_neuron_conn_roi_keys[dict_key_from_sub2_roi] = 1
                all_neuron_conn_roi_keys[dict_key_from_sub3_roi] = 1
                all_neuron_conn_roi_keys[dict_key_to_super_roi] = 1
                all_neuron_conn_roi_keys[dict_key_to_sub1_roi] = 1
                all_neuron_conn_roi_keys[dict_key_to_sub2_roi] = 1
                all_neuron_conn_roi_keys[dict_key_to_sub3_roi] = 1
                
                if dict_key_from_super_roi in neuron_conn_roi_pre:
                    neuron_conn_roi_pre[dict_key_from_super_roi] += 1
                else:
                    neuron_conn_roi_pre[dict_key_from_super_roi] = 1

                if dict_key_from_sub1_roi in neuron_conn_roi_pre:
                    neuron_conn_roi_pre[dict_key_from_sub1_roi] += 1
                else:
                    neuron_conn_roi_pre[dict_key_from_sub1_roi] = 1

                if dict_key_from_sub2_roi in neuron_conn_roi_pre:
                    neuron_conn_roi_pre[dict_key_from_sub2_roi] += 1
                else:
                    neuron_conn_roi_pre[dict_key_from_sub2_roi] = 1

                if dict_key_from_sub3_roi in neuron_conn_roi_pre:
                    neuron_conn_roi_pre[dict_key_from_sub3_roi] += 1
                else:
                    neuron_conn_roi_pre[dict_key_from_sub3_roi] = 1
                ###################
                if dict_key_to_super_roi in neuron_conn_roi_post:
                    neuron_conn_roi_post[dict_key_to_super_roi] += 1
                else:
                    neuron_conn_roi_post[dict_key_to_super_roi] = 1

                if dict_key_to_sub1_roi in neuron_conn_roi_post:
                    neuron_conn_roi_post[dict_key_to_sub1_roi] += 1
                else:
                    neuron_conn_roi_post[dict_key_to_sub1_roi] = 1

                if dict_key_to_sub2_roi in neuron_conn_roi_post:
                    neuron_conn_roi_post[dict_key_to_sub2_roi] += 1
                else:
                    neuron_conn_roi_post[dict_key_to_sub2_roi] = 1

                if dict_key_to_sub3_roi in neuron_conn_roi_post:
                    neuron_conn_roi_post[dict_key_to_sub3_roi] += 1
                else:
                    neuron_conn_roi_post[dict_key_to_sub3_roi] = 1

                #if dict_key in syn_connect_pre:
                #    syn_connect_pre[dict_key] += 1
                #else:
                #    syn_connect_pre[dict_key] = 1

                #if dict_key in syn_connect_post:
                #    syn_connect_post[dict_key] += 1
                #else:
                #    syn_connect_post[dict_key] = 1

                if dict_key in synapse_connect:
                    synapse_connect[dict_key] += 1
                else:
                    synapse_connect[dict_key] = 1
                
                if from_conf > HP_cuttoff:
                    if to_conf > HP_cuttoff:
                        if dict_key in synapse_connect_hp:
                            synapse_connect_hp[dict_key] += 1
                        else:
                            synapse_connect_hp[dict_key] = 1
            #elif connect_type == "PostSynTo":
            #    dict_key = from_bodyId + ":" + to_bodyId
            #    continue

    roiInfo_lookup = {}
    for neuron_conn_roi_key in all_neuron_conn_roi_keys:
        neuron_conn_roi_data = neuron_conn_roi_key.split("_")
        neuron_conn_id = neuron_conn_roi_data[0]
        roiName = neuron_conn_roi_data[1]
        if roiName == "":
            continue

        neuron_roiInfo_counts = {}
        if neuron_conn_roi_key in neuron_conn_roi_pre:
            neuron_conn_roi_pre_count = neuron_conn_roi_pre[neuron_conn_roi_key]
            neuron_roiInfo_counts["pre"] = int(neuron_conn_roi_pre_count)
        if neuron_conn_roi_key in neuron_conn_roi_post:
            neuron_conn_roi_post_count = neuron_conn_roi_post[neuron_conn_roi_key]
            neuron_roiInfo_counts["post"] = int(neuron_conn_roi_post_count)

        if neuron_conn_id in roiInfo_lookup:
            neuron_roiInfo_dict = roiInfo_lookup[neuron_conn_id]
            neuron_roiInfo_dict[roiName] = neuron_roiInfo_counts
        else:
            neuron_roiInfo_dict = {}
            neuron_roiInfo_dict[roiName] = neuron_roiInfo_counts
            roiInfo_lookup[neuron_conn_id] = neuron_roiInfo_dict

    #superLevelrois
    #superLevelrois = json.loads(open("superLevelROIs.json", 'rt').read())
    
    #superLevelrois = ["ME(R)","AME(R)","LO(R)","LOP(R)","CA(R)","CA(L)","PED(R)","a'L(R)","a'L(L)","aL(R)","aL(L)","gL(R)","gL(L)","b'L(R)","b'L(L)","bL(R)","bL(L)","FB","AB(R)","AB(L)","EB","PB","NO", "BU(R)","BU(L)","LAL(R)","LAL(L)","AOTU(R)","AVLP(R)","PVLP(R)","PLP(R)","WED(R)","LH(R)","SLP(R)","SIP(R)","SIP(L)","SMP(R)","SMP(L)","CRE(R)","CRE(L)","ROB(R)","SCL(R)","SCL(L)","ICL(R)","ICL(L)","IB","ATL(R)","ATL(L)","AL(R)","AL(L)","VES(R)","VES(L)","EPA(R)","EPA(L)","GOR(R)","GOR(L)","SPS(R)","SPS(L)","IPS(R)","SAD","FLA(R)","CAN(R)","PRW","GNG"]

    print (":START_ID(Body-ID),weight:int,weightHP:int,:END_ID(Body-ID),roiInfo:string")
    for connect_key in synapse_connect:
        connect_weight = synapse_connect[connect_key]
        none_pre = connect_weight
        none_post = connect_weight
        roiInfo = '{}'
                
        if connect_key in roiInfo_lookup:
            roiInfoJson = roiInfo_lookup[connect_key]
            roiInfoTmp = json.dumps(roiInfoJson)
            roiInfoTmp2 = roiInfoTmp.replace("|",",")
            roiInfo = roiInfoTmp2.replace('"','""')
            
        bodyData = connect_key.split(':')
        from_bodyID = bodyData[0]
        to_bodyID = bodyData[1]

        
        connect_weightHP = 0
        if connect_key in synapse_connect_hp:
            connect_weightHP = synapse_connect_hp[connect_key]
        print(from_bodyID + "," + str(connect_weight) + "," + str(connect_weightHP) + "," + to_bodyID + ",\"" + roiInfo + "\"" )
