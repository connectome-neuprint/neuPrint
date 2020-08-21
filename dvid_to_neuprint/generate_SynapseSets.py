#!/bin/env

# python generate_SynapseSets.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv hemibrain > Neuprint_SynapseSet_6f2cb.csv
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
    dataset = sys.argv[2]

    synapse_connect = {}
    synapse_connect_hp = {}
    #neuron_conn_roi_pre = {}
    #neuron_conn_roi_post = {}
    neuron_conn_roi ={}
    HP_cuttoff = 0.5
    all_neuron_conn_roi_keys = {}
    all_synSet = {}

    synapseList = open(synapses_connect_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synConnectData = data_str.split(',')            
            #from_synId,from_x,from_y,from_z,from_conf,from_roi,from_bodyId,connection,to_synId,to_x,to_y,to_z,to_conf,to_roi,to_bodyId
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
                dict_key_pre = from_bodyId + "_" + to_bodyId + "_pre"
                all_synSet[dict_key_pre] = 1
                dict_key_post = to_bodyId + "_" + from_bodyId + "_post"
                all_synSet[dict_key_post] = 1
                                
                if dict_key_pre in synapse_connect:
                    synapse_connect[dict_key_pre] += 1
                else:
                    synapse_connect[dict_key_pre] = 1

                if dict_key_post in synapse_connect:
                    synapse_connect[dict_key_post] += 1
                else:
                    synapse_connect[dict_key_post] = 1


    track_synapseSet = {}
    print (":ID,:Label")
    for connect_key in synapse_connect:
        if connect_key in track_synapseSet:
            continue
        else:
            track_synapseSet[connect_key] = 1
            print(connect_key + ",SynapseSet;" + dataset + "_SynapseSet")

