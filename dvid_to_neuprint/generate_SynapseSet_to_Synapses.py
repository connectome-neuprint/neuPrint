#!/bin/env

# python generate_SynapseSet_to_Synapses.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > Neuprint_SynapseSet_to_Synapses_6f2cb.csv
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
    
    synapse_sets = {}
    
    synapseList = open(synapses_connect_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synConnectData = data_str.split(',')
            #from_synId, from_x, from_y, from_z, from_conf, from_roi, from_bodyId, connection, to_synId, to_x, to_y, to_z, to_conf, to_roi, to_bodyId
            from_synID = synConnectData[0]            
            from_conf =  float(synConnectData[4])
            from_bodyId = synConnectData[9]
            connect_type = synConnectData[10]
            to_synID = synConnectData[11]
            to_conf = float(synConnectData[15])
            to_bodyId = synConnectData[20]

            
            if connect_type == "PreSynTo":
                synSet_ID1 = from_bodyId + "_" + to_bodyId + "_pre" 
                synSet_ID2 = to_bodyId + "_" + from_bodyId + "_post"

                if synSet_ID1 in synapse_sets:
                    synSet_synIDs = synapse_sets[synSet_ID1]
                    synSet_synIDs[from_synID] = 1
                else:
                    synSet_synIDs_new = {}
                    synSet_synIDs_new[from_synID] = 1
                    synapse_sets[synSet_ID1] = synSet_synIDs_new

                if synSet_ID2 in synapse_sets:
                    synSet_synIDs = synapse_sets[synSet_ID2]
                    synSet_synIDs[to_synID] = 1
                else:
                    synSet_synIDs_new = {}
                    synSet_synIDs_new[to_synID] = 1
                    synapse_sets[synSet_ID2] = synSet_synIDs_new


    print (":START_ID,:END_ID(Syn-ID)")
    for synapse_set_id in synapse_sets:
        synSet_synIDs = synapse_sets[synapse_set_id]
        for syn_Id in synSet_synIDs:
            print(synapse_set_id + "," + syn_Id)

