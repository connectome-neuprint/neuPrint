#!/bin/env

# python generate_Neuron_to_SynaseSet.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > Neuprint_Neuron_to_SynapseSet_6f2cb.csv
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

    neuron_synapse_sets = {}

    HP_cuttoff = 0.5
    
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
                #dict_key = from_bodyId + ":" + to_bodyId
                synSet_ID1 = from_bodyId + "_" + to_bodyId + "_pre"
                synSet_ID2 = to_bodyId + "_" + from_bodyId + "_post"

                pre_synSet = from_bodyId + ":" + synSet_ID1
                post_synSet = to_bodyId + ":" + synSet_ID2
                neuron_synapse_sets[pre_synSet] = 1
                neuron_synapse_sets[post_synSet] = 1


    print(":START_ID(Body-ID),:END_ID")
    for neuron_synSet_key in sorted(neuron_synapse_sets):
        neuron_synSet_data = neuron_synSet_key.split(':')
        bodyId = neuron_synSet_data[0]
        synSet = neuron_synSet_data[1]
        print(bodyId + "," + synSet)


