#!/bin/env

# python detect_downstream_synapses.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > downstream_synapses.csv
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


    neuron_downstream = {}
    
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
                if from_bodyId in neuron_downstream:
                    neuron_downstream[from_bodyId] += 1
                else:
                    neuron_downstream[from_bodyId] = 1
                
    for bodyId in neuron_downstream:
        downstream_syn = neuron_downstream[bodyId]
        print(bodyId + "," + str(downstream_syn))
