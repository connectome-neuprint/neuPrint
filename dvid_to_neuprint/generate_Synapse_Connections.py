#!/bin/env

# python generate_Synapse_Connections.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > Neuprint_Synapse_Connections_6f2cb.csv
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

    synapseList = open(synapses_connect_csv,'r')

    print(":START_ID(Syn-ID),:END_ID(Syn-ID)")
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synConnectData = data_str.split(',')
            
            #from_synId,from_x,from_y,from_z,from_conf,from_super_roi,from_sub1_roi,from_sub2_roi,from_sub3_roi,from_bodyId,connection,to_synId,to_x,to_y,to_z,to_conf,to_super_roi,to_sub1_roi,to_sub2_roi,to_sub3_roi,to_bodyId
            from_synId = synConnectData[0]
            connect_type = synConnectData[10]
            to_synId = synConnectData[11]
            if connect_type == "PreSynTo":
                print(from_synId + "," + to_synId )
