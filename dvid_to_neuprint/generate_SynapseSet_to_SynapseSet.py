#!/bin/env

# python generate_SynapseSet_to_SynapseSet.py Sorted_All_Neuprint_Synapse_Connections_6f2cb.csv > Neuprint_SynapseSet_to_SynapseSet_6f2cb.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import numpy as np
from tqdm import trange
from neuclease.dvid import *
from libdvid import DVIDNodeService, ConnectionMethod

if __name__ == '__main__':
    synapseSet_csv = sys.argv[1]

    synapseSets = open(synapseSet_csv,'r')
    unique_set = {}
    for line in synapseSets:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            data = data_str.split(",")
            if data[10] == "PreSynTo":
                pre_syn_set = data[9] + "_" + data[20] + "_pre"
                post_syn_set = data[20] + "_" + data[9] + "_post"
                #print(pre_syn_set)
                unique_set[pre_syn_set] = post_syn_set

    print(":START_ID,:END_ID")
    for synSet in unique_set:
        connectToset = unique_set[synSet]
        #bodies = synSet.split("_")
        #reverse_set = bodies[1] + "_" + bodies[0]
        #if synSet == reverse_set:
            #print("Error", synSet, reverse_set, "same")
        #    continue
        #else:
        print(synSet + "," + connectToset)
            

