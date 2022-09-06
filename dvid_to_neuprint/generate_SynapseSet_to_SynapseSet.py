#!/bin/env
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

    print(":START_ID(SynSet-ID),:END_ID(SynSet-ID)")
    for synSet in unique_set:
        connectToset = unique_set[synSet]
        print(synSet + "," + connectToset)
            

