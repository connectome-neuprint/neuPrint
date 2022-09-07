#!/bin/env
# ------------------------- imports -------------------------
import sys
import os

if __name__ == '__main__':
    synapse_body_map_file = sys.argv[1]

    synCount = 0
    print("synId,x,y,z,type,confidence,toplevel_roi,sub1_roi,sub2_roi,sub3_roi,body")
    synapseBodyMapList = open(synapse_body_map_file,'r')
    for line in synapseBodyMapList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synapseBodyMapData = data_str.split(',')
            if int(synapseBodyMapData[10]) != 0:
                print(data_str)
