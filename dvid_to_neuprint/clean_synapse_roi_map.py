#!/bin/env
# ------------------------- imports -------------------------
import sys
import os

if __name__ == '__main__':
    synapse_body_map_file = sys.argv[1]

    synCount = 0
    print("synId,x,y,z,type,confidence,toplevel_roi,sub1_roi,sub2_roi,sub3_roi")
    synapseBodyMapList = open(synapse_body_map_file,'r')
    for line in synapseBodyMapList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            data_str1 = data_str.replace("<unspecified>","")
            synapseBodyMapData = data_str1.split(',')
            synCount += 1
            synId = 99000000000 + synCount
            print(str(synId) + "," + synapseBodyMapData[0]  + "," + synapseBodyMapData[1]  + "," + synapseBodyMapData[2]  + "," + synapseBodyMapData[3]  + "," + synapseBodyMapData[4]  + "," + synapseBodyMapData[7] + ",,,")

