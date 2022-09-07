#!/bin/env
# ------------------------- imports -------------------------
import sys
import os

if __name__ == '__main__':
    synapse_bodies_file = sys.argv[1]

    synapseBodiesList = open(synapse_bodies_file,'r')
    for line in synapseBodiesList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synapseBodiesData = data_str.split(';')
            print(synapseBodiesData[0])
