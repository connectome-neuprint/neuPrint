#!/bin/env

# python generate_Synapse_Connections_All.py synIDs_synapses-6f2cb-rois-bodyIDs.csv synapses-dvid-ff278.json > All_Neuprint_Synapse_Connections_6f2cb.csv
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import numpy as np
import pandas as pd

if __name__ == '__main__':
    synapses_csv = sys.argv[1]
    synapse_connect_json = sys.argv[2]

    
    synapse_body_lookup = {}
    synapse_confidence = {}
    synapse_super_roi = {}
    synapse_sub1_roi = {}
    synapse_sub2_roi = {}
    synapse_sub3_roi = {}
    synapse_ids = {}

    synapseList = open(synapses_csv,'r')
    for line in synapseList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            synData = data_str.split(',')
            synID = synData[0]
            x = synData[1]
            y = synData[2]
            z = synData[3]
            confidence = synData[5]
            super_roi = synData[6]
            sub1_roi = synData[7]
            sub2_roi = synData[8]
            sub3_roi = synData[9]
            bodyID = int(synData[10])
            synapse_key = x + "," + y + "," + z
            synapse_body_lookup[synapse_key] = bodyID
            synapse_confidence[synapse_key] = confidence
            synapse_super_roi[synapse_key] = super_roi
            synapse_sub1_roi[synapse_key] = sub1_roi
            synapse_sub2_roi[synapse_key] = sub2_roi
            synapse_sub3_roi[synapse_key] = sub3_roi
            synapse_ids[synapse_key] = synID

    #for key in synapse_body_lookup:
    #    print(key, synapse_body_lookup[key])

    # open json connections
    all_synapses = json.loads(open(synapse_connect_json, 'rt').read())
    
    print("from_synId,from_x,from_y,from_z,from_conf,from_super_roi,from_sub1_roi,from_sub2_roi,from_sub3_roi,from_bodyId,connection,to_synId,to_x,to_y,to_z,to_conf,to_super_roi,to_sub1_roi,to_sub2_roi,to_sub3_roi,to_bodyId")

    for syn in all_synapses:
        pos = syn["Pos"]
        kind = syn["Kind"]
        #if kind != "PreSyn":
        #    continue
        from_x = str(pos[0])
        from_y = str(pos[1])
        from_z = str(pos[2])
        syn_key = from_x + "," + from_y + "," + from_z
        from_bodyId = 0
        from_conf = "0.0"
        from_super_roi = ""
        from_sub1_roi = ""
        from_sub2_roi = ""
        from_sub3_roi =""
        from_synId = ""

        if syn_key in synapse_ids:
            from_synId = synapse_ids[syn_key]
        else:
            # synapse isn't in labeled map file, it probably had label of 0, skip
            continue

        if syn_key in synapse_body_lookup:
            from_bodyId = str(synapse_body_lookup[syn_key])

        if syn_key in synapse_confidence:
            from_conf = synapse_confidence[syn_key]

        if syn_key in synapse_super_roi:
            from_super_roi = synapse_super_roi[syn_key]

        if syn_key in synapse_sub1_roi:
            from_sub1_roi = synapse_sub1_roi[syn_key]        

        if syn_key in synapse_sub2_roi:
            from_sub2_roi = synapse_sub2_roi[syn_key]

        if syn_key in synapse_sub3_roi:
            from_sub3_roi = synapse_sub3_roi[syn_key]

        if "Rels" in syn:
            syn_relations = syn["Rels"]
            for syn_rel in syn_relations:
                connection = syn_rel["Rel"]
                syn_rel_pos = syn_rel["To"]
                to_x = str(syn_rel_pos[0])
                to_y = str(syn_rel_pos[1])
                to_z = str(syn_rel_pos[2])
                rel_syn_key = to_x + "," + to_y + "," + to_z
                to_bodyId = 0
                to_conf = "0.0"
                to_super_roi = ""
                to_sub1_roi = ""
                to_sub2_roi = ""
                to_sub3_roi = ""
                to_synId = ""

                if rel_syn_key in synapse_ids:
                    to_synId = synapse_ids[rel_syn_key]
                else:
                    # synapse isn't in labeled map file, it probably had label of 0, skip
                    continue

                if rel_syn_key in synapse_body_lookup:
                    to_bodyId = str(synapse_body_lookup[rel_syn_key])

                if rel_syn_key in synapse_confidence:
                    to_conf = synapse_confidence[rel_syn_key]

                if rel_syn_key in synapse_super_roi:
                    to_super_roi = synapse_super_roi[rel_syn_key]

                if rel_syn_key in synapse_sub1_roi:
                    to_sub1_roi = synapse_sub1_roi[rel_syn_key]

                if rel_syn_key in synapse_sub2_roi:
                    to_sub2_roi = synapse_sub2_roi[rel_syn_key]

                if rel_syn_key in synapse_sub3_roi:
                    to_sub3_roi = synapse_sub3_roi[rel_syn_key]

                #print(from_synId + "," + kind + "," + to_synId)
                #print(from_synId + "," + to_synId)
                print(from_synId + "," + from_x + "," + from_y + "," + from_z + ","  + from_conf + "," + from_super_roi + "," + from_sub1_roi + "," + from_sub2_roi + "," + from_sub3_roi + "," + from_bodyId + "," + connection + "," + to_synId + "," + to_x + "," + to_y + "," + to_z + "," + to_conf + "," + to_super_roi + "," + to_sub1_roi + "," + to_sub2_roi + "," + to_sub3_roi + "," + to_bodyId)
