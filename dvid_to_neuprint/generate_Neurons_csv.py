#!/bin/env

# python generate_Neurons_csv.py emdata4:8900 6f2cb9f1d4514d64bf4bd788106ad8ab synapse_bodies_6f2cb.csv hemibrain > Neuprint_Neurons_6f2cb.csv
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
    dvid_server = sys.argv[1]
    dvid_uuid = sys.argv[2]
    bodies_syn_file =  sys.argv[3] #csv file
    dataset = sys.argv[4]

    all_rois_csv = "all_ROIs.txt"
    all_rois = []
    allRoisList = open(all_rois_csv,'r')
    for line in allRoisList:
        roi_name = line.rstrip('\n')
        #print(roi_name)
        all_rois.append(roi_name)

    downstream_lookup = {}
    downstream_csv = "downstream_synapses.csv"
    allDownStream = open(downstream_csv,'r')
    for line in allDownStream:
        clean_line = line.rstrip('\n')
        downstream_data = clean_line.split(",")
        bodyId = downstream_data[0]
        downstream_count = downstream_data[1]
        downstream_lookup[bodyId] = downstream_count

    downstream_roiInfo_lookup = {}
    downstream_roiInfo = "downstream_synapses_roiInfo.csv"
    allDownStreamRoiInfo = open(downstream_roiInfo,'r')
    for line in allDownStreamRoiInfo:
        clean_line = line.rstrip('\n')
        downstream_roiInfo_data = clean_line.split(";")
        bodyId = downstream_roiInfo_data[0]
        dns_roiInfo_str = downstream_roiInfo_data[1]
        dns_roiInfo = json.loads(dns_roiInfo_str)
        downstream_roiInfo_lookup[bodyId] = dns_roiInfo

    #somaFile = "/groups/flyem/home/flyem/bin/identify_soma/soma_bodies_" + dvid_uuid + ".txt"
    #somaList = open(somaFile,'r')
    soma_lookup = {}
    #for line in somaList:
    #    data_str = line.rstrip('\n')
    #    data = data_str.split(",")
    #    soma_bodyID = data[0]
    #    soma_data = data[1].split(" ")
    #    soma_lookup[str(soma_bodyID)] = soma_data

    sizes_csv = "neuron_sizes.csv"
    size_lookup = {}

    sizeList = open(sizes_csv,'r')

    for line in sizeList:
        if line[0].isdigit():
            data_str = line.rstrip('\n')
            sizeData = data_str.split(',')
            bodyID = sizeData[0]
            voxelsize = sizeData[1]
            size_lookup[bodyID] = voxelsize

    

    #keyvalue = "segmentation_annotations"
    keyvalue = sys.argv[5]

    node = (dvid_server, dvid_uuid)
    all_keys = fetch_keys(*node, keyvalue)
    seg_annot_count = len(all_keys)
    #print("Number of segmentation_annotations entries:", seg_annot_count)
    #sys.exit()
    #all_values = {}
    #for start in trange(0,300_000,100_000): #v1.0
    #for start in trange(0,400_000,100_000): #old head
    #for start in trange(0,500_000,100_000): # head
    #    values = fetch_keyvalues(*node, keyvalue, all_keys[start:start+100_000], as_json=True)
    #    all_values.update(values)
    all_values = fetch_keyvalues(*node, keyvalue, all_keys, as_json=True)


    #superLevelrois = ["ME(R)","AME(R)","LO(R)","LOP(R)","CA(R)","CA(L)","PED(R)","a'L(R)","a'L(L)","aL(R)","aL(L)","gL(R)","gL(L)","b'L(R)","b'L(L)","bL(R)","bL(L)","FB","AB(R)","AB(L)","EB","PB","NO", "BU(R)","BU(L)","LAL(R)","LAL(L)","AOTU(R)","AVLP(R)","PVLP(R)","PLP(R)","WED(R)","LH(R)","SLP(R)","SIP(R)","SIP(L)","SMP(R)","SMP(L)","CRE(R)","CRE(L)","ROB(R)","SCL(R)","SCL(L)","ICL(R)","ICL(L)","IB","ATL(R)","ATL(L)","AL(R)","AL(L)","VES(R)","VES(L)","EPA(R)","EPA(L)","GOR(R)","GOR(L)","SPS(R)","SPS(L)","IPS(R)","SAD","FLA(R)","CAN(R)","PRW","GNG"]

    bodiesList = open(bodies_syn_file,'r')

    #print (":ID,bodyId:long,pre:int,post:int,status:string,instance:string,type:string,primaryNeurite:string,majorInput:string,majorOutput:string,neurotransmitter:string,clonalUnit:string,somaLocation:point{srid:9157},somaRadius:float,size:long,:LABEL")
    header = '":ID(Body-ID)","bodyId:long","pre:int","post:int","upstream:int","downstream:int","status:string","statusLabel:string","cropped:boolean","instance:string","notes:string","type:string","cellBodyFiber:string","somaLocation:point{srid:9157}","somaRadius:float","size:long","roiInfo:string",":LABEL"'
    for roi in all_rois:
        header = header + ',"' + roi + ':boolean"'

    print (header)

    for line in bodiesList:
        tmp0 = line.rstrip('\n')
        #tmp1 = tmp0.replace("\ufeff","")
        if tmp0[0].isdigit():
            bodyData = tmp0.split(";")
            bodyID = bodyData[0]
            pre_syns = bodyData[1]
            post_syns = bodyData[2]

            upstream = "0"
            if int(post_syns) > 0:
                upstream = str(post_syns)

            downstream = "0"
            if bodyID in downstream_lookup:
                downstream = str(downstream_lookup[bodyID])

            #roiInfo = bodyData[3]
            roiInfo = bodyData[3]
            if len(roiInfo) > 0:
                roiInfo_str = bodyData[3].replace('"','""')
            else:
                roiInfo = "{}"
                roiInfo_str = "{}"

            if bodyID in downstream_roiInfo_lookup:
                downstream_roiInfo = downstream_roiInfo_lookup[bodyID]
                roiInfo_json = json.loads(roiInfo)
                for roiName in roiInfo_json:
                    roiData = roiInfo_json[roiName]
                    if roiName in downstream_roiInfo:
                        dns_data = downstream_roiInfo[roiName]
                        roiData["downstream"] = dns_data["downstream"]
                    if "post" in roiData:
                        roiData["upstream"] = roiData["post"]
                roiInfo_tmp = json.dumps(roiInfo_json)
                roiInfo_str = roiInfo_tmp.replace('"','""')

            somaLocation = ""
            somaLocationX = ""
            somaLocationY = ""
            somaLocationZ = ""
            somaRadius = ""
            bodySize = "0"
            status = ""
            instance = ""
            neuronType = ""
            primaryNeurite = ""
            majorInput = ""
            majorOutput = ""
            neurotransmitter = ""
            clonalUnit = ""
            statusLabel = ""
            cropped = ""
            synonyms = ""
            #isDistinct = ""

            roi_info_dict = {}
            if len(bodyData[3]) > 0:
                roi_info_dict = json.loads(bodyData[3])

            roi_booleans = ""
            for roi_name in all_rois:
                is_roi = ""
                #roi_search = roi_name.replace("|",",")
                if roi_name in roi_info_dict:
                    is_roi = "true"
                roi_booleans = roi_booleans + "," + is_roi

            if bodyID in size_lookup:
                bodySize = size_lookup[bodyID]

            if bodyID in soma_lookup:
                soma_data = soma_lookup[bodyID]
                somaLocationX = int(float(soma_data[2]))
                somaLocationY = int(float(soma_data[3]))
                somaLocationZ = int(float(soma_data[4]))
                somaLocation = '"{x:' + str(somaLocationX) + ',y:' + str(somaLocationY) + ',z:' + str(somaLocationZ) + '}"'
                somaRadius = soma_data[5]
            
            if bodyID in all_values:
                bodyData = all_values[bodyID]
                if bodyData is None:
                    continue
                
                if 'status' in bodyData:
                    status = bodyData["status"]
                    statusLabel = status
                    if 'Traced' == status:
                        status = "Traced"
                        cropped = "false"
                    if " traced" in status:
                        status = "Traced"
                        cropped = "false"
                    if status == "Leaves":
                        status = "Traced"
                        cropped = "true"
                    if status == "Orphan hotknife":
                        status = "Orphan"
                    if status == "Orphan-artifact":
                        status = "Orphan"
                    if status == "0.5assign":
                        status = "Assign"

                if 'synonym' in bodyData:
                    synonym1 = bodyData["synonym"]
                    synonym2 = synonym1.rstrip('\n')
                    synonyms = '"' + synonym2 + '"'

                if 'name' in bodyData:
                    instance1 = bodyData["name"]
                    instance2 = instance1.rstrip('\n')
                    instance = instance2.replace(',','')

                if 'instance' in bodyData:
                    instance1 = bodyData["instance"]
                    instance2 = instance1.rstrip('\n')
                    instance = instance2.replace(',','')
                
                if 'class' in bodyData:
                    neuronType1 = bodyData["class"]
                    neuronType2 = neuronType1.rstrip('\n')
                    neuronType = neuronType2.replace(',','')
                    
                #if 'property' in bodyData:
                #    if bodyData['property'] == "Distinct":
                #        isDistinct = "true"

                if 'primary neurite' in bodyData:
                    primaryNeurite1 = bodyData["primary neurite"]
                    primaryNeurite2 = primaryNeurite1.rstrip('\n')
                    primaryNeurite = primaryNeurite2.replace(',','')
                
                if 'major input' in bodyData:
                    majorInput1 = bodyData["major input"]
                    majorInput2 = majorInput1.rstrip('\n')
                    majorInput = '"' + majorInput2 + '"'

                if 'major output' in bodyData:
                    majorOutput1 = bodyData["major output"]
                    majorOutput2 = majorOutput1.rstrip('\n')
                    majorOutput = '"' + majorOutput2 + '"'

                if 'neurotransmitter' in bodyData:
                    neurotransmitter1 = bodyData["neurotransmitter"]
                    neurotransmitter2 = neurotransmitter1.rstrip('\n')
                    neurotransmitter = neurotransmitter2.replace(',','')

                if 'clonal unit' in bodyData:
                    clonalUnit1 = bodyData["clonal unit"]
                    clonalUnit2 = clonalUnit1.rstrip('\n')
                    clonalUnit = clonalUnit2.replace(',','')
                #clonalUnit = ""
                
            is_hemibrain_Neuron = ""
            if int(pre_syns) >= 2:
                is_hemibrain_Neuron = ";Neuron;" + dataset + "_Neuron"
            elif int(post_syns) >= 10:
                is_hemibrain_Neuron = ";Neuron;" + dataset + "_Neuron"
            elif len(somaRadius) > 0:
                is_hemibrain_Neuron = ";Neuron;" + dataset + "_Neuron"

            if status == "":
                if int(pre_syns) >= 2:
                    status = "Assign"
                    statusLabel = "0.5assign"
                if int(post_syns) >= 10:
                    status = "Assign"
                    statusLabel = "0.5assign"
                    
            #print(bodyID + "," + bodyID + "," + pre_syns + "," + post_syns + "," + status + "," + instance + "," + neuronType + "," + primaryNeurite + "," + majorInput + "," + majorOutput + "," + neurotransmitter + "," + clonalUnit + "," + somaLocation + "," + somaRadius + "," +  bodySize + ",Segment;hemibrain_Segment")
            print(bodyID + "," + bodyID + "," + pre_syns + "," + post_syns + "," + upstream + "," + downstream + "," + status + "," + statusLabel + "," + cropped + "," + instance + "," + synonyms + "," + neuronType + "," + primaryNeurite + "," + somaLocation + "," + somaRadius + "," +  bodySize + ",\"" + roiInfo_str +  "\",Segment;" + dataset + "_Segment" + is_hemibrain_Neuron + roi_booleans)
