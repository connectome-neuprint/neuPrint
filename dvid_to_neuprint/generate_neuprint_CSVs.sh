#Activate python env
source /groups/flyem/home/flyem/runminiconda3
source activate run_flyem2

# dvid_server name/address is emdata4:8900
# dvid_uuid to export from is a7835

# export all synapses into csv format from dvid annotation instance "synapses"
python get_csv_all_synapses_dvid.py emdata4:8900 a7835 synapses

# generate new json file export for all synapses from dvid annotation instance "synapses"
python get_json_all_synapses_dvid.py emdata4:8900 a7835 synapses


# If you choose to map synapses to multiple levels of ROI instances
# top level instance
python map_toplevel_roi_for_csv.py emdata4:8900 a7835 synapses-a7835-dvid.csv synapses-a7835-superlevel-rois.csv
# prep file for next level of subregion map
./clean_toplevel.pl synapses-a7835-superlevel-rois.csv > clean-synapses-a7835-superlevel-rois.csv

# Add sub1 rois
python map_sub1_roi_for_csv.py emdata4:8900 a7835 clean-synapses-a7835-superlevel-rois.csv synapses-a7835-sub1-rois-added.csv

# Clean up sub1 rois file before mapping sub2 ROIs
./clean_sub1.pl synapses-a7835-sub1-rois-added.csv > clean-synapses-a7835-sub1-roi-added.csv

# Add sub2 rois
python map_sub2_roi_for_csv.py emdata4:8900 a7835 clean-synapses-a7835-sub1-roi-added.csv synapses-a7835-sub2-rois-added.csv

# Clean up sub2 rois file before mapping sub3 ROIs
./clean_sub2.pl synapses-a7835-sub2-rois-added.csv > clean-synapses-a7835-sub2-roi-added.csv

# Add sub3 rois
python map_sub3_roi_for_csv.py emdata4:8900 a7835 clean-synapses-a7835-sub2-roi-added.csv synapses-a7835-sub3-rois-added.csv

# Clean up sub3 rois file before mapping to neurons
./clean_sub3.pl synapses-a7835-sub3-rois-added.csv > clean-synapses-a7835-sub3-roi-added.csv


# map neuron bodyID to all synapses using labelmap instance name "segmentation"
python map_csv_to_segmentation.py emdata4:8900 a7835 segmentation clean-synapses-a7835-sub3-roi-added.csv synIDs_synapses-a7835-rois-bodyIDs-v1.csv

# Option step to remove all synapses that map to areas with no segmentation (bodyID 0)
#./remove_0_label_syns.pl synIDs_synapses-a7835-rois-bodyIDs-v1.csv > synIDs_synapses-a7835-rois-bodyIDs.csv


# generate list of all bodyIDs with pre and post counts
python generate_Neurons_list.py synIDs_synapses-a7835-rois-bodyIDs.csv > synapse_bodies_a7835.csv

# generate Neuprint Synapse file. specify dataset name to be used in neuprint "hemibrain"
python generate_Neuprint_Synapses_csv.py synIDs_synapses-a7835-rois-bodyIDs.csv hemibrain > Neuprint_Synapses_a7835.csv

# generate Synapse Connections file for all bodies
python generate_Synapse_Connections_All.py synIDs_synapses-a7835-rois-bodyIDs.csv synapses-dvid-a7835.json > All_Neuprint_Synapse_Connections_a7835.csv

# sort the connection set?
python sort_synapse_set.py All_Neuprint_Synapse_Connections_a7835.csv

# detect downstream synapses for neurons
python detect_downstream_synapses.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > downstream_synapses.csv
# detect downstream synapses for roiInfo
python detect_downstream_roiInfo.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > downstream_synapses_roiInfo.csv

# Create Synapse Connections csv
python generate_Synapse_Connections.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > Neuprint_Synapse_Connections_a7835.csv

# generate Neurons connections file
python generate_Neuron_connections.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > Neuprint_Neuron_Connections_a7835.csv

# generate Synapse Set file
python generate_SynapseSet_to_Synapses.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > Neuprint_SynapseSet_to_Synapses_a7835.csv

# generate Neuron to ROI
#python generate_Neuron_to_ROI.py synIDs_synapses-a7835-rois-bodyIDs.csv > Neuprint_Neuron_ROI_a7835.csv

# generate Synapse Set collection
python generate_SynapseSets.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv hemibrain > Neuprint_SynapseSet_a7835.csv 

# generate Neuron to Synapse Set
python generate_Neuron_to_SynaseSet.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > Neuprint_Neuron_to_SynapseSet_a7835.csv

# generate Synapse Set to Synapse Set
python generate_SynapseSet_to_SynapseSet.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > Neuprint_SynapseSet_to_SynapseSet_a7835.csv

# generate Synapse Set to ROI
#python generate_Synapse_Set_ROI.py Sorted_All_Neuprint_Synapse_Connections_a7835.csv > Neuprint_SynapseSet_ROI_a7835.csv

# get all body sizes
cat synapse_bodies_a7835.csv | perl -ne 'chomp(); if ($_=~/^\d+/) { @t=split(/\;/,$_); print "$t[0]\n"; }' > bodyIds_only.csv
python get_body_sizes_batch.py emdata4:8900 a7835 segmentation bodyIds_only.csv neuron_sizes.csv
# generate Neurons file
python generate_Neurons_csv.py emdata4:8900 a7835 synapse_bodies_a7835.csv hemibrain > Neuprint_Neurons_a7835.csv

# generate hemiBrain Meta
# arguments are synpase csv, dvid uuid, last dvid segmentation mutationID, last dvid mutation date change 
python generate_Neuprint_Meta_csv.py synIDs_synapses-a7835-rois-bodyIDs.csv a783551a946a4472a6f9bfaa2a009e44 hemibrain 1009308192 "2020-09-18 22:00:01" > Neuprint_Meta_a7835.csv
