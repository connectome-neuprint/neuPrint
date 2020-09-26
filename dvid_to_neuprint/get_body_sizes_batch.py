# python get_body_sizes_batch.py emdata4:8900 bodyIds_only.csv neuron_sizes.csv 6f2cb

import sys
import pandas as pd
from requests import HTTPError

from neuclease.util import compute_parallel, read_csv_col
from neuclease.dvid import find_master, fetch_sizes

dvid_server = sys.argv[1]
uuid = sys.argv[2]
label_inst = sys.argv[3]
bodyid_csv = sys.argv[4]
out_file = sys.argv[5]


#master_seg = (prod, find_master(prod), 'segmentation')
master_seg = (dvid_server, uuid, label_inst)

#bodies = read_csv_col(bodyid_csv)
# chunk body list into groups of 1000
body_groups = []
bodyList = open(bodyid_csv,'r')
group_list = []
body_count = 0
for line in bodyList:
   if line[0].isdigit():
      bodyID = line.rstrip('\n')
      group_list.append(int(bodyID))
      body_count += 1
      if body_count == 1000:
         body_groups.append(group_list)
         group_list = []
         body_count = 0

if len(group_list) > 0:
   body_groups.append(group_list)

PROCESSES = 15
def get_sizes(label_ids):
   try:
      sizes_pd = fetch_sizes(*master_seg, label_ids, supervoxels=False)
   except HTTPError:
      s_empty_pd = pd.Series(index=label_ids, data=-1, dtype=int)
      s_empty_pd.name = 'size'
      s_empty_pd.index.name = 'body'
      return(s_empty_pd)
   else:
      return(sizes_pd)

body_sizes_df_list = compute_parallel(get_sizes, body_groups, chunksize=100, processes=PROCESSES, ordered=False)

body_sizes_pd = pd.concat(body_sizes_df_list)

body_sizes_pd.to_csv(out_file, index=True)
