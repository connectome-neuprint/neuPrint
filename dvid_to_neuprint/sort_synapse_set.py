#!/bin/env

# python /groups/flyem/home/flyem/bin/create_neuprint_imports/sort_synapse_set.py All_Neuprint_Synapse_Connections_6f2cb.csv

import os
import sys
import logging
import requests
import numpy as np
import pandas as pd
from neuclease.dvid import fetch_labels_batched

infile = sys.argv[1]

df = pd.read_csv(infile)

df_sorted = df.sort_values('from_bodyId')

print("Writing csv results...")

df_sorted.to_csv(f'Sorted_{infile}', header=True, index=False)
df_sorted.head()
