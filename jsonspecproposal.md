To be loaded in order: Synapses.json, Connections.json, Neurons.json

# Synapses
An array of synapses in the dataset. All synapses must have a unique x,y,z location. To provide the most flexible format, we may want to only require locations to be unique within the same type (pre or post).


Synapses.json
```console
[
	{
        "type”: “<pre or post>”,
	    “confidence”: <confidence value>,
		“location”: [x,y,z],
		“rois”: [“<roi1>”, “<roi2>”, …]
	},
  ...
]
```

# Synaptic Connections
An array of synaptic connections (edges between synapse nodes) in the dataset. Note that since in some organisms one pre can synapse onto many posts, a pre location may occur multiple times in the file. There should be one object per **SynapsesTo** relationship in the dataset.

Connections.json
```
[
    {
        "pre": [x,y,z],  
        "post": [x,y,z]  
    },
    ...
]
```



# Neurons
Describes all neurons/bodies in the dataset. Includes properties of the neurons as well as the synapses they contain.


Neurons.json
```console
[
	{
    “id”: <unique int64 indentifies>,
    “status”: “<status of neuron>”,
    “name”: “<neuron name>”,                 // Naming scheme to be discussed 
    “type”: “<neuron type name>”,
    "instance: "<neuron instance name>",
    “size”: <num voxels in body> ,
    “rois”: [“<roi1>”, “<roi2>”, …],
    “soma”: { "location”:[x,y,z],”radius”:<float>}
    "synapseSet": [[x1,y1,z1],[x2,y2,z2],...]       // can use the index on location to find these synapse nodes in the database, other properties (e.g. ConnectsTo relationships, roiInfo) can be derived database after all synapses are added.
    },
	...
]
```

# Skeletons (optional)
Skeletons of the neurons can be loaded from SWC files, which contains skeleton node connectivity, locations for each skeleton node, and the size of the skeleton node.

<body id>.swc

The SWC file also has a field for skeleton node type.  By default, this information will be ignored unless a file describing the encoding is provided.
