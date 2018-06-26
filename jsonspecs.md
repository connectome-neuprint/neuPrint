
This documents the input specifications for loading connectome data into Neo4j using neuPrint.


# Neurons
Describes all the neurons or segments in the dataset.  (Note: that if a neuron id is not listed in this file but exists in other files, a default status of “not annotated” will be given.)


Neurons.json
```console
[
	{
“Id”: <unique int64 indentifies>,
“Status”: “<status of neuron>” 
(optional but common properties)
“Name”: “<neuron name>”
“NeuronType”: “<neuron type name>” 
“Size”: <num voxels in body> 
“rois”: [“<roi1>”, “<roi2>”, …]
“Soma”: { “Location”:[x,y,z],”Radius”:<float>}
	}
  ...
]
```

# Synapses
Describes all the synapses (pre and post) in the dataset and the corresponding bodies that contain these synapses. 

Synapses.json (all synapses have a unique X,Y,Z location)
```console
[
	{
		“BodyId”: <body id containing synapses>
		“SynapseSet”: [
			{
				“Type”: “<pre or post>”,
				“Confidence”: <confidence value>,
				“Location”: [x,y,z],
				“ConnectsTo”: [[x,y,z],...] # location of pre or post synaptic 
                                  connection -- directed edge from this
                                  point to another
				(optional: “ConnectsFrom”)
				“rois”: [“<roi1>”, “<roi2>”, …]
			}
			...
		]
	}
	...
]
```

# Skeletons (optional)
Skeletons of the neurons can be loaded from SWC files, which contains skeleton node connectivity, locations for each skeleton node, and the size of the skeleton node.

<body id>.swc

The SWC file also has a field for skeleton node type.  By default, this information will be ignored unless a file describing the encoding is provided.
