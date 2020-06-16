To be loaded in order: Synapses.json, Connections.json, Neurons.json

# Synapses
An array of synapses in the dataset. All synapses must have a unique x,y,z location.

Synapses.json
```console
[
	{
        "type": <string> "pre" OR "post", (required)
	"confidence": <double> in range [0.0-1.0], (optional, default is 0.0)
	"location": [<int64> x, <int> y, <int> z], (required; unique per type (pre/post) per dataset)
	"rois": [<string> roi1, <string> roi2 …] (optional; first listed will be considered a "super" level ROI)
	},
  ...
]
```

# Synaptic Connections
An array of synaptic connections (edges between synapse nodes) in the dataset. Note that since in some organisms one pre can synapse onto many posts, a pre location may occur multiple times in the file. There should be one object per **SynapsesTo** relationship in the dataset.

Connections.json
```console
[
    {
        "pre": [<int64> x, <int64> y, <int64> z], (required) 
        "post": [<int64> x, <int64> y, <int64> z] (required)
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
	"id": <int64> unique indentifier per dataset, (required)
	"status": <string> status of neuron, (optional)
	"name": <string> name of neuron, (optional)
	"instance": <string> instance name of neuron, (optional)
	"type": <string> type of neuron, (optional)
	"instance": <string> instance of neuron, (optional)
	"primaryNeurite": <string> primary neurite, (optional)
	"majorInput": <string> major input, (optional)
	"majorOutput": <string> major output, (optional)
	"clonalUnit": <string> clonal unit, (optional)
	"neurotransmitter": <string> neurotransmitter, (optional)
	"property": <string> property, (optional)
	"size": <int64> num voxels in body, (optional)
	"rois": [<string> roi1, <string> roi2 …], (optional; we use rois from synapses to add rois to neurons)
	"soma": { (optional)
		"location": [<int64> x, <int64> y, <int64> z], (required if soma present)
		"radius":<double> (required if soma present)
		},
	"synapseSet": [ (optional)
    		[<int64> x1, <int64> y1, <int64> z1], 
		[<int64> x2, <int64> y2, <int64> z2],
		...
		]
	},
	...
]
```

# Skeletons (optional)
Skeletons of the neurons can be loaded from SWC files, which contains skeleton node connectivity, locations for each skeleton node, and the size of the skeleton node.

\<body id\>.swc

The SWC file also has a field for skeleton node type.  By default, this information will be ignored unless a file describing the encoding is provided.
