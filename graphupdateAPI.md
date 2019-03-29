# neuPrint Graph Update Procedures
This document describes available procedures for updating a neuPrint graph. These are contained in the neuprint-procedures.jar file and can be called with a Cypher query beginning with the `CALL` keyword. For example, `CALL proofreader.deleteSynapse(1,2,3,'testDataset')`
## Neuron/Segment properties
* **proofreader.updateProperties(\<string\> neuronJsonObject, \<string\> dataset)**: Update properties on a Neuron/Segment node. Supports adding status, type, name, size, and soma (location and radius). Input JSON should follow specifications for "Neurons" JSON file and supply a single Neuron/Segment object as a string: https://github.com/connectome-neuprint/neuPrint/blob/master/jsonspecs.md
* **proofreader.deleteSoma(\<int64\> bodyId, \<string\> dataset)**: Delete soma (radius and location) from Neuron/Segment node.
* **proofreader.deleteName(\<int64\> bodyId,  \<string\> dataset)**: Delete name from Neuron/Segment node.
* **proofreader.deleteStatus(\<int64\> bodyId,  \<string\> dataset)**: Delete status from Neuron/Segment node.
* (Coming soon) **proofreader.deleteType**

## Adding and removing Neurons/Segments
* **proofreader.addNeuron(\<string\> neuronAdditionJsonObject, \<string\> dataset)**: add a Neuron/Segment with properties, synapses, and connections specified by an input JSON:
```
{
    "Id": <int64>,
    "Size": <int64>,
    "MutationUUID": <string> (from DVID),
    "MutationID": <int64> (from DVID),
    "Status": <string>,
    "Soma": {
        "Location": [<int>,<int>,<int>],
        "Radius": <float>
        },
    "Name": <string>,
    "CurrentSynapses": [
        { 
            "Location": [<int>,<int>,<int>],
            "Type": <string> (pre or post)
        },
        ...
    ]
}
```
* **proofreader.deleteNeuron(\<int64\> bodyId,  \<string\> dataset)**: Delete a Neuron/Segment from the database. Will orphan any synapses contained by the body.
* **proofreader.addSkeleton(\<string\> swcFileURL, \<string\> dataset)**: Load skeleton from provided URL and connect it to its associated Neuron/Segment. (Note: file URL must end with "<bodyID>.swc" or "<bodyID>_swc" where <bodyID> is the body ID of the Neuron/Segment) 
* **proofreader.deleteSkeleton(\<int64\> bodyId,  \<string\> dataset)**: Delete skeleton for Neuron/Segment with provided body ID.

## Adding and removing ROIs (via Synapses)
These procedures will update ROI information for the Neuron/Segment containing the synapse and on the Meta node. 
* **proofreader.addRoiToSynapse(\<double\> x,\<double\> y,\<double\> z,\<string\> roiName,\<string\> dataset)**: Add provided ROI to synapse at location x,y,z.
* **proofreader.removeRoiFromSynapse(\<double\> x,\<double\> y,\<double\> z,\<string\> roiName,\<string\> dataset)**: Remove provided ROI from synpase at location x,y,z.

## Adding and removing Synapses
* **proofreader.addSynapse(\<string\> synapseJsonObject, \<string\> dataset)**: Add a synapse node to the dataset specified by an input JSON. Will only add the Synapse node, not the connections to other Synapse nodes. Input format:
```
{
    “Type”: <string> (pre or post),
    “Confidence”: <float>, (default is 0.0 if not provided)
    “Location”: [<int>,<int>,<int>],
    “rois”: [<string>, <string>, …]
}
```
* **proofreader.addConnectionBetweenSynapseNodes(\<double\> preX,\<double\> preY,\<double\> preZ,\<double\> postX,\<double\> postY,\<double\> postZ,\<string\> dataset)**: Add a SynapsesTo relationship between two Synapse nodes. Both nodes must exist in the dataset, and neither can be currently owned by a Neuron/Segment.
* **proofreader.deleteSynapse(\<double\> x,\<double\> y,\<double\> z,\<string\> dataset)**: Remove Synapse node with provided location. This procedure will orphan a Synapse prior to deleting it if necessary.
* **proofreader.orphanSynapse(\<double\> x,\<double\> y,\<double\> z,\<string\> dataset)**: Orphan (but do not delete) Synapse node with provided location.
* (Coming soon) **proofreader.addSynapseToSegment**