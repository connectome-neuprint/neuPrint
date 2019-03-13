[![Build Status](https://travis-ci.org/connectome-neuprint/neuPrint.svg?branch=master)](https://travis-ci.org/connectome-neuprint/neuPrint) 
[![Waffle.io - Columns and their card count](https://badge.waffle.io/connectome-neuprint/neuPrint.svg?columns=all)](https://waffle.io/connectome-neuprint/neuPrint)


# neuPrint
A blueprint of the brain. A set of tools for loading and analyzing connectome data into a Neo4j database. Analyze connectome data stored in Neo4j using [neuPrintExplorer](https://github.com/connectome-neuprint/neuPrintExplorer). 

[Javadocs](https://connectome-neuprint.github.io/neuPrint/)

## Requirements
* Neo4j version 3.5.3
* [apoc version 3.5.0.1](https://neo4j-contrib.github.io/neo4j-apoc-procedures/)<sup>1</sup>
* **Optional** neuprint-load-procedures.jar OR neuprint-procedures.jar<sup>2</sup>
    
1. Note that to install plugins in neo4j, the .jar files must be copied into the plugins directory of your neo4j database. [This may be helpful.](https://community.neo4j.com/t/how-can-i-install-apoc-library-for-neo4j-version-3-4-6-edition-community/1495)
2. One of these is required for loading if using flag `--addConnectionSetRoiInfoAndWeightHP`. Triggers in neuprint-procedures.jar may slow down the load, so neuprint-load-procedures.jar is recommended. neuprint-procedures.jar contains custom stored procedures and functions for use with the rest of the neuPrint ecosystem. If using with neuPrintHTTP and neuPrintExplorer, install neuprint-procedures after loading the data.


## Example data

* mb6 : from ["A connectome of a learning and memory center in the adult Drosophila brain"](https://elifesciences.org/articles/26975) (Takemura, et al. 2017)

* fib25 : from ["Synaptic circuits and their variations within different columns in the visual system of Drosophila"](https://www.pnas.org/content/112/44/13711) (Takemura, et al. 2015)

## Load mb6 connectome data into Neo4j

1. After cloning the repository, set uri, user, and password in the example.properties file to match the those of the target database. You can also change the batch size for database transactions in this file (default is 100). Unzip mb6_neo4j_inputs.zip.  

2. Check that you're using the correct version of Neo4j (see Requirements) and that apoc and neuprint-load-procedures are installed. 

3. Run the following on the command line:
```console
$ java -jar neuprint.jar --dbProperties=example.properties --datasetLabel=mb6 --addNeuronsAndSynapses --neuronJson=mb6_neo4j_inputs/mb6_Neurons_with_nt.json --synapseJson=mb6_neo4j_inputs/mb6_Synapses.json --metaInfoJson=meta-data/mb6_meta_data.json
```

## Load mb6 skeleton data into Neo4j

1. Follow step 1 and 2 above. 

2. Run the following on the command line:
```console
$ java -jar neuprint.jar --dbProperties=example.properties --datasetLabel=mb6 --prepDatabase --addSkeletons --skeletonDirectory=mb6_neo4j_inputs/mb6_skeletons
```
The ```prepDatabase``` flag ensures that the proper indices and constraints are set in the database. Note that ```--addSkeletons --skeletonDirectory=mb6_neo4j_inputs/mb6_skeletons``` can be added to the previous command to load skeletons with the neuron/synapse data.

## Load your own connectome data into Neo4j using neuPrint

Follow these [input specifications](jsonspecs.md) to create your own neurons.json, synapses.json, and skeleton files. To create a database on your computer, use [Neo4j Desktop](https://neo4j.com/download/?ref=product).

```console
$ java -jar neuprint.jar --help
  
Usage: java -jar neuprint.jar [options]
  Options:
    --addAutoNames
      Indicates that automatically generated names should be added for this 
      dataset. Auto-names are in the format ROIA-ROIB_8 where ROIA is the roi 
      in which a given neuron has the most inputs (postsynaptic densities) and 
      ROIB is the roi in which a neuron has the most outputs (presynaptic 
      densities). The final number renders this name unique per dataset. Names 
      are only generated for neurons that have greater than the number of 
      synapses indicated by neuronThreshold. If neurons do not already have a 
      name, the auto-name is added to the name property. (skip to omit)
      Default: false
    --addAutoNamesOnly
      Indicates that only the autoNames should be added for this dataset. 
      Requires the existing dataset to be completely loaded into neo4j. Names 
      are only generated for neurons that have greater than the number of 
      synapsesindicated by neuronThreshold (omit to skip)
      Default: false
    --addClusterNames
      Indicates that cluster names should be added to Neuron nodes. (true by 
      default) 
      Default: true
    --addConnectionSetRoiInfoAndWeightHP
      Indicates that an roiInfo property should be added to each ConnectionSet 
      and that the weightHP property should be added to all ConnectionSets 
      (omit to skip).
      Default: false
    --addConnectionSets
      Indicates that connection set nodes should be added (omit to skip)
      Default: false
    --addConnectsTo
      Indicates that ConnectsTo relations should be added (omit to skip)
      Default: false
    --addMetaNodeOnly
      Indicates that only the Meta Node should be added for this dataset. 
      Requires the existing dataset to be completely loaded into neo4j. (omit 
      to skip)
      Default: false
    --addNeuronsAndSynapses
      Indicates that both neurons and synapses JSONs should be loaded and all 
      database features added
      Default: false
    --addSegmentRois
      Indicates that neuron ROI labels should be added (omit to skip)
      Default: false
    --addSkeletons
      Indicates that skeleton nodes should be added (omit to skip)
      Default: false
    --addSynapses
      Indicates that synapse nodes should be added (omit to skip)
      Default: false
    --addSynapsesTo
      Indicates that SynapsesTo relations should be added (omit to skip)
      Default: false
    --dataModelVersion
      Data model version (required)
      Default: 1.0
  * --datasetLabel
      Dataset value for all nodes (required)
  * --dbProperties
      Properties file containing database information (required)
    --editMode
      Indicates that neuprint is being used in edit mode to alter data in an 
      existing database (omit to skip).
      Default: false
    --getSuperLevelRoisFromSynapses
      Indicates that super level rois should be computed from synapses JSON 
      and added to the Meta node.
      Default: false
    --help

    --indexBooleanRoiPropertiesOnly
      Indicates that only boolean roi properties should be indexed. Requires 
      the existing dataset to be completely loaded into neo4j. (omit to skip)
      Default: false
    --loadNeurons
      Indicates that data from neurons JSON should be loaded to database (omit 
      to skip)
      Default: false
    --loadSynapses
      Indicates that data from synapses JSON should be loaded to database 
      (omit to skip)
      Default: false
    --metaInfoJson
      JSON file containing meta information for dataset
    --neuronJson
      JSON file containing neuron data to import
    --neuronThreshold
      Integer indicating the number of synaptic densities (>=neuronThreshold/5 
      pre OR >=neuronThreshold post) a neuron should have to be given the 
      label of :Neuron (all have the :Segment label by default) and an 
      auto-name. To add auto-names, must have --addAutoName OR 
      --addAutoNamesOnly enabled.
      Default: 10
    --postHPThreshold
      Confidence threshold to distinguish high-precision postsynaptic 
      densities (required)
      Default: 0.0
    --preHPThreshold
      Confidence threshold to distinguish high-precision presynaptic densities 
      (required) 
      Default: 0.0
    --prepDatabase
      Indicates that database constraints and indexes should be setup (omit to 
      skip) 
      Default: false
    --server
      DVID server to be added to Meta node.
    --skeletonDirectory
      Path to directory containing skeleton files for this dataset
    --startFromSynapseLoad
      Indicates that load should start from the synapses JSON.
      Default: false
    --synapseJson
      JSON file containing body synapse data to import
    --uuid
      DVID UUID to be added to Meta node.
```
## neuPrint Property Graph Model

[Model details](pgmspecs.md)

## neuPrint Neo4j Stored Procedures (Under development; may not be up-to-date)

Place neuprint-procedures.jar into the plugins folder of your neo4j database, and restart the database. Under development. All features are experimental. 
### Proofreader procedures (READ/WRITE)
1. applies time stamp to nodes when they are created, when their properties change, when relationships are changed, and when relationship properties are changed. 
2. proofreader.mergeNeuronsFromJson(mergeJson, datasetLabel) : merge neurons from json file containing single mergeaction json
3. proofreader.cleaveNeuronsFromJson(cleaveJson, datasetLabel) : cleave neuron from json file containing a single cleaveaction json
3. proofreader.addSkeleton(fileUrl,datasetLabel): Load skeleton file from url into database and connect to appropriate neuron. Returns the new skeleton node. e.g.: ``` CALL proofreader.addSkeleton("http://fileurl/87475_swc","mb6") YIELD node RETURN node ```
### Analysis procedures (READ)
1. analysis.getLineGraph(bodyId,datasetLabel,vertexSynapseThreshold=50): used to produce an edge-to-vertex dual graph, or line graph, for a neuron. Returned value is a map with the vertex json under key "Vertices" and edge json under "Edges". 
2. analysis.getConnectionCentroidsAndSkeleton(bodyId,datasetLabel,vertexSynapseThreshold=50) Provides the synapse points and centroid for each type of synaptic connection (e.g. neuron A to neuron B) present in the neuron with the provided bodyId as well as the skeleton for that body. Returned value is a map with the centroid json " +
under key "Centroids" and the skeleton json under key "Skeleton". 
3. analysis.calculateSkeletonDistance(datasetLabel, SkelNodeA,SkelNodeB): Calculates the distance between two :SkelNodes for a body. See #4.
4. analysis.getNearestSkelNodeOnBodyToPoint(bodyId,datasetLabel,x,y,z): Returns the :SkelNode on the given body's skeleton that is closest to the provided point. To be used with #3.
5. analysis.getInputAndOutputCountsForRois(bodyId,datasetLabel): Produces json array with counts of inputs and outputs per roi for a given neuron.
6. analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi(roi,datasetLabel,synapseThreshold): Produces json array with input and output feature vectors for each neuron in the provide ROI. Vectors contain the synapse count per ROI.
### neuprintUserFunctions (READ, output a single value)
1. neuprint.locationAs3dCartPoint(x,y,z) : returns a 3D Cartesian org.neo4j.graphdb.spatial.Point type with the provided coordinates. 
      

