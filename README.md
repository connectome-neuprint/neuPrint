[![Build Status](https://travis-ci.org/connectome-neuprint/neuPrint.svg?branch=master)](https://travis-ci.org/connectome-neuprint/neuPrint) 
[![GitHub issues](https://img.shields.io/github/issues/connectome-neuprint/neuPrint.svg)](https://GitHub.com/connectome-neuprint/neuPrint/issues/)


# neuPrint
A blueprint of the brain. A set of tools for loading and analyzing connectome data into a Neo4j database. Analyze and explore connectome data stored in Neo4j using the neuPrint ecosystem: [neuPrintHTTP](https://github.com/connectome-neuprint/neuPrintHTTP), [neuPrintExplorer](https://github.com/connectome-neuprint/neuPrintExplorer), [Python API](https://github.com/connectome-neuprint/neuprint-python). 

[Javadocs](https://connectome-neuprint.github.io/neuPrint/)

## Requirements
* Neo4j version 3.5.3
* [apoc version 3.5.0.1](https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/tag/3.5.0.1)<sup>1</sup>
* neuprint-load-procedures.jar (for loading) OR neuprint-procedures.jar (for integration with neuPrint ecosystem after loading). Both are found in `executables` directory. Only one of these should be installed at a time since neuprint-load-procedures is a dependency of neuprint-procedures. <sup>2</sup>
    
1. Note that to install plugins in neo4j, the .jar files must be copied into the plugins directory of your neo4j database. [This may be helpful.](https://community.neo4j.com/t/how-can-i-install-apoc-library-for-neo4j-version-3-4-6-edition-community/1495)
2. One of these is required for loading. Triggers in neuprint-procedures.jar may slow down the load, so neuprint-load-procedures.jar is recommended. neuprint-procedures.jar contains the required load procedures as well as custom stored procedures and functions for use with the rest of the neuPrint ecosystem. If using with neuPrintHTTP and neuPrintExplorer, install neuprint-procedures (and delete neuprint-load-procedures) after loading the data.


## Example data

* mb6 : from ["A connectome of a learning and memory center in the adult Drosophila brain"](https://elifesciences.org/articles/26975) (Takemura, et al. 2017)

* fib25 : from ["Synaptic circuits and their variations within different columns in the visual system of Drosophila"](https://www.pnas.org/content/112/44/13711) (Takemura, et al. 2015)

## Load mb6 connectome data into Neo4j

1. After cloning the repository, set uri, user, and password in the example.properties file to match the those of the target database. You can also change the batch size for database transactions in this file (default is 100). Unzip mb6_neo4j_inputs.zip.  

2. Check that you're using the correct version of Neo4j (see Requirements) and that apoc and neuprint-load-procedures are installed. 

3. Run the following on the command line:
```console
$ java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=mb6 --synapseJson=mb6_neo4j_inputs/mb6_new_spec_Synapses.json --connectionJson=mb6_neo4j_inputs/mb6_new_spec_Synaptic_Connections.json --neuronJson=mb6_neo4j_inputs/mb6_new_spec_Neurons.json --skeletonDirectory=mb6_neo4j_inputs/mb6_skeletons --metaInfoJson=meta-data/mb6_meta_data.json
```

If data from JSON and/or .swc files is too large to fit into memory, neuprint can batch load these files by setting `--neuronBatchSize`, `--connectionBatchSize`, `--synapseBatchSize`, and/or `--skeletonBatchSize` to a value greater than 0. Alternatively, one can load multiple json files by repeatedly running the loader as long as all synapses are loaded prior to loading all connections, which are loaded prior to loading all neurons. For example:

```console
// load all synapses
java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=test --synapseJson=synapse_1.json
java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=test --synapseJson=synapse_2.json

// load all connections
java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=test --connectionJson=connections_1.json
java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=test --connectionJson=connections_2.json

// load all neurons
java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=test --neuronJson=neuron_1.json
java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=test --neuronJson=neuron_2.json

// load skeletons and meta data
java -jar executables/neuprint.jar --dbProperties=example.properties --datasetLabel=test --skeletonDirectory=test_skeletons --metaInfoJson=meta-data/test_meta_data.json
```

## Load your own connectome data into Neo4j using neuPrint

Follow these [input specifications](jsonspecs.md) to create your own synapse, connection, and neuron JSON files and skeleton files. To create a database on your computer, use [Neo4j Desktop](https://neo4j.com/download/?ref=product).

```console
$ java -jar executables/neuprint.jar --help
  
Usage: java -jar neuprint.jar [options]
  Options:
    --addClusterNames
      Indicates that cluster names should be added to Neuron nodes. (true by 
      default) 
      Default: true
    --addConnectionInfoOnly
      If finished adding synapses, synaptic connections, and neuron/segment 
      nodes, can choose to load connection info (ConnectsTo relationships, 
      ConnectionSets, neuron/segment properties) separately with this flag. 
      (omit to skip)
      Default: false
    --addConnectionSetRoiInfoAndWeightHP
      Indicates that an roiInfo property should be added to each ConnectionSet 
      and that the weightHP property should be added to all ConnectionSets 
      (true by default).
      Default: true
    --connectionBatchSize
      If > 0, the connection JSON file will be loaded in batches of this size.
      Default: 0
    --connectionJson
      Path to JSON file containing synaptic connections.
    --dataModelVersion
      Data model version (required)
      Default: 1.0
  * --datasetLabel
      Dataset value for all nodes (required)
  * --dbProperties
      Properties file containing database information (required)
    --help

    --metaInfoJson
      JSON file containing meta information for dataset
    --neuronBatchSize
      If > 0, the neuron JSON file will be loaded in batches of this size.
      Default: 0
    --neuronJson
      JSON file containing neuron data to import
    --neuronThreshold
      Integer indicating the number of synaptic densities (>=neuronThreshold/5 
      pre OR >=neuronThreshold post) a neuron should have to be given the 
      label of :Neuron (all have the :Segment label by default).
      Default: 10
    --postHPThreshold
      Confidence threshold to distinguish high-precision postsynaptic 
      densities (default is 0.0)
      Default: 0.0
    --preHPThreshold
      Confidence threshold to distinguish high-precision presynaptic densities 
      (default is 0.0)
      Default: 0.0
    --skeletonBatchSize
      If > 0, the skeleton files will be loaded in batches of this size.
      Default: 0
    --skeletonDirectory
      Path to directory containing skeleton files for this dataset
    --synapseBatchSize
      If > 0, the synapse JSON file will be loaded in batches of this size.
      Default: 0
    --synapseJson
      JSON file containing body synapse data to import

```
## neuPrint Property Graph Model

[Model details](pgmspecs.md)

## Developer Instructions

`mvn test` will run all tests, `mvn package` will run all tests and package the .jar file. Running `mvn verify` will copy .jar files to the `executables` directory (`package` places .jar files in the `target` directory). Versioning must be done manually in the pom.xml file.

## neuPrint Custom Procedures and Functions
These are found in the neuprint-procedures.jar file:
* [Graph Update API](graphupdateAPI.md)
* Analysis Procedures
      

