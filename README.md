# neuPrint
A tool for loading connectome data into a Neo4j database. 

## Load mb6 connectome data into Neo4j

1. After cloning the repository, set uri, user, and password in the example.properties file to match the those of the target database. You can also change the batch size for database transactions in this file (default is 100). Unzip mb6_neo4j_inputs.zip.  

2. Run the following on the command line:
```console
java -jar neuprinter.jar --dbProperties=example.properties --doAll --datasetLabel=mb6 --neuronJson=mb6_neo4j_inputs/mb6_Neurons_with_nt.json --synapseJson=mb6_neo4j_inputs/mb6_Synapses.json
```

## Load mb6 skeleton data into Neo4j

1. Follow step 1 above. 

2. Run the following on the command line:
```console
java -jar neuprinter.jar --dbProperties=example.properties --prepDatabase --addSkeletons --datasetLabel=mb6 --skeletonDirectory=mb6_neo4j_inputs/mb6_skeletons
```
