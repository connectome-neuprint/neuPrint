[![Build Status](https://travis-ci.org/connectome-neuprint/neuPrint.svg?branch=master)](https://travis-ci.org/connectome-neuprint/neuPrint) 
[![GitHub issues](https://img.shields.io/github/issues/connectome-neuprint/neuPrint.svg)](https://GitHub.com/connectome-neuprint/neuPrint/issues/)


# neuPrint+
### exploring inter and intra cellular interactions
A blueprint of the brain. A set of tools for loading and analyzing connectome data
into a Neo4j database. Analyze and explore connectome data stored in Neo4j using
the neuPrint ecosystem: [neuPrintHTTP](https://github.com/connectome-neuprint/neuPrintHTTP),
[neuPrintExplorer](https://github.com/connectome-neuprint/neuPrintExplorer),
[Python API](https://github.com/connectome-neuprint/neuprint-python).



## Requirements
* Neo4j version 3.5.3
* [apoc version 3.5.0.1](https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/tag/3.5.0.1)<sup>1</sup>

## Example data
* hemibrain (hemibrain v1.0) : from ["A Connectome of the Adult Drosophila Central Brain"](https://www.biorxiv.org/content/10.1101/2020.01.21.911859v1) (C. Shan Xu, et al. 2020)

* mushroombody (mb6) : from ["A connectome of a learning and memory center in the adult Drosophila brain"](https://elifesciences.org/articles/26975) (Takemura, et al. 2017)

* medulla7column (fib25) : from ["Synaptic circuits and their variations within different columns in the visual system of Drosophila"](https://www.pnas.org/content/112/44/13711) (Takemura, et al. 2015)

## Download the Hemibrain Data Set 
* The hemibrain dataset (v1.0.1) is available for [download on Google Bucket](https://storage.cloud.google.com/hemibrain-release/neuprint/hemibrain_v1.0.1_neo4j_inputs.zip) or by using gsutil ```
gs://hemibrain-release/neuprint/hemibrain_v1.0.1_neo4j_inputs.zip```. Format of these files are csv and can be imported into neo4j to generate a neuPrint database.

## Load mushroombody (mb6) connectome data into Neo4j
Instructions on how to create a neuPrint instance of the [mushroombody(mb6)](neo4j_desktop_load.md).

## Load your own connectome data into Neo4j using neuPrint
Coming Soon

## neuPrint Property Graph Model
Coming Soon

## neuPrint Custom Procedures and Functions

* [Graph Update API](https://github.com/connectome-neuprint/neuPrint/tree/master/update_scripts)

      

