# Developer Tips

## Add a new property to Segment/Neuron nodes or Synapse nodes
1. Add field to input jsons for loader (neuprint module). This will require updating the gson deserialization settings (see neuprint/json). Neurons are loaded as Neuron objects, and synapses are loaded as Synapse objects (see neuprint/model).
2. Decide if the property needs to have a uniqueness constraint or needs to be indexed. The loader handles these (see neuprint/Neo4jImporter), but these can also be added manually to an existing database.
3. The relevant cypher query in the loader will need to take the data from each Neuron or Synapse object and assign it to the property on nodes (see neuprint/Neo4jImporter).
4. Consider adding procedures for adding/deleting these properties (see neuprint-procedures module). For example, the procedure proofreader.updateProperties is used to update properties on Segments/Neurons, and there are various methods for deleting properties on Segments/Neurons.

## Add a new property on ConnectsTo relationships
1. Add field during load (see Neo4jImporter). This is currently handled by the procedure `loader.addPropsAndConnectionInfoToSegment` in neuprint-load-procedures/procedures/LoadingProcedures. 
2. Ensure that this property is correctly handled by update procedures that change/add ConnectsTo relationships (e.g. `proofreader.addSynapseToSegment`, `proofreader.orphanSynapse`, `proofreader.addNeuron`) found in neuprint-procedures/proofreading/ProofreaderProcedures.

## Create a new node type
1. Decide on a label (e.g. `:Soma`). It's useful to have a unique property (not necessarily used by neuprint users) to ensure that duplicate nodes aren't created during loading/updates and can help with fast retrievals of these nodes later. For example, PreSyn and PostSyn nodes must have a unique `location` property, and SynapseSet nodes have a unique property `datasetBodyId` since there should only be one SynapseSet per segment per dataset. Every node should receive a minimum of three labels: :<dataset>, :<dataset>-newlabel, and :newlabel, where newlabel is the chosen label. This makes within-dataset queries faster while allowing across-dataset queries.
2. Decide which nodes this new node type should be connected to and make sure that these relationships will be correctly handled by any update/deletion procedures (e.g. in neuprint-procedures/proofreading). 
3. Incorporate this new node type into the loader (add to input jsons and modify neuprint/Neo4jImporter).

Note: may want to make a temporary update procedure to add these nodes to a live database.

## Create a new relationship type
1. Decide on a label (e.g. `:MergesTo`).
2. Decide which nodes this new relationship type should be connected to and make sure that these relationships will be correctly handled by any update/deletion procedures (e.g. in neuprint-procedures/proofreading). 
3. Incorporate this new relationship type into the loader (modify neuprint/Neo4jImporter).

Note: may want to make a temporary update procedure to add these relationships to a live database.

