package org.janelia.flyem.neuprintprocedures;

import apoc.result.StringResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class AnalysisProcedures {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @Procedure(value = "analysis.getLineGraph", mode = Mode.READ)
    @Description("analysis.getLineGraph(roi,datasetLabel) : used to produce an edge-to-vertex dual graph, or line graph, for the :Big neurons within the provided ROI. " +
            "The returned value is a string containing the neuron json. Note: to be used with the neo4j driver (see neuprint-reader); large results will crash the neo4j browser." +
            " To get a JSON describing the vertices, " +
            "use the following query: CALL analysis.getLineGraph(roi,datasetLabel) YIELD value RETURN value.")
    public Stream<StringResult> getLineGraph(@Name("ROI") String roi, @Name("datasetLabel") String datasetLabel) {

        SynapticConnectionNodeMap synapticConnectionNodeMap = null;


        List<Long> bodyIdList = getBigNeuronBodyIdListFromRoi(roi, datasetLabel);
        System.out.println("Number of :Big neurons within roi: " + bodyIdList.size());


        try {
            synapticConnectionNodeMap = getSynapticConnectionNodeMap(bodyIdList, datasetLabel);
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }

        String nodeJson = synapticConnectionNodeMap.getNodesAsJsonObjects();

        System.out.println("Created vertex json.");


        return Stream.of(new StringResult(nodeJson));

    }


    private List<Long> getBigNeuronBodyIdListFromRoi(String roi, String datasetLabel) {

        Map<String, Object> roiQueryResult = null;
        try {
            roiQueryResult = dbService.execute("MATCH (node:Neuron:" + datasetLabel + ":" + roi + ":Big) WITH collect(node.bodyId) AS bodyIdList RETURN bodyIdList").next();
        } catch (Exception e) {
            System.out.println("Error getting node body ids for roi with name " + roi + ".:");
            e.printStackTrace();
        }

        return (ArrayList<Long>) roiQueryResult.get("bodyIdList");

    }


    private Node acquireBigNeuronFromDatabase(Long nodeBodyId, String datasetLabel) {

        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("nodeBodyId", nodeBodyId);
        Map<String, Object> nodeQueryResult = null;
        try {
            nodeQueryResult = dbService.execute("MATCH (node:Neuron:" + datasetLabel + ":Big{bodyId:$nodeBodyId}) RETURN node", parametersMap).next();
        } catch (java.util.NoSuchElementException nse) {
            System.out.println("Error using analysis procedures: Node must exist in the dataset and be labeled :Neuron.");
        }

        return (Node) nodeQueryResult.get("node");
    }

    private Node getSynapseSetForNode(final Node node) {

        Node nodeSynapseSetNode = null;
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("Contains"))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label("SynapseSet"))) {
                nodeSynapseSetNode = containedNode;
            }
        }
        return nodeSynapseSetNode;
    }

    private SynapticConnectionNodeMap getSynapticConnectionNodeMap(List<Long> neuronBodyIdList, String datasetLabel) {
        SynapticConnectionNodeMap synapticConnectionNodeMap = new SynapticConnectionNodeMap();

        for (Long neuronBodyId : neuronBodyIdList) {

            Node neuron = acquireBigNeuronFromDatabase(neuronBodyId, datasetLabel);
            Node neuronSynapseSet = getSynapseSetForNode(neuron);

            if (neuronSynapseSet != null) {
                for (Relationship synapseRelationship : neuronSynapseSet.getRelationships(Direction.OUTGOING, RelationshipType.withName("Contains"))) {
                    // get each synapse node
                    Node synapseNode = synapseRelationship.getEndNode();

                    //get all the synapses that connect to this neuron
                    for (Relationship synapsesToRelationship : synapseNode.getRelationships(RelationshipType.withName("SynapsesTo"))) {
                        Node connectedSynapseNode = synapsesToRelationship.getOtherNode(synapseNode);
                        if (!connectedSynapseNode.hasLabel(Label.label("createdforsynapsesto"))) {
                            Relationship synapseToSynapseSetRelationship = connectedSynapseNode.getSingleRelationship(RelationshipType
                                    .withName("Contains"), Direction.INCOMING);
                            if (synapseToSynapseSetRelationship != null) {
                                Node synapseSet = synapseToSynapseSetRelationship.getStartNode();
                                Relationship neuronToSynapseSetRelationship = synapseSet.getSingleRelationship(RelationshipType.withName("Contains"), Direction.INCOMING);
                                Node connectedNeuron = neuronToSynapseSetRelationship.getStartNode();
                                Long connectedNeuronBodyId = (Long) connectedNeuron.getProperty("bodyId");

                                String categoryString = null;

                                if (synapseNode.hasLabel(Label.label("PreSyn"))) {
                                    categoryString = neuron.getProperty("bodyId") + "_to_" + connectedNeuronBodyId;
                                    synapticConnectionNodeMap.addSynapticConnection(categoryString, synapseNode, connectedSynapseNode);
                                }
//                        } else if (synapseNode.hasLabel(Label.label("PostSyn"))) {
//                            categoryString = connectedNeuronBodyId + "_to_" + neuron.getProperty("bodyId");
//                            synapticConnectionNodeMap.addSynapticConnection(categoryString, connectedSynapseNode, synapseNode);
//                            System.out.println("Adding " + categoryString + " with prenode " + connectedSynapseNode + " and postnode " + synapseNode);
//                        }


                            } else {
                                System.out.println("No synapse set relationship found for synapse: " + connectedSynapseNode.getAllProperties());
                            }


                        } else {
                            System.out.println("Connected synapse is not associated with any neuron: " + connectedSynapseNode.getAllProperties());
                        }
                    }
                }
            } else {
                System.out.println("No synapse set found for neuron " + neuronBodyId);
            }
        }

        return synapticConnectionNodeMap;
    }

}
