package org.janelia.flyem.neuprintprocedures;

import apoc.result.MapResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

public class AnalysisProcedures {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @Procedure(value = "analysis.getLineGraph", mode = Mode.READ)
    @Description("analysis.getLineGraph(roi,datasetLabel) : produces an edge-to-vertex dual graph, or line graph, for the neurons within the provided ROI. To get JSONs describing edges and vertices, " +
            "use the following query: CALL analysis.getLineGraph(roi,datasetLabel) YIELD value RETURN value. The returned value is a map with keys \"Edges\" and \"Vertices\" that contain the appropriate JSONs.")
    public Stream<MapResult> getLineGraph(@Name("ROI") String roi, @Name("datasetLabel") String datasetLabel) {
        Map<String,Object> nodesAndEdges = new HashMap<>();

        SynapticConnectionNodeMap synapticConnectionNodeMap = null;

        try {

            List<Long> bodyIdList = getBigNeuronBodyIdListFromRoi(roi,datasetLabel);


            synapticConnectionNodeMap = getSynapticConnectionNodeMap(bodyIdList, datasetLabel);

            String nodeJson = synapticConnectionNodeMap.getNodesAsJsonObjects();
            System.out.println(nodeJson);
            nodesAndEdges.put("Vertices", nodeJson);
            String edgeJson = synapticConnectionNodeMap.getEdgesAsJsonObjects();
            System.out.println(edgeJson);
            nodesAndEdges.put("Edges", edgeJson);

        } catch (Exception e) {
            System.out.println(e);
        }


        return Stream.of(new MapResult(nodesAndEdges));

    }


    private List<Long> getBigNeuronBodyIdListFromRoi(String roi, String datasetLabel) {

        Map<String, Object> roiQueryResult = null;
        try {
            roiQueryResult = dbService.execute("MATCH (node:Neuron:" + datasetLabel + ":" + roi + ":Big) WITH collect(node.bodyId) AS bodyIdList RETURN bodyIdList").next();
        } catch (Exception e) {
            System.out.println("Error getting node body ids for roi with name " + roi + ".:" + Arrays.toString(e.getStackTrace()));
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
            System.exit(1);
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

            for (Relationship synapseRelationship : neuronSynapseSet.getRelationships(Direction.OUTGOING, RelationshipType.withName("Contains"))) {
                Node synapseNode = synapseRelationship.getEndNode();
                for (Relationship synapsesToRelationship : synapseNode.getRelationships(RelationshipType.withName("SynapsesTo"))) {
                    Node connectedSynapseNode = synapsesToRelationship.getOtherNode(synapseNode);
                    if (connectedSynapseNode != null) {
                        Node connectedNeuron = connectedSynapseNode.getSingleRelationship(RelationshipType
                                .withName("Contains"), Direction.INCOMING).getStartNode()
                                .getSingleRelationship(RelationshipType.withName("Contains"), Direction.INCOMING)
                                .getStartNode();
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
                    }
                }


            }
        }

        return synapticConnectionNodeMap;
    }
}
