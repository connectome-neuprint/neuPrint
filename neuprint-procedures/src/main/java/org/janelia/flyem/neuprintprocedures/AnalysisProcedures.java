package org.janelia.flyem.neuprintprocedures;

import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.result.NodeResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnalysisProcedures {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @Procedure(value = "analysis.getLineGraphForRoi", mode = Mode.READ)
    @Description("analysis.getLineGraph(roi,datasetLabel,synapseThreshold,vertexSynapseThreshold=50) : used to produce an edge-to-vertex dual graph, or line graph, for neurons within the provided ROI " +
            " with greater than synapseThreshold synapses. " +
            " Return value is a map with the vertex json under key \"Vertices\" and edge json under \"Edges\".  " +
            "e.g. CALL analysis.getLineGraphForRoi(roi,datasetLabel,synapseThreshold,vertexSynapseThreshold=50) YIELD value RETURN value.")
    public Stream<MapResult> getLineGraphForRoi(@Name("ROI") String roi, @Name("datasetLabel") String datasetLabel, @Name("synapseThreshold") Long synapseThreshold, @Name(value = "vertexSynapseThreshold", defaultValue = "50") Long vertexSynapseThreshold) {
        if (roi == null || datasetLabel == null || synapseThreshold == null) return Stream.empty();
        SynapticConnectionVertexMap synapticConnectionVertexMap = null;

        List<Long> bodyIdList = getNeuronBodyIdListFromRoi(roi, datasetLabel, synapseThreshold);
        System.out.println("Number of neurons within roi with greater than " + synapseThreshold + " synapses: " + bodyIdList.size());


        try {
            synapticConnectionVertexMap = getSynapticConnectionNodeMap(bodyIdList, datasetLabel);
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }


        String vertexJson = synapticConnectionVertexMap.getVerticesAboveThresholdAsJsonObjects(vertexSynapseThreshold);

        SynapticConnectionVertexMap synapticConnectionVertexMapFromJson = new SynapticConnectionVertexMap(vertexJson);
        String edgeJson = synapticConnectionVertexMapFromJson.getEdgesAsJsonObjects();

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("Vertices", vertexJson);
        jsonMap.put("Edges", edgeJson);

        //String graphJson = synapticConnectionVertexMapFromJson.getGraphJson(edgeJson,vertexJson);


        return Stream.of(new MapResult(jsonMap));

    }


    @Procedure(value = "analysis.getLineGraphForNeuron", mode = Mode.READ)
    @Description("analysis.getLineGraph(bodyId,datasetLabel,vertexSynapseThreshold=50) : used to produce an edge-to-vertex dual graph, or line graph, for a neuron." +
            " Return value is a map with the vertex json under key \"Vertices\" and edge json under \"Edges\".  " +
            "e.g. CALL analysis.getLineGraphForNeuron(bodyId,datasetLabel,vertexSynapseThreshold=50) YIELD value RETURN value.")
    public Stream<MapResult> getLineGraphForNeuron(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel, @Name(value = "vertexSynapseThreshold", defaultValue = "50") Long vertexSynapseThreshold) {
        //TODO: deal with null pointer exceptions when body doesn't exist etc.
        if (bodyId == null || datasetLabel == null) return Stream.empty();
        SynapticConnectionVertexMap synapticConnectionVertexMap = null;

        List<Long> bodyIdList = new ArrayList<>();
        //add selected body
        bodyIdList.add(bodyId);
        //add 1st degree connections to body
        //bodyIdList.addAll(getFirstDegreeConnectionsForNeuron(bodyId,datasetLabel,synapseThreshold));


        try {
            synapticConnectionVertexMap = getSynapticConnectionNodeMap(bodyIdList, datasetLabel);
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }

        String vertexJson = synapticConnectionVertexMap.getVerticesAboveThresholdAsJsonObjects(vertexSynapseThreshold);

        SynapticConnectionVertexMap synapticConnectionVertexMapFromJson = new SynapticConnectionVertexMap(vertexJson);
        String edgeJson = synapticConnectionVertexMapFromJson.getEdgesAsJsonObjects();

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("Vertices", vertexJson);
        jsonMap.put("Edges", edgeJson);

        //String graphJson = synapticConnectionVertexMapFromJson.getGraphJson(edgeJson,vertexJson);


        return Stream.of(new MapResult(jsonMap));


    }

    @Procedure(value = "analysis.calculateSkeletonDistance", mode = Mode.READ)
    @Description("Calculates the distance between two :SkelNodes for a body.")
    public Stream<LongResult> calculateSkeletonDistance(@Name("datasetLabel") String datasetLabel,
                                                        @Name("skelNodeA") Node skelNodeA, @Name("skelNodeB") Node skelNodeB) {
        //TODO: deal with situations in which skeleton doesn't exist or user inputs invalid parameters
        if (datasetLabel == null || skelNodeA == null || skelNodeB == null) return Stream.empty();


        //find nodes along path between node a and node b, inclusive
        List<Node> pathNodeList = getNodesAlongPath(skelNodeA, skelNodeB);

        // calculate the distance from start to finish
        Double distance = pathNodeList.stream()
                .map(n -> getSkelOrSynapseNodeLocation(n))
                .collect(DistanceHelper::new, DistanceHelper::add, DistanceHelper::combine).getSum();


        return Stream.of(new LongResult(Math.round(distance)));


    }

    @Procedure(value = "analysis.getNearestSkelNodeOnBodyToPoint", mode = Mode.READ)
    @Description("Returns the :SkelNode on the given body's skeleton that is closest to the provided point.")
    public Stream<NodeResult> getNearestSkelNodeOnBodyToPoint(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel,
                                                              @Name("x") Long x, @Name("y") Long y, @Name("z") Long z) {
        if (datasetLabel == null || bodyId == null || x == null || y == null || z == null) return Stream.empty();

        //Location synapseLocation = getSkelOrSynapseNodeLocation(synapse);
        Location location = new Location(x, y, z);
        Node neuron = acquireNeuronFromDatabase(bodyId, datasetLabel);

        //get all skelnodes for the skeleton and distances to point
        List<SkelNodeDistanceToPoint> skelNodeDistanceToPointList = getSkelNodesForSkeleton(neuron).stream()
                .map((s) -> new SkelNodeDistanceToPoint(s, getSkelOrSynapseNodeLocation(s), location))
                .collect(Collectors.toList());

        skelNodeDistanceToPointList.sort(new SortSkelNodeDistancesToPoint());


        return Stream.of(new NodeResult(skelNodeDistanceToPointList.get(0).getSkelNode()));


    }

    private List<Node> getSkelNodesForSkeleton(Node neuron) {

        Node nodeSkeletonNode = null;
        List<Node> skelNodeList = new ArrayList<>();
        for (Relationship nodeRelationship : neuron.getRelationships(RelationshipType.withName("Contains"))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label("Skeleton"))) {
                nodeSkeletonNode = containedNode;
            }
        }

        if (nodeSkeletonNode != null) {
            for (Relationship skeletonRelationship : nodeSkeletonNode.getRelationships(RelationshipType.withName("Contains"))) {
                Node containedNode = skeletonRelationship.getEndNode();
                if (containedNode.hasLabel(Label.label("SkelNode"))) {
                    skelNodeList.add(containedNode);
                }
            }
        }

        return skelNodeList;
    }


    private Location getSkelOrSynapseNodeLocation(Node node) {
        return new Location((Long) node.getProperty("x"), (Long) node.getProperty("y"), (Long) node.getProperty("z"));
    }

    private List<Node> getNodesAlongPath(Node skelNodeA, Node skelNodeB) {

        Map<String, Object> pathQueryResult = null;
        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("skelNodeIdA", skelNodeA.getProperty("skelNodeId"));
        parametersMap.put("skelNodeIdB", skelNodeB.getProperty("skelNodeId"));
        try {
            pathQueryResult = dbService.execute("MATCH p=(:SkelNode{skelNodeId:$skelNodeIdA})-[:LinksTo*]-(:SkelNode{skelNodeId:$skelNodeIdB}) RETURN nodes(p) AS nodeList", parametersMap).next();
        } catch (Exception e) {
            System.out.println("Error getting path between SkelNodes.");
            e.printStackTrace();
        }

        return (ArrayList<Node>) pathQueryResult.get("nodeList");

    }


    private List<Long> getNeuronBodyIdListFromRoi(String roi, String datasetLabel, Long synapseThreshold) {

        Map<String, Object> roiQueryResult = null;
        try {
            roiQueryResult = dbService.execute("MATCH (node:Neuron:" + datasetLabel + ":" + roi + ") WHERE (node.pre+node.post)>" + synapseThreshold + " WITH collect(node.bodyId) AS bodyIdList RETURN bodyIdList").next();
        } catch (Exception e) {
            System.out.println("Error getting node body ids for roi with name " + roi + ".");
            e.printStackTrace();
        }

        return (ArrayList<Long>) roiQueryResult.get("bodyIdList");

    }

    private List<Long> getFirstDegreeConnectionsForNeuron(Long bodyId, String datasetLabel, Long synapseThreshold) {

        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("nodeBodyId", bodyId);
        Map<String, Object> roiQueryResult = null;
        try {
            roiQueryResult = dbService.execute("MATCH (node:Neuron:" + datasetLabel + "{bodyId:$nodeBodyId})-[:ConnectsTo]-(p) WHERE (p.pre+p.post)>" + synapseThreshold + " WITH collect(p.bodyId) AS bodyIdList RETURN bodyIdList", parametersMap).next();
        } catch (Exception e) {
            System.out.println("Error getting node body ids connected to " + bodyId + ".");
            e.printStackTrace();
        }

        return (ArrayList<Long>) roiQueryResult.get("bodyIdList");

    }


    private Node acquireNeuronFromDatabase(Long nodeBodyId, String datasetLabel) {

        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("nodeBodyId", nodeBodyId);
        Map<String, Object> nodeQueryResult = null;
        try {
            nodeQueryResult = dbService.execute("MATCH (node:Neuron:" + datasetLabel + "{bodyId:$nodeBodyId}) RETURN node", parametersMap).next();
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

    private SynapticConnectionVertexMap getSynapticConnectionNodeMap(List<Long> neuronBodyIdList, String datasetLabel) {
        SynapticConnectionVertexMap synapticConnectionVertexMap = new SynapticConnectionVertexMap();

        for (Long neuronBodyId : neuronBodyIdList) {

            Node neuron = acquireNeuronFromDatabase(neuronBodyId, datasetLabel);
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
                                    synapticConnectionVertexMap.addSynapticConnection(categoryString, synapseNode, connectedSynapseNode);

                                } else if (synapseNode.hasLabel(Label.label("PostSyn"))) {
                                    categoryString = connectedNeuronBodyId + "_to_" + neuron.getProperty("bodyId");
                                    synapticConnectionVertexMap.addSynapticConnection(categoryString, connectedSynapseNode, synapseNode);
                                }


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

        return synapticConnectionVertexMap;
    }

}

class DistanceHelper {
    private double sum = 0;
    private Location first = null;
    private Location last = null;

    public void add(Location location) {
        if (this.first == null) {
            this.first = location;
        }
        if (this.last != null) {
            this.sum += Location.getDistanceBetweenLocations(location, this.last);
        }
        this.last = location;
    }

    public void combine(DistanceHelper otherHelper) {
        this.sum += otherHelper.sum;
        if (this.last != null && otherHelper.first != null) {
            this.sum += Location.getDistanceBetweenLocations(this.last, otherHelper.first);
        }
        this.last = otherHelper.last;
    }

    public double getSum() {
        return sum;
    }

}
