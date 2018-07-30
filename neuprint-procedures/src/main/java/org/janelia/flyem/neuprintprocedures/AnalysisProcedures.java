package org.janelia.flyem.neuprintprocedures;

import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.result.NodeResult;
import apoc.result.StringResult;
import org.janelia.flyem.neuprinter.model.SkelNode;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
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
        String edgeJson = synapticConnectionVertexMapFromJson.getEdgesAsJsonObjects(false, dbService, datasetLabel, 0L); //bodyId 0; won't be relevant since cableDistance is false

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
    public Stream<MapResult> getLineGraphForNeuron(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel, @Name(value = "vertexSynapseThreshold", defaultValue = "50") Long vertexSynapseThreshold, @Name(value = "cableDistance", defaultValue = "false") Boolean cableDistance) {
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
        String edgeJson = synapticConnectionVertexMapFromJson.getEdgesAsJsonObjects(cableDistance, dbService, datasetLabel, bodyId);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("Vertices", vertexJson);
        jsonMap.put("Edges", edgeJson);

        //String graphJson = synapticConnectionVertexMapFromJson.getGraphJson(edgeJson,vertexJson);


        return Stream.of(new MapResult(jsonMap));


    }

    @Procedure(value = "analysis.getConnectionCentroidsAndSkeleton", mode = Mode.READ)
    @Description("Provides the synapse points and centroid for each type of synaptic connection (e.g. neuron A to neuron B) " +
            "present in the neuron with the provided bodyId as well as the skeleton for that body. Returned value is a map with the centroid json " +
            "under key \"Centroids\" and the skeleton json under key \"Skeleton\". " +
            "e.g. CALL analysis.getConnectionCentroidsAndSkeleton(bodyId,datasetLabel,vertexSynapseThreshold=50) YIELD value RETURN value.")
    public Stream<MapResult> getConnectionCentroidsAndSkeleton(@Name("bodyId") Long bodyId,
                                                               @Name("datasetLabel") String datasetLabel,
                                                               @Name(value = "vertexSynapseThreshold", defaultValue = "50") Long vertexSynapseThreshold) {

        if (bodyId == null || datasetLabel == null) return Stream.empty();
        SynapticConnectionVertexMap synapticConnectionVertexMap = null;

        List<Long> bodyIdList = new ArrayList<>();
        //add selected body
        bodyIdList.add(bodyId);

        try {
            synapticConnectionVertexMap = getSynapticConnectionNodeMap(bodyIdList, datasetLabel);
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }

        String centroidJson = synapticConnectionVertexMap.getVerticesAboveThresholdAsJsonObjects(vertexSynapseThreshold);

        //get skeleton points
        Node neuron = acquireNeuronFromDatabase(bodyId, datasetLabel);
        List<Node> nodeList = getSkelNodesForSkeleton(neuron);
        List<SkelNode> skelNodeList = nodeList.stream()
                .map((node) -> new SkelNode(bodyId, (String) node.getProperty("location"), (float) ((double) node.getProperty("radius")), (int) ((long) node.getProperty("rowNumber"))))
                .collect(Collectors.toList());
        String skeletonJson = SkelNode.getSkelNodeListJson(skelNodeList);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("Centroids", centroidJson);
        jsonMap.put("Skeleton", skeletonJson);
        return Stream.of(new MapResult(jsonMap));
    }

    @Procedure(value = "analysis.calculateSkeletonDistance", mode = Mode.READ)
    @Description("Calculates the distance between two :SkelNodes for a body.")
    public Stream<LongResult> calculateSkeletonDistance(@Name("datasetLabel") String datasetLabel,
                                                        @Name("skelNodeA") Node skelNodeA, @Name("skelNodeB") Node skelNodeB) {
        //TODO: deal with situations in which user inputs invalid parameters
        if (datasetLabel == null || skelNodeA == null || skelNodeB == null) return Stream.empty();
        if (skelNodeA.equals(skelNodeB)) return Stream.of(new LongResult(0L));


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

    @Procedure(value = "analysis.getInputAndOutputCountsForRois", mode = Mode.READ)
    @Description("")
    public Stream<StringResult> getInputAndOutputCountsForRois(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {
        if (datasetLabel == null || bodyId == null) return Stream.empty();
        // NOTE: assumes rois are mutually exclusive.
        Node neuron = acquireNeuronFromDatabase(bodyId, datasetLabel);

        RoiCounts roiCounts = getRoiCountsForNeuron(neuron, datasetLabel);

        String roiCountJson = roiCounts.roiCountsToJson();

        return Stream.of(new StringResult(roiCountJson));

    }

    @Procedure(value = "analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi", mode = Mode.READ)
    @Description("")
    public Stream<StringResult> getInputAndOutputFeatureVectorsForNeuronsInRoi(@Name("roi") String roi, @Name("datasetLabel") String datasetLabel, @Name("synapseThreshold") Long synapseThreshold) {
        if (datasetLabel == null || roi == null || synapseThreshold == null) return Stream.empty();


        //get all rois for dataset except for kc_alpha_roi and seven_column_roi
        List<String> roiList = getRoiListForDataset(datasetLabel).stream().sorted().collect(Collectors.toList());
        System.out.println("Rois in " + datasetLabel + ": " + roiList.size());
        System.out.println(roiList);

        List<Long> bodyIdList = getNeuronBodyIdListFromRoi(roi, datasetLabel, synapseThreshold);
        System.out.println("Number of neurons within roi with greater than " + synapseThreshold + " synapses: " + bodyIdList.size());

        //for each neuron get the number of inputs per roi
        //another vector with number of outputs per roi
        //to be normalized and/or combined into one vector later.
        Set<ClusteringFeatureVector> clusteringFeatureVectors = new HashSet<>();
        for (Long bodyId : bodyIdList) {
            Node neuron = acquireNeuronFromDatabase(bodyId, datasetLabel);
            RoiCounts roiCounts = getRoiCountsForNeuron(neuron, datasetLabel);
            long[] inputFeatureVector = new long[roiList.size()];
            long[] outputFeatureVector = new long[roiList.size()];
            for (int i = 0; i < roiList.size(); i++) {
                if (roiCounts.getRoiSynapseCount(roiList.get(i)) != null) {
                    inputFeatureVector[i] = roiCounts.getRoiSynapseCount(roiList.get(i)).getInputCount();
                    outputFeatureVector[i] = roiCounts.getRoiSynapseCount(roiList.get(i)).getOutputCount();
                }
            }
            clusteringFeatureVectors.add(new ClusteringFeatureVector(bodyId, inputFeatureVector, outputFeatureVector));
        }

        String featureVectorsJson = ClusteringFeatureVector.getClusteringFeatureVectorSetJson(clusteringFeatureVectors);

        return Stream.of(new StringResult(featureVectorsJson));
    }

    private RoiCounts getRoiCountsForNeuron(Node neuron, String datasetLabel) {

        Long totalInputCount = 0L;
        Long totalOutputCount = 0L;
        RoiCounts roiCounts = new RoiCounts();

        for (Relationship partOfRelationship : neuron.getRelationships(RelationshipType.withName("PartOf"))) {
            Node neuronPart = partOfRelationship.getStartNode();
            String roiName = "";
            for (Label label : neuronPart.getLabels()) {
                if (!label.equals(Label.label(datasetLabel)) && !label.equals(Label.label("NeuronPart"))) {
                    roiName = label.name();
                }
            }
            Long outputCount = (Long) neuronPart.getProperty("pre");
            totalOutputCount += outputCount;
            Long inputCount = (Long) neuronPart.getProperty("post");
            totalInputCount += inputCount;

            roiCounts.addRoiCount(roiName, inputCount, outputCount);

        }

        roiCounts.addRoiCount("total", totalInputCount, totalOutputCount);

        return roiCounts;
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
        } else {
            throw new Error("No skeleton for bodyId " + neuron.getProperty("bodyId"));
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
        String bigQuery = "MATCH (node:Neuron:" + datasetLabel + ":" + roi + ":Big) WHERE (node.pre+node.post)>" + synapseThreshold + " WITH collect(node.bodyId) AS bodyIdList RETURN bodyIdList";
        String smallQuery = "MATCH (node:Neuron:" + datasetLabel + ":" + roi + ") WHERE (node.pre+node.post)>" + synapseThreshold + " WITH collect(node.bodyId) AS bodyIdList RETURN bodyIdList";
        try {
            if (synapseThreshold > 10) {
                roiQueryResult = dbService.execute(bigQuery).next();
            } else {
                roiQueryResult = dbService.execute(smallQuery).next();
            }
        } catch (Exception e) {
            System.out.println("Error getting node body ids for roi with name " + roi + ".");
            e.printStackTrace();
        }

        return (ArrayList<Long>) roiQueryResult.get("bodyIdList");

    }

    private List<String> getRoiListForDataset(String datasetLabel) {

        String getRoiFromMeta = "MATCH (n:Meta:" + datasetLabel + ") RETURN n.rois AS rois";
        String getRoiFromNeurons = "MATCH (n:Neuron:" + datasetLabel + ") WITH labels(n) AS labs UNWIND labs AS labels WITH DISTINCT labels ORDER BY labels RETURN collect(labels) AS rois";

        Map<String, Object> roiListQueryResult = null;
        List<String> rois = null;
        try {
            try {
                roiListQueryResult = dbService.execute(getRoiFromMeta).next();
                List<String> labels = Arrays.asList((String[]) roiListQueryResult.get("rois"));
                rois = labels.stream()
                        .filter((l) -> (!l.equals("Neuron") && !l.equals(datasetLabel) && !l.equals("Big") && !l.equals("seven_column_roi") && !l.equals("kc_alpha_roi")))
                        .collect(Collectors.toList());
            } catch (NoSuchElementException nse) {
                System.out.println("No Meta node found for this dataset. Acquiring rois from Neuron nodes...");
                roiListQueryResult = dbService.execute(getRoiFromNeurons).next();
                List<String> labels = (ArrayList<String>) roiListQueryResult.get("rois");
                rois = labels.stream()
                        .filter((l) -> (!l.equals("Neuron") && !l.equals(datasetLabel) && !l.equals("Big") && !l.equals("seven_column_roi") && !l.equals("kc_alpha_roi")))
                        .collect(Collectors.toList());
            } catch (NullPointerException npe) {
                System.out.println("No rois property found on Meta node for this dataset. Acquiring rois from Neuron nodes...");
                roiListQueryResult = dbService.execute(getRoiFromNeurons).next();
                List<String> labels = (ArrayList<String>) roiListQueryResult.get("rois");
                rois = labels.stream()
                        .filter((l) -> (!l.equals("Neuron") && !l.equals(datasetLabel) && !l.equals("Big") && !l.equals("seven_column_roi") && !l.equals("kc_alpha_roi")))
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            System.out.println("Error getting roi list from " + datasetLabel + ".");
            e.printStackTrace();
        }


        return rois;

    }

    private List<Long> getFirstDegreeConnectionsForNeuron(Long bodyId, String datasetLabel, Long synapseThreshold) {

        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("nodeBodyId", bodyId);
        Map<String, Object> roiQueryResult = null;
        String bigQuery = "MATCH (node:Neuron:" + datasetLabel + "{bodyId:$nodeBodyId})-[:ConnectsTo]-(p:Neuron:Big) WHERE (p.pre+p.post)>" + synapseThreshold + " WITH collect(p.bodyId) AS bodyIdList RETURN bodyIdList";
        String smallQuery = "MATCH (node:Neuron:" + datasetLabel + "{bodyId:$nodeBodyId})-[:ConnectsTo]-(p:Neuron) WHERE (p.pre+p.post)>" + synapseThreshold + " WITH collect(p.bodyId) AS bodyIdList RETURN bodyIdList";
        try {
            if (synapseThreshold > 10) {
                roiQueryResult = dbService.execute(bigQuery, parametersMap).next();
            } else {
                roiQueryResult = dbService.execute(smallQuery, parametersMap).next();
            }
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
