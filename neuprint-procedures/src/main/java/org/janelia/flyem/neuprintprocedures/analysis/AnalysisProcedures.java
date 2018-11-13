package org.janelia.flyem.neuprintprocedures.analysis;

import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.result.NodeResult;
import apoc.result.StringResult;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.model.SkelNode;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.janelia.flyem.neuprintprocedures.Location;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.*;

public class AnalysisProcedures {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

//    @Procedure(value = "analysis.getLineGraphForRoi", mode = Mode.READ)
//    @Description("analysis.getLineGraph(roi,datasetLabel,synapseThreshold,vertexSynapseThreshold=50) : used to produce an edge-to-vertex dual graph, or line graph, for neurons within the provided ROI " +
//            " with greater than synapseThreshold synapses. " +
//            " Return value is a map with the vertex json under key \"Vertices\" and edge json under \"Edges\".  " +
//            "e.g. CALL analysis.getLineGraphForRoi(roi,datasetLabel,synapseThreshold,vertexSynapseThreshold=50) YIELD value RETURN value.")
//    public Stream<MapResult> getLineGraphForRoi(@Name("ROI") String roi, @Name("datasetLabel") String datasetLabel, @Name("synapseThreshold") Long synapseThreshold, @Name(value = "vertexSynapseThreshold", defaultValue = "50") Long vertexSynapseThreshold) {
//        if (roi == null || datasetLabel == null || synapseThreshold == null) return Stream.empty();
//        SynapticConnectionVertexMap synapticConnectionVertexMap = null;
//
//        Set<Long> bodyIdSet = getNeuronSetFromRoi(roi, 0L, datasetLabel, synapseThreshold);
//        log.info(String.format("Number of neurons within roi with greater than %d synapses: %d", synapseThreshold, bodyIdSet.size()));
//
//        try {
//            synapticConnectionVertexMap = getSynapticConnectionNodeMap(bodyIdSet, datasetLabel);
//        } catch (NullPointerException npe) {
//            npe.printStackTrace();
//        }
//
//        assert synapticConnectionVertexMap != null;
//        String vertexJson = synapticConnectionVertexMap.getVerticesAboveThresholdAsJsonObjects(vertexSynapseThreshold);
//
//        SynapticConnectionVertexMap synapticConnectionVertexMapFromJson = new SynapticConnectionVertexMap(vertexJson);
//        String edgeJson = synapticConnectionVertexMapFromJson.getEdgesAsJsonObjects(false, dbService, datasetLabel, 0L); //bodyId 0; won't be relevant since cableDistance is false
//
//        Map<String, Object> jsonMap = new HashMap<>();
//        jsonMap.put("Vertices", vertexJson);
//        jsonMap.put("Edges", edgeJson);
//
//        //String graphJson = synapticConnectionVertexMapFromJson.getGraphJson(edgeJson,vertexJson);
//
//        return Stream.of(new MapResult(jsonMap));
//
//    }

    @Procedure(value = "analysis.getLineGraphForNeuron", mode = Mode.READ)
    @Description("analysis.getLineGraph(bodyId,datasetLabel,vertexSynapseThreshold=50) : used to produce an edge-to-vertex dual graph, or line graph, for a neuron." +
            " Return value is a map with the vertex json under key \"Vertices\" and edge json under \"Edges\".  " +
            "e.g. CALL analysis.getLineGraphForNeuron(bodyId,datasetLabel,vertexSynapseThreshold=50) YIELD value RETURN value.")
    public Stream<MapResult> getLineGraphForNeuron(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel, @Name(value = "vertexSynapseThreshold", defaultValue = "50") Long vertexSynapseThreshold, @Name(value = "cableDistance", defaultValue = "false") Boolean cableDistance) {
        //TODO: deal with null pointer exceptions when body doesn't exist etc.
        if (bodyId == null || datasetLabel == null) return Stream.empty();
        SynapticConnectionVertexMap synapticConnectionVertexMap = null;

        Set<Long> bodyIdSet = new HashSet<>();
        //add selected body
        bodyIdSet.add(bodyId);
        //add 1st degree connections to body
        //bodyIdSet.addAll(getFirstDegreeConnectionsForNeuron(bodyId,datasetLabel,synapseThreshold));

        try {
            synapticConnectionVertexMap = getSynapticConnectionNodeMap(bodyIdSet, datasetLabel);
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }

        assert synapticConnectionVertexMap != null;
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

        Set<Long> bodyIdSet = new HashSet<>();
        //add selected body
        bodyIdSet.add(bodyId);

        try {
            synapticConnectionVertexMap = getSynapticConnectionNodeMap(bodyIdSet, datasetLabel);
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }

        assert synapticConnectionVertexMap != null;
        String centroidJson = synapticConnectionVertexMap.getVerticesAboveThresholdAsJsonObjects(vertexSynapseThreshold);

        //get skeleton points
        Node neuron = acquireSegmentFromDatabase(bodyId, datasetLabel);
        List<Node> nodeList = getSkelNodesForSkeleton(neuron);
        List<SkelNode> skelNodeList = nodeList.stream()
                .map((node) -> new SkelNode(bodyId, getNeo4jPointLocationAsLocationList((Point) node.getProperty("location")), (float) ((double) node.getProperty("radius")), (int) ((long) node.getProperty("rowNumber"))))
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
        List<Node> pathNodeList = getNodesAlongPath(skelNodeA, skelNodeB, datasetLabel);

        // calculate the distance from start to finish
        Double distance = pathNodeList.stream()
                .map(AnalysisProcedures::getSkelOrSynapseNodeLocation)
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
        Node neuron = acquireSegmentFromDatabase(bodyId, datasetLabel);

        //get all skelnodes for the skeleton and distances to point
        List<SkelNodeDistanceToPoint> skelNodeDistanceToPointList = getSkelNodesForSkeleton(neuron)
                .stream()
                .map((s) -> new SkelNodeDistanceToPoint(s, getSkelOrSynapseNodeLocation(s), location))
                .sorted(new SortSkelNodeDistancesToPoint()).collect(Collectors.toList());

        return Stream.of(new NodeResult(skelNodeDistanceToPointList.get(0).getSkelNode()));

    }

    @Procedure(value = "analysis.getInputAndOutputCountsForRois", mode = Mode.READ)
    @Description("")
    public Stream<StringResult> getInputAndOutputCountsForRois(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {
        if (datasetLabel == null || bodyId == null) return Stream.empty();
        // NOTE: assumes rois are mutually exclusive.
        Node neuron = acquireSegmentFromDatabase(bodyId, datasetLabel);

        String roiInfo = (String) neuron.getProperty("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> roiInfoMap = gson.fromJson(roiInfo, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
        long pre = (long) neuron.getProperty("pre");
        long post = (long) neuron.getProperty("post");

        NeuronWithRoiInfoMap neuronWithRoiInfoMap = new NeuronWithRoiInfoMap(neuron, bodyId, roiInfoMap, pre, post);

        Map<String, SynapseCounter> synapseCounterMap = getSynapseCounterMapForNeuron(neuronWithRoiInfoMap);

        String roiCountJson = gson.toJson(synapseCounterMap, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        return Stream.of(new StringResult(roiCountJson));

    }

    @Procedure(value = "analysis.getInputAndOutputFeatureVectorsForNeuronsInRoiAndTopXFirstDegreeConnections", mode = Mode.READ)
    @Description("")
    public Stream<StringResult> getInputAndOutputFeatureVectorsForNeuronsInRoiAndTopXFirstDegreeConnections(@Name("roi") String roi,
                                                                                                            @Name("roiSynapseThreshold") Long roiSynapseThreshold,
                                                                                                            @Name("datasetLabel") String datasetLabel,
                                                                                                            @Name("synapseThreshold") Long synapseThreshold,
                                                                                                            @Name("numberOf1stDegreeConnections") Long numberOf1stDegreeConnections) {
        if (datasetLabel == null || roi == null || roiSynapseThreshold == null || synapseThreshold == null || numberOf1stDegreeConnections == null) {
            log.error("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoiAndTopXFirstDegreeConnections.");
            throw new RuntimeException("Missing input arguments.");
        }

        //get all rois for dataset except for kc_alpha_roi and seven_column_roi
        List<String> roiList = getRoiListForDataset(datasetLabel).stream().sorted().collect(Collectors.toList());
        log.info("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoiAndTopXFirstDegreeConnections: " + roiList.size() + " rois in " + datasetLabel);
        log.info(roiList.toString());

        Set<NeuronWithRoiInfoMap> neuronSet = getNeuronSetFromRoi(roi, roiSynapseThreshold, datasetLabel, synapseThreshold);
        log.info("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi: " + neuronSet.size() + " neurons within roi " +
                "with greater than " + synapseThreshold + " total synapses and greater than " + roiSynapseThreshold + " synapses within roi.");

        ClusteringFeatureVectorStore clusteringFeatureVectorStore = new ClusteringFeatureVectorStore();

        for (NeuronWithRoiInfoMap neuronWithRoiInfoMap : neuronSet) {
            Node neuron = neuronWithRoiInfoMap.getNeuron();
            Map<String, SynapseCounter> synapseCounterMap = getSynapseCounterMapForNeuron(neuronWithRoiInfoMap);
            long[] inputFeatureVector = new long[roiList.size()];
            long[] outputFeatureVector = new long[roiList.size()];
            for (int i = 0; i < roiList.size(); i++) {
                if (synapseCounterMap.get(roiList.get(i)) != null) {
                    inputFeatureVector[i] = synapseCounterMap.get(roiList.get(i)).getPost();
                    outputFeatureVector[i] = synapseCounterMap.get(roiList.get(i)).getPre();
                }
            }

            Map<String, Object> topXFirstDegreeConnectionsInfo = getTopXFirstDegreeConnectionsForNeuron(neuronWithRoiInfoMap.getBodyId(), datasetLabel, numberOf1stDegreeConnections);
            List<Node> topXFirstDegreeConnections = (ArrayList<Node>) topXFirstDegreeConnectionsInfo.get("neuronList");
            List<Boolean> topXFirstDegreeConnectionsPolarity = (ArrayList<Boolean>) topXFirstDegreeConnectionsInfo.get("outList"); //true is output from original body, false is input
            Set<Long> inputIds = new HashSet<>();
            Set<Long> outputIds = new HashSet<>();
//            NeuronConnections neuronConnections = new NeuronConnections(neuronWithRoiInfoMap.getBodyId());

            for (int c = 0; c < topXFirstDegreeConnections.size(); c++) {

                Node firstDegreeConnection = topXFirstDegreeConnections.get(c);
                Map<String, Object> firstDegreeProperties = firstDegreeConnection.getProperties("bodyId", "pre", "post", "roiInfo");

                Gson gson = new Gson();
                Map<String, SynapseCounter> roiInfo = gson.fromJson((String) firstDegreeProperties.get("roiInfo"), new TypeToken<Map<String, SynapseCounter>>() {
                }.getType());
                long pre = (long) firstDegreeProperties.get("pre");
                long post = (long) firstDegreeProperties.get("post");
                long firstDegreeBodyId = (long) firstDegreeProperties.get("bodyId");
                NeuronWithRoiInfoMap firstDegreeConnectionRoiInfoMap = new NeuronWithRoiInfoMap(firstDegreeConnection, firstDegreeBodyId, roiInfo, pre, post);
                Map<String, SynapseCounter> firstDegreeSynapseCounterMap = getSynapseCounterMapForNeuron(firstDegreeConnectionRoiInfoMap);

                long[] firstDegreeInputFeatureVector = new long[roiList.size()];
                long[] firstDegreeOutputFeatureVector = new long[roiList.size()];
                for (int i = 0; i < roiList.size(); i++) {
                    if (firstDegreeSynapseCounterMap.get(roiList.get(i)) != null) {
                        firstDegreeInputFeatureVector[i] = firstDegreeSynapseCounterMap.get(roiList.get(i)).getPost();
                        firstDegreeOutputFeatureVector[i] = firstDegreeSynapseCounterMap.get(roiList.get(i)).getPre();
                    }
                }

                //add first degree connection
                clusteringFeatureVectorStore.addClusteringFeatureVector(firstDegreeBodyId, firstDegreeInputFeatureVector, firstDegreeOutputFeatureVector);

                if (topXFirstDegreeConnectionsPolarity.get(c)) {
                    outputIds.add(firstDegreeBodyId);
                } else {
                    inputIds.add(firstDegreeBodyId);
                }

            }
            //add primary neuron
            clusteringFeatureVectorStore.addClusteringFeatureVector(neuronWithRoiInfoMap.getBodyId(), inputFeatureVector, outputFeatureVector);
            try {
                clusteringFeatureVectorStore.addInputAndOutputIds(neuronWithRoiInfoMap.getBodyId(), inputIds, outputIds);
            } catch (Exception e) {
                log.error("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoiAndTopXFirstDegreeConnections: Error adding feature vector input and output ids to store.");
                throw new RuntimeException("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoiAndTopXFirstDegreeConnections: Error adding feature vector input and output ids to store.");
            }

        }

        String featureVectorsJson = ClusteringFeatureVector.getClusteringFeatureVectorSetJson(clusteringFeatureVectorStore.getClusteringFeatureVectorStoreAsSet());

        return Stream.of(new StringResult(featureVectorsJson));

    }

    @Procedure(value = "analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi", mode = Mode.READ)
    @Description("")
    public Stream<StringResult> getInputAndOutputFeatureVectorsForNeuronsInRoi(@Name("roi") String roi,
                                                                               @Name("roiSynapseThreshold") Long roiSynapseThreshold,
                                                                               @Name("datasetLabel") String datasetLabel,
                                                                               @Name("synapseThreshold") Long synapseThreshold) {
        if (datasetLabel == null || roi == null || roiSynapseThreshold == null || synapseThreshold == null) {
            log.error("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi: Missing input arguments.");
            throw new RuntimeException("Missing input arguments.");
        }

        //get all rois for dataset except for kc_alpha_roi and seven_column_roi
        List<String> roiList = getRoiListForDataset(datasetLabel).stream().sorted().collect(Collectors.toList());
        log.info("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi: " + roiList.size() + " rois in " + datasetLabel);
        log.info(roiList.toString());

        Set<NeuronWithRoiInfoMap> neuronSet = getNeuronSetFromRoi(roi, roiSynapseThreshold, datasetLabel, synapseThreshold);
        log.info("analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi: " + neuronSet.size() + " neurons within roi " +
                "with greater than " + synapseThreshold + " total synapses and greater than " + roiSynapseThreshold + " synapses within roi.");

        Set<ClusteringFeatureVector> clusteringFeatureVectors = getSetOfClusteringFeatureVectors(neuronSet, roiList);

        String featureVectorsJson = ClusteringFeatureVector.getClusteringFeatureVectorSetJson(clusteringFeatureVectors);

        return Stream.of(new StringResult(featureVectorsJson));
    }

    @Procedure(value = "analysis.getInputAndOutputFeatureVectorsForAllNeurons", mode = Mode.READ)
    @Description("")
    public Stream<StringResult> getInputAndOutputFeatureVectorsForAllNeurons(@Name("datasetLabel") String datasetLabel,
                                                                               @Name("synapseThreshold") Long synapseThreshold) {
        if (datasetLabel == null || synapseThreshold == null) {
            log.error("analysis.getInputAndOutputFeatureVectorsForAllNeurons: Missing input arguments.");
            throw new RuntimeException("analysis.getInputAndOutputFeatureVectorsForAllNeurons: Missing input arguments.");
        }

        //get all rois for dataset except for kc_alpha_roi and seven_column_roi
        List<String> roiList = getRoiListForDataset(datasetLabel).stream().sorted().collect(Collectors.toList());
        log.info("analysis.getInputAndOutputFeatureVectorsForAllNeurons: " + roiList.size() + " rois in " + datasetLabel);
        log.info(roiList.toString());

        Set<NeuronWithRoiInfoMap> neuronSet = getNeuronSetFromDataset(datasetLabel, synapseThreshold);
        log.info("analysis.getInputAndOutputFeatureVectorsForAllNeurons: " + neuronSet.size() + " neurons within " + datasetLabel +
                " dataset with greater than " + synapseThreshold + " total synapses.");


        Set<ClusteringFeatureVector> clusteringFeatureVectors = getSetOfClusteringFeatureVectors(neuronSet, roiList);

        String featureVectorsJson = ClusteringFeatureVector.getClusteringFeatureVectorSetJson(clusteringFeatureVectors);

        return Stream.of(new StringResult(featureVectorsJson));
    }

    private Set<ClusteringFeatureVector> getSetOfClusteringFeatureVectors(Set<NeuronWithRoiInfoMap> neuronSet, List<String> roiList) {

        //for each neuron get the number of inputs per roi
        //another vector with number of outputs per roi
        //to be normalized and/or combined into one vector later.
        Set<ClusteringFeatureVector> clusteringFeatureVectors = new HashSet<>();
        for (NeuronWithRoiInfoMap neuron : neuronSet) {
            Map<String, SynapseCounter> synapseCounterMap = getSynapseCounterMapForNeuron(neuron);
            long[] inputFeatureVector = new long[roiList.size()];
            long[] outputFeatureVector = new long[roiList.size()];
            for (int i = 0; i < roiList.size(); i++) {
                if (synapseCounterMap.get(roiList.get(i)) != null) {
                    inputFeatureVector[i] = synapseCounterMap.get(roiList.get(i)).getPost();
                    outputFeatureVector[i] = synapseCounterMap.get(roiList.get(i)).getPre();
                }
            }
            clusteringFeatureVectors.add(new ClusteringFeatureVector(neuron.getBodyId(), inputFeatureVector, outputFeatureVector));
        }

        return clusteringFeatureVectors;

    }

    private Map<String, SynapseCounter> getSynapseCounterMapForNeuron(NeuronWithRoiInfoMap neuron) {

//        String synapseCountPerRoiJson = (String) neuron.getProperty(ROI_INFO);
//        Gson gson = new Gson();
//        Map<String, SynapseCounter> synapseCountPerRoiMap = gson.fromJson(synapseCountPerRoiJson, new TypeToken<Map<String, SynapseCounter>>() {
//        }.getType());
        Map<String, SynapseCounter> synapseCountPerRoiMap = new HashMap<>(neuron.getRoiInfoMap());
        synapseCountPerRoiMap.put("total", new SynapseCounter(Math.toIntExact(neuron.getPre()), Math.toIntExact(neuron.getPost())));

        return synapseCountPerRoiMap;
    }

    private List<Node> getSkelNodesForSkeleton(Node neuron) {

        Node nodeSkeletonNode = null;
        List<Node> skelNodeList = new ArrayList<>();
        for (Relationship nodeRelationship : neuron.getRelationships(RelationshipType.withName(CONTAINS))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label(SKELETON))) {
                nodeSkeletonNode = containedNode;
            }
        }

        if (nodeSkeletonNode != null) {
            for (Relationship skeletonRelationship : nodeSkeletonNode.getRelationships(RelationshipType.withName(CONTAINS))) {
                Node containedNode = skeletonRelationship.getEndNode();
                if (containedNode.hasLabel(Label.label(SKEL_NODE))) {
                    skelNodeList.add(containedNode);
                }
            }
        } else {
            throw new Error("No skeleton for bodyId " + neuron.getProperty(BODY_ID));
        }

        return skelNodeList;
    }

    static Location getSkelOrSynapseNodeLocation(Node node) {
        List<Integer> locationList = getNeo4jPointLocationAsLocationList((Point) node.getProperty(LOCATION));
        return new Location((long) locationList.get(0), (long) locationList.get(1), (long) locationList.get(2));
    }

    private static List<Integer> getNeo4jPointLocationAsLocationList(Point neo4jPoint) {
        return neo4jPoint.getCoordinate().getCoordinate().stream().map(d -> (int) Math.round(d)).collect(Collectors.toList());
    }

    private List<Node> getNodesAlongPath(Node skelNodeA, Node skelNodeB, String datasetLabel) {

        Map<String, Object> pathQueryResult = null;
        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("skelNodeIdA", skelNodeA.getProperty(SKEL_NODE_ID));
        parametersMap.put("skelNodeIdB", skelNodeB.getProperty(SKEL_NODE_ID));
        try {
            pathQueryResult = dbService.execute("MATCH p=(:`" + datasetLabel + "-SkelNode`{skelNodeId:$skelNodeIdA})-[:LinksTo*]-(:`" + datasetLabel + "-SkelNode`{skelNodeId:$skelNodeIdB}) RETURN nodes(p) AS nodeList", parametersMap).next();
        } catch (Exception e) {
            log.error("Error getting path between SkelNodes.");
            e.printStackTrace();
        }

        return (ArrayList<Node>) pathQueryResult.get("nodeList");

    }

    private Set<NeuronWithRoiInfoMap> getNeuronSetFromDataset(String datasetLabel, Long synapseThreshold) {

        ResourceIterator<Node> nodes = dbService.findNodes(Label.label(datasetLabel + "-Neuron"));
        Set<NeuronWithRoiInfoMap> relevantNeuronNodes = new HashSet<>();
        Gson gson = new Gson();
        while (nodes.hasNext()) {
            Node currentNode = nodes.next();
            Map<String, Object> properties = currentNode.getProperties("pre", "post", "roiInfo", "bodyId");
            Map<String, SynapseCounter> roiInfo = gson.fromJson((String) properties.get("roiInfo"), new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());
            long pre = (long) properties.get("pre");
            long post = (long) properties.get("post");
            if ((pre + post) > synapseThreshold) {
                relevantNeuronNodes.add(new NeuronWithRoiInfoMap(currentNode, (long) properties.get("bodyId"), roiInfo, pre, post));
            }
        }
//        Map<String, Object> roiQueryResult = null;
//        String bigQuery = "MATCH (node:`" + datasetLabel + "-Neuron`{`" + roi + "`:true}) WHERE (node.pre+node.post)>" + synapseThreshold +
//                " AND apoc.convert.fromJsonMap(node.roiInfo).`" + roi + "`.pre+apoc.convert.fromJsonMap(node.roiInfo).`" + roi + "`.post>" + roiSynapseThreshold + " WITH collect(node.bodyId) AS bodyIdList RETURN bodyIdList";
//            try {
//            roiQueryResult = dbService.execute(bigQuery).next();
//        } catch (Exception e) {
//            log.error("getNeuronSetFromRoi: Error getting node body ids for roi with name " + roi + ".");
//            e.printStackTrace();
//        }

//        return new HashSet<>((ArrayList<Long>) roiQueryResult.get("bodyIdList"));

        return relevantNeuronNodes;

    }

    private Set<NeuronWithRoiInfoMap> getNeuronSetFromRoi(String roi, Long roiSynapseThreshold, String datasetLabel, Long synapseThreshold) {

        ResourceIterator<Node> nodes = dbService.findNodes(Label.label(datasetLabel + "-Neuron"), roi, true);
        Set<NeuronWithRoiInfoMap> relevantNeuronNodes = new HashSet<>();
        Gson gson = new Gson();
        while (nodes.hasNext()) {
            Node currentNode = nodes.next();
            Map<String, Object> properties = currentNode.getProperties("pre", "post", "roiInfo", "bodyId");
            Map<String, SynapseCounter> roiInfo = gson.fromJson((String) properties.get("roiInfo"), new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());
            long pre = (long) properties.get("pre");
            long post = (long) properties.get("post");
            long roiSynapseTotal = roiInfo.get(roi).getPre() + roiInfo.get(roi).getPost();
            if ((pre + post) > synapseThreshold && roiSynapseTotal > roiSynapseThreshold) {
                relevantNeuronNodes.add(new NeuronWithRoiInfoMap(currentNode, (long) properties.get("bodyId"), roiInfo, pre, post));
            }
        }
//        Map<String, Object> roiQueryResult = null;
//        String bigQuery = "MATCH (node:`" + datasetLabel + "-Neuron`{`" + roi + "`:true}) WHERE (node.pre+node.post)>" + synapseThreshold +
//                " AND apoc.convert.fromJsonMap(node.roiInfo).`" + roi + "`.pre+apoc.convert.fromJsonMap(node.roiInfo).`" + roi + "`.post>" + roiSynapseThreshold + " WITH collect(node.bodyId) AS bodyIdList RETURN bodyIdList";
//            try {
//            roiQueryResult = dbService.execute(bigQuery).next();
//        } catch (Exception e) {
//            log.error("getNeuronSetFromRoi: Error getting node body ids for roi with name " + roi + ".");
//            e.printStackTrace();
//        }

//        return new HashSet<>((ArrayList<Long>) roiQueryResult.get("bodyIdList"));

        return relevantNeuronNodes;

    }

    private List<String> getRoiListForDataset(String datasetLabel) {

        Node metaNode = dbService.findNode(Label.label("Meta"), "dataset", datasetLabel);

        String roiInfo = (String) metaNode.getProperty("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> roiInfoMap = gson.fromJson(roiInfo, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        //        String getRoiFromMeta = "MATCH (n:Meta{dataset:\"" + datasetLabel + "\"}) RETURN keys(apoc.convert.fromJsonMap(n.roiInfo)) AS rois";
//
//        Map<String, Object> roiListQueryResult;
//        List<String> rois = null;
//        try {
//            roiListQueryResult = dbService.execute(getRoiFromMeta).next();
//            List<String> labels = (ArrayList<String>) roiListQueryResult.get("rois");
//            rois = labels.stream()
//                    .filter((l) -> (!l.equals("seven_column_roi") && !l.equals("kc_alpha_roi")))
//                    .collect(Collectors.toList());
//        } catch (NoSuchElementException nse) {
//            throw new RuntimeException("getRoiListForDataset: No Meta node found for this dataset.");
//        } catch (NullPointerException npe) {
//            throw new RuntimeException("getRoiListForDataset: No rois property found on Meta node for this dataset.");
//        } catch (Exception e) {
//            log.error("getRoiListForDataset: Error getting roi list from " + datasetLabel + ".");
//            e.printStackTrace();
//        }
        return roiInfoMap.keySet().stream().filter(l -> (!l.equals("seven_column_roi") && !l.equals("kc_alpha_roi"))).collect(Collectors.toList());
    }

    private Map<String, Object> getTopXFirstDegreeConnectionsForNeuron(Long bodyId, String datasetLabel, long maxFirstDegreeConnections) {

        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("nodeBodyId", bodyId);
        Map<String, Object> roiQueryResult = null;
        String firstDegreeConnectionQuery = "MATCH (node:`" + datasetLabel + "-Neuron`{bodyId:$nodeBodyId})-[r:ConnectsTo]-(p:`" + datasetLabel + "-Neuron`) WITH p AS connection, " +
                "startNode(r)=node AS out " +
                "ORDER BY r.weight DESC LIMIT " + maxFirstDegreeConnections + " RETURN collect(connection) AS neuronList, collect(out) AS outList";
        try {
            roiQueryResult = dbService.execute(firstDegreeConnectionQuery, parametersMap).next();
        } catch (Exception e) {
            log.error("Error getting node body ids connected to " + bodyId + ".");
            e.printStackTrace();
        }

        return roiQueryResult;

    }

    private Node acquireSegmentFromDatabase(Long nodeBodyId, String datasetLabel) throws Error {

        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put(BODY_ID, nodeBodyId);
        Map<String, Object> nodeQueryResult;
        Node foundNode;

        try {
            nodeQueryResult = dbService.execute("MATCH (node:`" + datasetLabel + "-Segment`{bodyId:$bodyId}) RETURN node", parametersMap).next();
        } catch (java.util.NoSuchElementException nse) {
            log.error(String.format("acquireSegmentFromDatabase: Error using analysis procedures: Node must exist in the dataset and be labeled :%s-%s.", datasetLabel, SEGMENT));
            nse.printStackTrace();
            throw new RuntimeException(String.format("acquireSegmentFromDatabase: Error using analysis procedures: Node must exist in the dataset and be labeled :%s-%s.", datasetLabel, SEGMENT));
        }

        try {
            foundNode = (Node) nodeQueryResult.get("node");
        } catch (NullPointerException npe) {
            npe.printStackTrace();
            throw new RuntimeException(String.format("acquireSegmentFromDatabase: Error using analysis procedures: Node must exist in the dataset and be labeled :%s-%s.", datasetLabel, SEGMENT));
        }

        return foundNode;

    }

    private Node getSynapseSetForNode(final Node node) {

        Node nodeSynapseSetNode = null;
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName(CONTAINS))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label(SYNAPSE_SET))) {
                nodeSynapseSetNode = containedNode;
            }
        }
        return nodeSynapseSetNode;
    }

    private SynapticConnectionVertexMap getSynapticConnectionNodeMap(Set<Long> neuronBodyIdSet, String datasetLabel) {
        SynapticConnectionVertexMap synapticConnectionVertexMap = new SynapticConnectionVertexMap();

        for (Long neuronBodyId : neuronBodyIdSet) {

            Node neuron = acquireSegmentFromDatabase(neuronBodyId, datasetLabel);
            Node neuronSynapseSet = getSynapseSetForNode(neuron);

            if (neuronSynapseSet != null) {
                for (Relationship synapseRelationship : neuronSynapseSet.getRelationships(Direction.OUTGOING, RelationshipType.withName(CONTAINS))) {
                    // get each synapse node
                    Node synapseNode = synapseRelationship.getEndNode();

                    //get all the synapses that connect to this neuron
                    for (Relationship synapsesToRelationship : synapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                        Node connectedSynapseNode = synapsesToRelationship.getOtherNode(synapseNode);
                        if (!connectedSynapseNode.hasLabel(Label.label("createdforsynapsesto"))) {
                            Relationship synapseToSynapseSetRelationship = connectedSynapseNode.getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING);
                            if (synapseToSynapseSetRelationship != null) {
                                Node synapseSet = synapseToSynapseSetRelationship.getStartNode();
                                Relationship neuronToSynapseSetRelationship = synapseSet.getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING);
                                Node connectedNeuron = neuronToSynapseSetRelationship.getStartNode();
                                Long connectedNeuronBodyId = (Long) connectedNeuron.getProperty(BODY_ID);

                                String categoryString;

                                if (synapseNode.hasLabel(Label.label(PRE_SYN))) {
                                    categoryString = neuron.getProperty(BODY_ID) + "_to_" + connectedNeuronBodyId;
                                    synapticConnectionVertexMap.addSynapticConnection(categoryString, synapseNode, connectedSynapseNode);

                                } else if (synapseNode.hasLabel(Label.label(POST_SYN))) {
                                    categoryString = connectedNeuronBodyId + "_to_" + neuron.getProperty(BODY_ID);
                                    synapticConnectionVertexMap.addSynapticConnection(categoryString, connectedSynapseNode, synapseNode);
                                }

                            } else {
                                log.info(String.format("No %s relationship found for %s: %s", SYNAPSE_SET, SYNAPSE, connectedSynapseNode.getAllProperties()));
                            }

                        } else {
                            log.info(String.format("Connected %s is not associated with any %s: %s", SYNAPSE, SEGMENT, connectedSynapseNode.getAllProperties()));
                        }
                    }
                }
            } else {
                log.info("No %s found for neuron %d.", SYNAPSE_SET, neuronBodyId);
            }
        }

        return synapticConnectionVertexMap;
    }

}

class DistanceHelper {
    private double sum = 0;
    private Location first = null;
    private Location last = null;

    void add(Location location) {
        if (this.first == null) {
            this.first = location;
        }
        if (this.last != null) {
            this.sum += Location.getDistanceBetweenLocations(location, this.last);
        }
        this.last = location;
    }

    void combine(DistanceHelper otherHelper) {
        this.sum += otherHelper.sum;
        if (this.last != null && otherHelper.first != null) {
            this.sum += Location.getDistanceBetweenLocations(this.last, otherHelper.first);
        }
        this.last = otherHelper.last;
    }

    double getSum() {
        return sum;
    }

}

class NeuronWithRoiInfoMap {
    private Node neuron;
    private Map<String, SynapseCounter> roiInfoMap;
    private Long bodyId;
    private long pre;
    private long post;

    NeuronWithRoiInfoMap(Node neuron, long bodyId, Map<String, SynapseCounter> roiInfoMap, long pre, long post) {
        this.neuron = neuron;
        this.roiInfoMap = roiInfoMap;
        this.bodyId = bodyId;
        this.pre = pre;
        this.post = post;
    }

    public Node getNeuron() {
        return neuron;
    }

    public Map<String, SynapseCounter> getRoiInfoMap() {
        return roiInfoMap;
    }

    public Long getBodyId() {
        return bodyId;
    }

    public long getPost() {
        return post;
    }

    public long getPre() {
        return pre;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof NeuronWithRoiInfoMap) {
            final NeuronWithRoiInfoMap that = (NeuronWithRoiInfoMap) o;
            isEqual = this.bodyId.equals(that.bodyId);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + bodyId.hashCode();
        return result;
    }
}
