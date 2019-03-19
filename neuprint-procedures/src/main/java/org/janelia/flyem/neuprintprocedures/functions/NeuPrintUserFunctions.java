package org.janelia.flyem.neuprintprocedures.functions;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.model.SynapseCounter;
import org.janelia.flyem.neuprintloadprocedures.Location;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONTAINS;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.NEURON;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.POST_SYN;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.PRE_SYN;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.ROI_INFO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.SYNAPSES_TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.SYNAPSE_SET;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getLocationAs3dCartesianPoint;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSegment;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapseLocationSet;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapseSetForNeuron;
import static org.janelia.flyem.neuprintprocedures.proofreading.ProofreaderProcedures.getRoiInfoAsMap;

public class NeuPrintUserFunctions {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @UserFunction("neuprint.locationAs3dCartPoint")
    @Description("neuprint.locationAs3dCartPoint(x,y,z) : returns a 3D Cartesian org.neo4j.graphdb.spatial.Point type with the provided coordinates. ")
    public Point locationAs3dCartPoint(@Name("x") Double x, @Name("y") Double y, @Name("z") Double z) {
        if (x == null || y == null || z == null) {
            throw new RuntimeException("Must provide x, y, and z coordinate.");
        }
        Point point = null;
        try {
            point = getLocationAs3dCartesianPoint(dbService, x, y, z);
        } catch (Exception e) {
            log.error("Error using neuprint.locationAs3dCartPoint(x,y,z): ");
            e.printStackTrace();
        }
        return point;
    }

    @UserFunction("neuprint.getNeuronCentroid")
    @Description("neuprint.getNeuronCentroid(bodyId, dataset) : returns location of synapse closest to centroid of queried neuron as a list of longs. Returns [0,0,0] if there are no synapses on the body.")
    public List<Long> getNeuronCentroid(@Name("bodyId") Long bodyId, @Name("dataset") String dataset) {
        if (bodyId == null || dataset == null) {
            throw new RuntimeException("Must provide bodyId and dataset.");
        }

        final Location centralSynapseLocation;
        final Node neuron = getSegment(dbService, bodyId, dataset);
        if (neuron != null) {
            // get all synapse locations
            final Node synapseSet = getSynapseSetForNeuron(neuron);
            if (synapseSet != null) {

                final Set<Location> synapseLocationSet = getSynapseLocationSet(synapseSet);

                //compute centroid
                Location centroid = Location.getCentroid(new ArrayList<>(synapseLocationSet));

                // find synapse location closest to centroid
                final TreeSet<Location> sortedLocationSet = new TreeSet<>(Comparator.comparingDouble(a -> Location.getDistanceBetweenLocations(centroid, a)));
                sortedLocationSet.addAll(synapseLocationSet);
                centralSynapseLocation = sortedLocationSet.first();
            } else {
                centralSynapseLocation = new Location(0L, 0L, 0L);
            }
        } else {
            throw new RuntimeException("Body id " + bodyId + " does not exist in dataset " + dataset + ".");
        }

        return centralSynapseLocation.getLocationAsList();
    }

    @UserFunction("neuprint.roiInfoAsName")
    @Description("neuprint.roiInfoAsName(roiInfo, totalPre, totalPost, threshold, includedRois) ")
    public String roiInfoAsName(@Name("roiInfo") String roiInfo, @Name("totalPre") Long totalPre, @Name("totalPost") Long totalPost, @Name("threshold") Double threshold, @Name("includedRois") List<String> includedRois) {
        if (roiInfo == null || totalPre == null || totalPost == null || threshold == null || includedRois == null) {
            throw new RuntimeException("Must provide roiInfo, totalPre, totalPost, threshold, and includedRois.");
        }

        Map<String, SynapseCounter> roiInfoMap = getRoiInfoAsMap(roiInfo);

        return Neo4jImporter.generateClusterName(roiInfoMap, totalPre, totalPost, threshold, new HashSet<>(includedRois));

    }

    @UserFunction("neuprint.roiInfoAsNameUsingSubRois")
    @Description("neuprint.roiInfoAsNameUsingSubRois(roiInfo, totalPre, totalPost, threshold, superRois, allRois) ")
    public String roiInfoAsNameUsingSubRois(@Name("roiInfo") String roiInfo, @Name("totalPre") Long totalPre, @Name("totalPost") Long totalPost, @Name("threshold") Double threshold, @Name("superRois") List<String> superRois, @Name("superRois") List<String> allRois) {
        if (roiInfo == null || totalPre == null || totalPost == null || threshold == null || superRois == null || allRois == null) {
            throw new RuntimeException("Must provide roiInfo, totalPre, totalPost, threshold, superRois, and subRois.");
        }

        Map<String, SynapseCounter> roiInfoMap = getRoiInfoAsMap(roiInfo);

        StringBuilder inputs = new StringBuilder();
        StringBuilder outputs = new StringBuilder();
        for (String roi : roiInfoMap.keySet()) {
            if (allRois.contains(roi) && !superRois.contains(roi)) {
                if ((roiInfoMap.get(roi).getPre() * 1.0) / totalPre > threshold) {
                    outputs.append(roi).append(".");
                }
                if ((roiInfoMap.get(roi).getPost() * 1.0) / totalPost > threshold) {
                    inputs.append(roi).append(".");
                }
            }
        }
        if (outputs.length() > 0) {
            outputs.deleteCharAt(outputs.length() - 1);
        } else {
            outputs.append("none");
        }
        if (inputs.length() > 0) {
            inputs.deleteCharAt(inputs.length() - 1);
        } else {
            inputs.append("none");
        }

        return inputs + "-" + outputs;

    }

    @UserFunction("neuprint.getCategoriesOfConnections")
    @Description("neuprint.getCategoriesOfConnections")
    public String getCategoriesOfConnections(@Name("bodyId") Long bodyId, @Name("dataset") String dataset) {
        if (bodyId == null || dataset == null) {
            throw new RuntimeException("Must provide body id and dataset");
        }

        Node neuron = getSegment(dbService, bodyId, dataset);
        if (neuron == null) {
            log.error("Body id does not exist in the dataset");
            throw new RuntimeException("Body id does not exist in the dataset.");
        }

        // get synapse set
        Node synapseSet = getSynapseSetForNeuron(neuron);
        Gson gson = new Gson();

        Map<String, SynapseCounter> categoryCounts = new TreeMap<>();

        if (synapseSet != null) {
            // get cluster names of connected neurons
            for (Relationship containsRelationship : synapseSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                Node synapseNode = containsRelationship.getEndNode();
                for (Relationship synapsesToRelationship : synapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                    Node otherSynapse = synapsesToRelationship.getOtherNode(synapseNode);
                    for (Relationship otherSynapseContainsRel : otherSynapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
                        Node containingNode = otherSynapseContainsRel.getStartNode();
                        if (containingNode.hasLabel(Label.label(SYNAPSE_SET))) {
                            for (Relationship otherSynapseSetContainsRel : containingNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
                                Node otherSegment = otherSynapseSetContainsRel.getStartNode();
                                if (otherSegment.hasLabel(Label.label(NEURON))) {

                                    // get roi info from other neuron
                                    String roiInfo = (String) otherSegment.getProperty(ROI_INFO);
                                    if (roiInfo == null) {
                                        roiInfo = "{}";
                                    }

                                    Map<String, SynapseCounter> roiInfoObject = gson.fromJson(roiInfo, new TypeToken<Map<String, SynapseCounter>>() {
                                    }.getType());

                                    if (synapseNode.hasLabel(Label.label(PRE_SYN))) {
                                        // if a synapse is pre, get top output ROI for connected neuron
                                        SortedSet<Map.Entry<String, SynapseCounter>> sortedRois = Neo4jImporter.sortRoisByPreCount(roiInfoObject);
                                        String topOutputRoi;
                                        if (!sortedRois.isEmpty()) {
                                            topOutputRoi = sortedRois.first().getKey();
                                        } else {
                                            topOutputRoi = "None";
                                        }
                                        if (!categoryCounts.containsKey(topOutputRoi)) {
                                            categoryCounts.put(topOutputRoi, new SynapseCounter());
                                        }
                                        categoryCounts.get(topOutputRoi).incrementPost();

                                    } else if (synapseNode.hasLabel(Label.label(POST_SYN))) {
                                        // if a synapse is post get top input ROI for connected neuron
                                        SortedSet<Map.Entry<String, SynapseCounter>> sortedRois = Neo4jImporter.sortRoisByPostCount(roiInfoObject);
                                        String topInputRoi;
                                        if (!sortedRois.isEmpty()) {
                                            topInputRoi = sortedRois.first().getKey();
                                        } else {
                                            topInputRoi = "None";
                                        }
                                        if (!categoryCounts.containsKey(topInputRoi)) {
                                            categoryCounts.put(topInputRoi, new SynapseCounter());
                                        }
                                        categoryCounts.get(topInputRoi).incrementPre();
                                    }

                                }
                            }
                        }
                    }
                }

            }
        }

//        List<Connection> connectionList = new ArrayList<>();
//
//        for (Relationship connectsToRel : neuron.getRelationships(RelationshipType.withName("ConnectsTo"))) {
//            Node connectedNeuron = connectsToRel.getOtherNode(neuron);
//            if (connectedNeuron.hasLabel(Label.label("Neuron"))) {
//                String clusterName = (String) connectedNeuron.getProperty("clusterName");
//                long connectedBodyId = (long) connectedNeuron.getProperty("bodyId");
//                long postWeight = (long) connectsToRel.getProperty("weight");
//                long preWeight = (long) connectsToRel.getProperty("pre");
//                String direction = connectsToRel.getStartNode().equals(neuron) ? "output" : "input";
//                Connection connection = new Connection(clusterName, connectedBodyId, postWeight, preWeight, direction);
//                connectionList.add(connection);
//            }
//        }

//        connectionList.sort(new SortConnectionsByWeight());

        return gson.toJson(categoryCounts);

    }

//    class Connection {
//
//        String clusterName;
//        long bodyId;
//        long postWeight;
//        long preWeight;
//        String direction;
//
//        Connection(String clusterName, long bodyId, long postWeight, long preWeight, String direction) {
//            this.clusterName = clusterName;
//            this.bodyId = bodyId;
//            this.direction = direction;
//            this.postWeight = postWeight;
//            this.preWeight = preWeight;
//        }
//
//    }
//
////    class SortConnectionsByWeight implements Comparator<Connection> {
////
////        public int compare(Connection a, Connection b) {
////            return (int) (b.weight - a.weight);
////        }
////
////    }
}
