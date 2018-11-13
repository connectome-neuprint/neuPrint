package org.janelia.flyem.neuprintprocedures.functions;

import com.google.gson.Gson;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.janelia.flyem.neuprintprocedures.Location;
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.CLUSTER_NAME;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.CONTAINS;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.NEURON;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.POST_SYN;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.PRE_SYN;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.SYNAPSES_TO;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.SYNAPSE_SET;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.getLocationAs3dCartesianPoint;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.getRoiInfoAsMap;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.getSegment;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.getSynapseLocations;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.getSynapseSetForNeuron;

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
    @Description("neuprint.getNeuronCentroid(bodyId, dataset) : returns centroid of queried neuron as a list of longs. Returns [0,0,0] if there are no synapses on the body.")
    public List<Long> getNeuronCentroid(@Name("bodyId") Long bodyId, @Name("dataset") String dataset) {
        if (bodyId == null || dataset == null) {
            throw new RuntimeException("Must provide bodyId and dataset.");
        }

        Location centroid;
        final Node neuron = getSegment(dbService, bodyId, dataset);
        if (neuron != null) {
            // get all synapse locations
            final Node synapseSet = getSynapseSetForNeuron(neuron);
            if (synapseSet != null) {
                final List<Location> synapseLocations = getSynapseLocations(synapseSet);
                //compute centroid
                centroid = Location.getCentroid(synapseLocations);
            } else {
                centroid = new Location(0L, 0L, 0L);
            }
        } else {
            throw new RuntimeException("Body id " + bodyId + " does not exist in dataset " + dataset + ".");
        }

        return centroid.getLocationAsList();
    }

    @UserFunction("neuprint.roiInfoAsName")
    @Description("neuprint.roiInfoAsName(roiInfo, totalPre, totalPost, threshold, includedRois) ")
    public String roiInfoAsName(@Name("roiInfo") String roiInfo, @Name("totalPre") Long totalPre, @Name("totalPost") Long totalPost, @Name("threshold") Double threshold, @Name("includedRois") List<String> includedRois) {
        if (roiInfo == null || totalPre == null || totalPost == null || threshold == null || includedRois == null) {
            throw new RuntimeException("Must provide roiInfo, totalPre, totalPost, threshold, and includedRois.");
        }

        Map<String, SynapseCounter> roiInfoMap = getRoiInfoAsMap(roiInfo);

        return Neo4jImporter.generateClusterName(roiInfoMap, totalPre, totalPost, threshold, includedRois);

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

    @UserFunction("neuprint.getClusterNamesOfConnections")
    @Description("neuprint.getClusterNamesOfConnections")
    public String getClusterNamesOfConnections(@Name("bodyId") Long bodyId, @Name("dataset") String dataset) {
        if (bodyId == null || dataset == null) {
            throw new RuntimeException("Must provide bodyId, dataset, and number of connections.");
        }

        Node neuron = getSegment(dbService, bodyId, dataset);

        // get synapse set
        Node synapseSet = getSynapseSetForNeuron(neuron);

        Map<String, SynapseCounter> categoryCounts = new TreeMap<>();

        if (synapseSet != null) {
            // get cluster names of connected neurons
            for (Relationship containsRelationship : synapseSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                Node synapseNode = containsRelationship.getEndNode();
                Set<String> connectedNeuronClusterNames = new TreeSet<>();
                for (Relationship synapsesToRelationship : synapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                    Node otherSynapse = synapsesToRelationship.getOtherNode(synapseNode);
                    for (Relationship otherSynapseContainsRel : otherSynapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
                        Node containingNode = otherSynapseContainsRel.getStartNode();
                        if (containingNode.hasLabel(Label.label(SYNAPSE_SET))) {
                            for (Relationship otherSynapseSetContainsRel : containingNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
                                Node otherSegment = otherSynapseSetContainsRel.getStartNode();
                                if (otherSegment.hasLabel(Label.label(NEURON))) {
                                    connectedNeuronClusterNames.add((String) otherSegment.getProperty(CLUSTER_NAME));
                                } else {
                                    connectedNeuronClusterNames.add("_");
                                }
                            }
                        }
                    }
                }

                StringBuilder categoryKey = new StringBuilder();
                for (String clusterName : connectedNeuronClusterNames) {
                    if (categoryKey.length() > 0) {
                        categoryKey.append(":").append(clusterName);
                    } else {
                        categoryKey = new StringBuilder(clusterName);
                    }
                }

                SynapseCounter synapseCounter = categoryCounts.getOrDefault(categoryKey.toString(), new SynapseCounter());
                if (synapseNode.hasLabel(Label.label(PRE_SYN))) {
                    synapseCounter.incrementPre();
                } else if (synapseNode.hasLabel(Label.label(POST_SYN))) {
                    synapseCounter.incrementPost();
                }
                categoryCounts.put(categoryKey.toString(), synapseCounter);

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

        Gson gson = new Gson();

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
