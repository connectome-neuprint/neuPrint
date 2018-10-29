package org.janelia.flyem.neuprintprocedures.functions;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
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

public class NeuPrintUserFunctions {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @UserFunction("neuprint.locationAs3dCartPoint")
    @Description("neuprint.locationAs3dCartPoint(x,y,z) : returns a 3D Cartesian org.neo4j.graphdb.spatial.Point type with the provided coordinates. ")
    public Point locationAs3dCartPoint(@Name("x") Double x, @Name("y") Double y, @Name("z") Double z) {
        if (x == null || y == null || z == null) {
            throw new Error("Must provide x, y, and z coordinate.");
        }
        Point point = null;
        try {
            Map<String, Object> pointQueryResult = dbService.execute("RETURN point({ x:" + x + ", y:" + y + ", z:" + z + ", crs:'cartesian-3D'}) AS point").next();
            point = (Point) pointQueryResult.get("point");
        } catch (Exception e) {
            log.error("Error using neuprint.locationAs3dCartPoint(x,y,z): ");
            e.printStackTrace();
        }
        return point;
    }

    @UserFunction("neuprint.roiInfoAsName")
    @Description("neuprint.roiInfoAsName(roiInfo, totalPre, totalPost, threshold, includedRois) ")
    public String roiInfoAsName(@Name("roiInfo") String roiInfo, @Name("totalPre") Long totalPre, @Name("totalPost") Long totalPost, @Name("threshold") Double threshold, @Name("includedRois") List<String> includedRois) {
        if (roiInfo == null || totalPre == null || totalPost == null || threshold == null) {
            throw new Error("Must provide roiInfo, totalPre, totalPost, and threshold.");
        }

        Gson gson = new Gson();
        Map<String, SynapseCounter> roiInfoMap = gson.fromJson(roiInfo, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        StringBuilder inputs = new StringBuilder();
        StringBuilder outputs = new StringBuilder();
        for (String roi : roiInfoMap.keySet()) {
            if (includedRois.contains(roi)) {
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
            throw new Error("Must provide bodyId, dataset, and number of connections.");
        }

        Node neuron = dbService.findNode(Label.label(dataset + "-Segment"), "bodyId", bodyId);

        // get synapse set
        Node synapseSet = null;
        for (Relationship containsRelationship : neuron.getRelationships(RelationshipType.withName("Contains"), Direction.OUTGOING)) {
            Node containedNode = containsRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label("SynapseSet"))) {
                synapseSet = containedNode;
            }
        }

        Map<String, SynapseCounter> categoryCounts = new TreeMap<>();

        if (synapseSet != null) {
            for (Relationship containsRelationship : synapseSet.getRelationships(RelationshipType.withName("Contains"), Direction.OUTGOING)) {
                Node synapseNode = containsRelationship.getEndNode();
                Set<String> connectedNeuronClusterNames = new TreeSet<>();
                for (Relationship synapsesToRelationship : synapseNode.getRelationships(RelationshipType.withName("SynapsesTo"))) {
                    Node otherSynapse = synapsesToRelationship.getOtherNode(synapseNode);
                    for (Relationship otherSynapseContainsRel : otherSynapse.getRelationships(RelationshipType.withName("Contains"), Direction.INCOMING)) {
                        Node containingNode = otherSynapseContainsRel.getStartNode();
                        if (containingNode.hasLabel(Label.label("SynapseSet"))) {
                            for (Relationship otherSynapseSetContainsRel : containingNode.getRelationships(RelationshipType.withName("Contains"), Direction.INCOMING)) {
                                Node otherSegment = otherSynapseSetContainsRel.getStartNode();
                                if (otherSegment.hasLabel(Label.label("Neuron"))) {
                                    connectedNeuronClusterNames.add((String) otherSegment.getProperty("clusterName"));
                                }
                            }
                        }
                    }
                }

                String categoryKey = "";
                for (String clusterName : connectedNeuronClusterNames) {
                    if (categoryKey.length() > 0) {
                        categoryKey = categoryKey + ":" + clusterName;
                    } else {
                        categoryKey = clusterName;
                    }
                }

                SynapseCounter synapseCounter = categoryCounts.getOrDefault(categoryKey, new SynapseCounter());
                if (synapseNode.hasLabel(Label.label("PreSyn"))) {
                    synapseCounter.incrementPre();
                } else if (synapseNode.hasLabel(Label.label("PostSyn"))) {
                    synapseCounter.incrementPost();
                }
                categoryCounts.put(categoryKey, synapseCounter);

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
