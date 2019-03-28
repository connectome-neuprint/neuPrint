package org.janelia.flyem.neuprintloadprocedures.procedures;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools;
import org.janelia.flyem.neuprintloadprocedures.model.RoiInfoWithHighPrecisionCounts;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounterWithHighPrecisionCounts;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONFIDENCE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONNECTS_TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.FROM;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.POST;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.PRE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.ROI_INFO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.TYPE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.WEIGHT;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.WEIGHT_HP;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapseRois;

public class LoadingProcedures {
    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    public static final Type ROI_INFO_WITH_HP_TYPE = new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
    }.getType();

    @Procedure(value = "loader.setConnectionSetRoiInfoAndWeightHP", mode = Mode.WRITE)
    @Description("loader.setConnectionSetRoiInfoAndWeightHP : Add roiInfo property to ConnectionSet node.")
    public void setConnectionSetRoiInfoAndWeightHP(@Name("preBodyId") final Long preBodyId,
                                                   @Name("postBodyId") final Long postBodyId,
                                                   @Name("datasetLabel") final String datasetLabel,
                                                   @Name("preHPThreshold") final Double preHPThreshold,
                                                   @Name("postHPThreshold") final Double postHPThreshold) {

        log.info("loader.setConnectionSetRoiInfoAndWeightHP: entry");

        try {

            if (preBodyId == null || postBodyId == null || datasetLabel == null || preHPThreshold == null || postHPThreshold == null) {
                log.error("loader.setConnectionSetRoiInfoAndWeightHP: Missing input arguments.");
                throw new RuntimeException("loader.setConnectionSetRoiInfoAndWeightHP: Missing input arguments.");
            }

            Node connectionSet = GraphTraversalTools.getConnectionSetNode(dbService, preBodyId, postBodyId, datasetLabel);

            if (connectionSet == null) {
                log.error(String.format("loader.setConnectionSetRoiInfoAndWeightHP: ConnectionSet does not exist: %d to %d in dataset %s.", preBodyId, postBodyId, datasetLabel));
                throw new RuntimeException(String.format("loader.setConnectionSetRoiInfoAndWeightHP: ConnectionSet does not exist: %d to %d in dataset %s.", preBodyId, postBodyId, datasetLabel));
            }

            // get all synapses on connection set
            Set<Node> synapsesForConnectionSet = GraphTraversalTools.getSynapsesForConnectionSet(connectionSet);

            int[] results = setConnectionSetRoiInfoAndGetWeightAndWeightHP(synapsesForConnectionSet, connectionSet, preHPThreshold, postHPThreshold);
            int weight = results[0];
            int weightHP = results[1];

            // add postHP to ConnectsTo
            addWeightAndWeightHPToConnectsTo(connectionSet, weight, weightHP);

        } catch (Exception e) {
            log.info(String.format("loader.setConnectionSetRoiInfoAndWeightHP: Error adding roiInfo: %s, pre body ID: %d, post body ID: %d", e, preBodyId, postBodyId));
            throw new RuntimeException(String.format("loader.setConnectionSetRoiInfoAndWeightHP: Error adding roiInfo: %s, pre body ID: %d, post body ID: %d", e, preBodyId, postBodyId));
        }

        log.info("loader.setConnectionSetRoiInfoAndWeightHP: exit");

    }

    public static void addWeightAndWeightHPToConnectsTo(Node connectionSet, int weight, int weightHP) {
        // will delete ConnectsTo if weight == 0
        Node preSynapticNode = connectionSet.getSingleRelationship(RelationshipType.withName(FROM), Direction.OUTGOING).getEndNode();
        long postSynapticNodeId = connectionSet.getSingleRelationship(RelationshipType.withName(TO), Direction.OUTGOING).getEndNodeId();

        Iterable<Relationship> connectsToRelationships = preSynapticNode.getRelationships(RelationshipType.withName(CONNECTS_TO), Direction.OUTGOING);

        for (Relationship connectsToRel : connectsToRelationships) {
            long endNodeIdForRel = connectsToRel.getEndNodeId();
            if (postSynapticNodeId == endNodeIdForRel) {
                if (weight > 0) {
                    connectsToRel.setProperty(WEIGHT, weight);
                    connectsToRel.setProperty(WEIGHT_HP, weightHP);
                } else {
                    connectsToRel.delete();
                }
            }
        }
    }

    public static int[] setConnectionSetRoiInfoAndGetWeightAndWeightHP(Set<Node> synapsesForConnectionSet, Node connectionSet, Double preHPThreshold, Double postHPThreshold) {

        Object[] roiInfoPostHPAndPost = getRoiInfoForConnectionSet(synapsesForConnectionSet, preHPThreshold, postHPThreshold);
        RoiInfoWithHighPrecisionCounts roiInfo = (RoiInfoWithHighPrecisionCounts) roiInfoPostHPAndPost[0];
        int postHP = (int) roiInfoPostHPAndPost[1];
        int post = (int) roiInfoPostHPAndPost[2];

        // add to connection set node
        connectionSet.setProperty(ROI_INFO, roiInfo.getAsJsonString());

        return new int[]{post, postHP};

    }

    private static Object[] getRoiInfoForConnectionSet(Set<Node> synapsesForConnectionSet, Double preHPThreshold, Double postHPThreshold) {

        // for each pre/post add to count and check confidence to add to hp count
        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts();

        // total postHP count for weightHP on ConnectsTo
        int postHP = 0;
        // total post count for weight on ConnectsTo
        int post = 0;

        for (Node synapse : synapsesForConnectionSet) {
            String type;
            Double confidence;
            if (synapse.hasProperty(TYPE)) {
                type = (String) synapse.getProperty(TYPE);
            } else {
                break;
            }
            if (synapse.hasProperty(CONFIDENCE)) {
                confidence = (Double) synapse.getProperty(CONFIDENCE);
            } else {
                confidence = null;
            }
            Set<String> synapseRois = getSynapseRois(synapse);
            if (type.equals(PRE) && confidence != null && confidence > preHPThreshold) {
                for (String roi : synapseRois) {
                    roiInfo.incrementPreForRoi(roi);
                    roiInfo.incrementPreHPForRoi(roi);
                }
            } else if (type.equals(PRE)) {
                for (String roi : synapseRois) {
                    roiInfo.incrementPreForRoi(roi);
                }
            } else if (type.equals(POST) && confidence != null && confidence > postHPThreshold) {
                postHP++;
                post++;
                for (String roi : synapseRois) {
                    roiInfo.incrementPostForRoi(roi);
                    roiInfo.incrementPostHPForRoi(roi);
                }
            } else if (type.equals(POST)) {
                post++;
                for (String roi : synapseRois) {
                    roiInfo.incrementPostForRoi(roi);
                }
            }
        }

        return new Object[]{roiInfo, postHP, post};
    }

    public static String addSynapseToRoiInfoWithHP(String roiInfoString, String roi, String synapseType, Double synapseConfidence, Double preHPThreshold, Double postHPThreshold) {
        Gson gson = new Gson();
        Map<String, SynapseCounterWithHighPrecisionCounts> roiInfoMap = gson.fromJson(roiInfoString, ROI_INFO_WITH_HP_TYPE);
        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts(roiInfoMap);

        if (synapseType.equals(PRE) && synapseConfidence != null && synapseConfidence > preHPThreshold) {
            roiInfo.incrementPreForRoi(roi);
            roiInfo.incrementPreHPForRoi(roi);
        } else if (synapseType.equals(PRE)) {
            roiInfo.incrementPreForRoi(roi);
        } else if (synapseType.equals(POST) && synapseConfidence != null && synapseConfidence > postHPThreshold) {
            roiInfo.incrementPostForRoi(roi);
            roiInfo.incrementPostHPForRoi(roi);
        } else if (synapseType.equals(POST)) {
            roiInfo.incrementPostForRoi(roi);
        }

        return roiInfo.getAsJsonString();

    }

    public static String removeSynapseFromRoiInfoWithHP(String roiInfoString, String roi, String synapseType, Double synapseConfidence, Double preHPThreshold, Double postHPThreshold) {
        Gson gson = new Gson();
        Map<String, SynapseCounterWithHighPrecisionCounts> roiInfoMap = gson.fromJson(roiInfoString, ROI_INFO_WITH_HP_TYPE);
        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts(roiInfoMap);

        if (synapseType.equals(PRE) && synapseConfidence != null && synapseConfidence > preHPThreshold) {
            roiInfo.decrementPreForRoi(roi);
            roiInfo.decrementPreHPForRoi(roi);
        } else if (synapseType.equals(PRE)) {
            roiInfo.decrementPreForRoi(roi);
        } else if (synapseType.equals(POST) && synapseConfidence != null && synapseConfidence > postHPThreshold) {
            roiInfo.decrementPostForRoi(roi);
            roiInfo.decrementPostHPForRoi(roi);
        } else if (synapseType.equals(POST)) {
            roiInfo.decrementPostForRoi(roi);
        }

        return roiInfo.getAsJsonString();

    }

}
