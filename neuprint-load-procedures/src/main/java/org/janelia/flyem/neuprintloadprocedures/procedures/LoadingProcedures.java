package org.janelia.flyem.neuprintloadprocedures.procedures;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools;
import org.janelia.flyem.neuprintloadprocedures.model.RoiInfo;
import org.janelia.flyem.neuprintloadprocedures.model.RoiInfoWithHighPrecisionCounts;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.BODY_ID;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONFIDENCE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONNECTION_SET;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONNECTS_TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONTAINS;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.DATASET_BODY_IDs;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.FROM;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.LINKS_TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.NEURON;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.POST;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.POST_HP_THRESHOLD;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.PRE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.PRE_HP_THRESHOLD;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.ROI_INFO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.SUPER_LEVEL_ROIS;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.SYNAPSES_TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.TIME_STAMP;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.TYPE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.WEIGHT;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.WEIGHT_HP;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getConnectionSetNode;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getMetaNode;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSegmentThatContainsSynapse;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapseNodesFromSynapseSet;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapseRois;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapsesForConnectionSet;
import static org.janelia.flyem.neuprintloadprocedures.model.RoiInfo.getRoiInfoFromString;
import static org.janelia.flyem.neuprintloadprocedures.model.RoiInfoWithHighPrecisionCounts.getRoiInfoHPFromString;

public class LoadingProcedures {
    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

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
            acquireWriteLockForNode(connectionSet);

            // get all synapses on connection set
            Set<Node> synapsesForConnectionSet = GraphTraversalTools.getSynapsesForConnectionSet(connectionSet);

            for (Node synapse : synapsesForConnectionSet) {
                acquireWriteLockForNode(synapse);
            }

            Node metaNode = getMetaNode(dbService, datasetLabel);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + datasetLabel);
                throw new RuntimeException("Meta node not found for dataset: " + datasetLabel);
            }
            acquireWriteLockForNode(metaNode);
            Set<String> metaNodeRoiSet = getMetaNodeRoiSet(metaNode);

            int[] results = setConnectionSetRoiInfoAndGetWeightAndWeightHP(synapsesForConnectionSet, connectionSet, preHPThreshold, postHPThreshold, metaNodeRoiSet);
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

    @Procedure(value = "loader.addPropsAndConnectionInfoToSegment", mode = Mode.WRITE)
    @Description("loader.addPropsAndConnectionInfoToSegment(segmentNode, synapseSetNode, dataset, preHPThreshold, postHPThreshold, neuronThreshold, addCSRoiInfoAndWeightHP)")
    public void addPropsAndConnectionInfoToSegment(@Name("segment") final Node segment,
                                                   @Name("synapseSet") final Node synapseSet,
                                                   @Name("dataset") final String dataset,
                                                   @Name("preHPThreshold") final Double preHPThreshold,
                                                   @Name("postHPThreshold") final Double postHPThreshold,
                                                   @Name("neuronThreshold") final Long neuronThreshold,
                                                   @Name("addCSRoiInfoAndWeightHP") final boolean addCSRoiInfoAndWeightHP) {

        log.info("proofreader.addPropsAndConnectionInfoToSegment: entry");

        try {
            if (segment == null || synapseSet == null || dataset == null || preHPThreshold == null || postHPThreshold == null) {
                log.error("loader.addPropsAndConnectionInfoToSegment: Missing input arguments.");
                throw new RuntimeException("loader.addPropsAndConnectionInfoToSegment: Missing input arguments.");
            }

            acquireWriteLockForSegmentSubgraph(segment);

            Long bodyId = (Long) segment.getProperty("bodyId");
            if (bodyId == null) {
                log.error("Segment node is missing a bodyId. Neo4j ID is: " + segment.getId());
                throw new RuntimeException("Segment node is missing a bodyId. Neo4j ID is: " + segment.getId());
            }

            Set<Node> synapseNodes = getSynapseNodesFromSynapseSet(synapseSet);
            Long preCount = 0L;
            Long postCount = 0L;
            RoiInfo roiInfo = new RoiInfo();

            final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

            // acquire meta node for updating
            Node metaNode = getMetaNode(dbService, dataset);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + dataset);
                throw new RuntimeException("Meta node not found for dataset: " + dataset);
            }
            acquireWriteLockForNode(metaNode);
            Set<String> metaNodeRoiSet = getMetaNodeRoiSet(metaNode);

            for (Node synapse : synapseNodes) {

                acquireWriteLockForNode(synapse);

                String synapseType;
                if (synapse.hasProperty(TYPE)) {
                    synapseType = (String) synapse.getProperty(TYPE);
                } else {
                    log.error(String.format("Synapse does not have type property: %s", synapse.getAllProperties()));
                    throw new RuntimeException(String.format("Synapse does not have type property: %s", synapse.getAllProperties()));
                }
                if (!synapseType.equals(PRE) && !synapseType.equals(POST)) {
                    log.error(String.format("Synapse does not have type property equal to pre or post: %s", synapse.getAllProperties()));
                    throw new RuntimeException(String.format("Synapse does not have type property equal to pre or post: %s", synapse.getAllProperties()));
                }

                // for each synapse that the synapse SynapsesTo, create or add to a ConnectionSet and ConnectsTo
                for (Relationship synapsesToRel : synapse.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                    Node otherSynapse = synapsesToRel.getOtherNode(synapse);
                    Node otherSegment = getSegmentThatContainsSynapse(otherSynapse);
                    Node connectionSet;
                    if (otherSegment == null) {
                        log.warn("Synapse does not belong to segment: " + otherSynapse.getAllProperties());
                    } else {
                        Long otherBodyId;
                        if (otherSegment.hasProperty(BODY_ID)) {
                            otherBodyId = (Long) otherSegment.getProperty(BODY_ID);
                        } else {
                            log.error("Segment node is missing a bodyId. Neo4j ID is: " + otherSegment.getId());
                            throw new RuntimeException("Segment node is missing a bodyId. Neo4j ID is: " + otherSegment.getId());
                        }
                        if (synapseType.equals(PRE)) {
                            // look for connection set from original segment to other segment (create ConnectsTo and ConnectionSet if doesn't exist)
                            connectionSet = getConnectionSetOrCreateConnectionSetAndConnectsToRelFromSynapses(bodyId, otherBodyId, segment, otherSegment, synapse, otherSynapse, dataset, timeStamp);
                        } else {
                            // look for connection set from other segment to original segment (create ConnectsTo and ConnectionSet if doesn't exist)
                            connectionSet = getConnectionSetOrCreateConnectionSetAndConnectsToRelFromSynapses(otherBodyId, bodyId, otherSegment, segment, otherSynapse, synapse, dataset, timeStamp);
                        }

                        Set<Node> synapseForConnectionSet = getSynapsesForConnectionSet(connectionSet);
                        // recompute roiInfo on connection sets and set weight and weightHP
                        if (addCSRoiInfoAndWeightHP) {
                            setConnectionSetRoiInfoWeightAndWeightHP(synapseForConnectionSet, connectionSet, preHPThreshold, postHPThreshold, metaNodeRoiSet);
                        } else {
                            addWeightToConnectsTo(synapseForConnectionSet, connectionSet, metaNodeRoiSet);
                        }
                    }
                }

                // get synapse rois for adding to the body and roiInfo
                final Set<String> synapseRois = getSynapseRois(synapse, metaNodeRoiSet);

                if (synapseType.equals(PRE)) {
                    for (String roi : synapseRois) {
                        roiInfo.incrementPreForRoi(roi);
                    }
                    preCount++;
                } else {
                    for (String roi : synapseRois) {
                        roiInfo.incrementPostForRoi(roi);
                    }
                    postCount++;
                }
            }
            // update neuron pre/post, roiInfo, rois
            // recompute information on containing segment
            recomputeSegmentPropertiesFollowingSynapsesAddition(preCount, postCount, roiInfo, segment, metaNode, dataset, neuronThreshold);

        } catch (Exception e) {
            log.error("Error running loader.addPropsAndConnectionInfoToSegment: " + e);
            throw new RuntimeException("Error running loader.addPropsAndConnectionInfoToSegment: " + e);
        }

        log.info("loader.addPropsAndConnectionInfoToSegment: exit");

    }

    public static Set<String> getMetaNodeRoiSet(final Node metaNode) {
        Set<String> metaNodeRoiSet = new HashSet<>();
        if (metaNode.hasProperty(ROI_INFO)) {
            String metaRoiInfoString = (String) metaNode.getProperty(ROI_INFO);
            RoiInfoWithHighPrecisionCounts metaRoiInfo = RoiInfoWithHighPrecisionCounts.getRoiInfoHPFromString(metaRoiInfoString);
            metaNodeRoiSet = metaRoiInfo.getSetOfRois();
        }
        return metaNodeRoiSet;
    }

    public static void incrementSegmentPreCount(Node segment) {
        if (segment.hasProperty(PRE)) {
            Long currentTotalPreCount = (Long) segment.getProperty(PRE);
            segment.setProperty(PRE, ++currentTotalPreCount);
        } else {
            segment.setProperty(PRE, 1L);
        }
    }

    public static void incrementSegmentPostCount(Node segment) {
        if (segment.hasProperty(POST)) {
            Long currentTotalPostCount = (Long) segment.getProperty(POST);
            segment.setProperty(POST, ++currentTotalPostCount);
        } else {
            segment.setProperty(POST, 1L);
        }
    }

    public static String addSynapseToRoiInfo(String roiInfoString, String roiName, String synapseType) {
        RoiInfo roiInfo = getRoiInfoFromString(roiInfoString);

        if (synapseType.equals(PRE)) {
            roiInfo.incrementPreForRoi(roiName);
        } else if (synapseType.equals(POST)) {
            roiInfo.incrementPostForRoi(roiName);
        }

        return roiInfo.getAsJsonString();

    }

    public static Map<String, SynapseCounter> getRoiInfoAsMap(String roiInfoString) {
        Gson gson = new Gson();
        return gson.fromJson(roiInfoString, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
    }

    private void recomputeSegmentPropertiesFollowingSynapsesAddition(Long preCount, Long postCount, RoiInfo roiInfo, Node containingSegment, Node metaNode, String dataset, Long neuronThreshold) {
        // set pre and post count, rois, roiInfo
        if (preCount > 0 || postCount > 0) {
            containingSegment.setProperty(PRE, preCount);
            containingSegment.setProperty(POST, postCount);
            containingSegment.setProperty(ROI_INFO, roiInfo.getAsJsonString());
            for (String roi : roiInfo.getSetOfRois()) {
                containingSegment.setProperty(roi, true);
            }
        }

        if (shouldBeLabeledNeuron(containingSegment, neuronThreshold)) {
            convertSegmentToNeuron(containingSegment, dataset, metaNode);
        }

    }

    private static boolean shouldBeLabeledNeuron(Node neuronNode, Long neuronThreshold) {
        long preSynapseThreshold = (long) (neuronThreshold / 5.0F);
        long preCount = 0;
        long postCount = 0;
        if (neuronNode.hasProperty(PRE)) {
            preCount = (long) neuronNode.getProperty(PRE);
        }
        if (neuronNode.hasProperty(POST)) {
            postCount = (long) neuronNode.getProperty(POST);
        }

        // returns true if meets the definition for a neuron
        return (preCount >= preSynapseThreshold || postCount >= neuronThreshold);
    }

    public static void convertSegmentToNeuron(final Node segment, final String datasetLabel, final Node metaNode) {

        segment.addLabel(Label.label(NEURON));
        segment.addLabel(Label.label(datasetLabel + "-" + NEURON));

        //generate cluster name
        RoiInfo roiInfoObject = new RoiInfo();
        long totalPre = 0;
        long totalPost = 0;
        boolean setClusterName = true;
        try {
            roiInfoObject = getRoiInfoFromString((String) segment.getProperty(ROI_INFO));
        } catch (Exception e) {
            setClusterName = false;
        }

        try {
            totalPre = (long) segment.getProperty(PRE);
        } catch (Exception e) {
            setClusterName = false;
        }

        try {
            totalPost = (long) segment.getProperty(POST);
        } catch (Exception e) {
            setClusterName = false;
        }

        if (setClusterName) {
            if (metaNode != null) {
                String[] metaNodeSuperLevelRois;
                try {
                    metaNodeSuperLevelRois = (String[]) metaNode.getProperty(SUPER_LEVEL_ROIS);
                } catch (Exception e) {
                    throw new RuntimeException("Error retrieving " + SUPER_LEVEL_ROIS + " from Meta node for " + datasetLabel + ":" + e);
                }
                final Set<String> roiSet = new HashSet<>(Arrays.asList(metaNodeSuperLevelRois));
                segment.setProperty("clusterName", generateClusterName(roiInfoObject, totalPre, totalPost, 0.10, roiSet));
            } else {
                throw new RuntimeException("Meta node is null.");
            }
        }

    }

    public static String generateClusterName(RoiInfo roiInfo, long totalPre, long totalPost, double threshold, Set<String> includedRois) {

        StringBuilder inputs = new StringBuilder();
        StringBuilder outputs = new StringBuilder();
        for (String roi : roiInfo.getSetOfRois()) {
            if (includedRois.contains(roi)) {
                if ((roiInfo.get(roi).getPre() * 1.0) / totalPre > threshold) {
                    outputs.append(roi).append(".");
                }
                if ((roiInfo.get(roi).getPost() * 1.0) / totalPost > threshold) {
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

    public static void addWeightToConnectsTo(Set<Node> synapsesForConnectionSet, Node connectionSet, Set<String> metaNodeRoiSet) {
        Object[] roiInfoPostHPAndPost = getRoiInfoForConnectionSet(synapsesForConnectionSet, 0.0, 0.0, metaNodeRoiSet);
        int weight = (int) roiInfoPostHPAndPost[2];

        Node preSynapticNode = connectionSet.getSingleRelationship(RelationshipType.withName(FROM), Direction.OUTGOING).getEndNode();
        long postSynapticNodeId = connectionSet.getSingleRelationship(RelationshipType.withName(TO), Direction.OUTGOING).getEndNodeId();

        Iterable<Relationship> connectsToRelationships = preSynapticNode.getRelationships(RelationshipType.withName(CONNECTS_TO), Direction.OUTGOING);

        for (Relationship connectsToRel : connectsToRelationships) {
            long endNodeIdForRel = connectsToRel.getEndNodeId();
            if (postSynapticNodeId == endNodeIdForRel) {
                connectsToRel.setProperty(WEIGHT, weight);
            }
        }
    }

    public static int[] setConnectionSetRoiInfoAndGetWeightAndWeightHP(Set<Node> synapsesForConnectionSet, Node connectionSet, Double preHPThreshold, Double postHPThreshold, Set<String> metaNodeRoiSet) {

        Object[] roiInfoPostHPAndPost = getRoiInfoForConnectionSet(synapsesForConnectionSet, preHPThreshold, postHPThreshold, metaNodeRoiSet);
        RoiInfoWithHighPrecisionCounts roiInfo = (RoiInfoWithHighPrecisionCounts) roiInfoPostHPAndPost[0];
        int postHP = (int) roiInfoPostHPAndPost[1];
        int post = (int) roiInfoPostHPAndPost[2];

        // add to connection set node
        connectionSet.setProperty(ROI_INFO, roiInfo.getAsJsonString());

        return new int[]{post, postHP};

    }

    public static int[] setConnectionSetRoiInfoWeightAndWeightHP(Set<Node> synapsesForConnectionSet, Node connectionSetNode, double preHPThreshold, double postHPThreshold, Set<String> metaNodeRoiSet) {
        int[] results = setConnectionSetRoiInfoAndGetWeightAndWeightHP(synapsesForConnectionSet, connectionSetNode, preHPThreshold, postHPThreshold, metaNodeRoiSet);
        int weight = results[0];
        int weightHP = results[1];

        // add weight info to ConnectsTo (will delete ConnectsTo relationship if weight is == 0)
        addWeightAndWeightHPToConnectsTo(connectionSetNode, weight, weightHP);

        return results;
    }

    private static Object[] getRoiInfoForConnectionSet(Set<Node> synapsesForConnectionSet, Double preHPThreshold, Double postHPThreshold, Set<String> metaNodeRoiSet) {

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
                try {
                    confidence = (Double) synapse.getProperty(CONFIDENCE);
                } catch (ClassCastException cce) {
                    float floatConfidence = (Float) synapse.getProperty(CONFIDENCE);
                    confidence = (double) floatConfidence;
                    // fix the issue
                    synapse.setProperty(CONFIDENCE, confidence);
                }
            } else {
                confidence = null;
            }

            Set<String> synapseRois = getSynapseRois(synapse, metaNodeRoiSet);
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
        RoiInfoWithHighPrecisionCounts roiInfo = getRoiInfoHPFromString(roiInfoString);

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
        RoiInfoWithHighPrecisionCounts roiInfo = getRoiInfoHPFromString(roiInfoString);

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

    private Node getConnectionSetOrCreateConnectionSetAndConnectsToRelFromSynapses(Long preBodyId, Long postBodyId, Node preBody, Node postBody, Node preSynapse, Node postSynapse, String dataset, LocalDateTime timeStamp) {
        // look for connection set from original segment to other segment
        Node connectionSet = getConnectionSetNode(dbService, preBodyId, postBodyId, dataset);
        if (connectionSet == null) {
            // create connection set if it doesn't exist
            connectionSet = createConnectionSetNode(dataset, preBody, postBody, timeStamp);
            // create connects to between neurons
            addConnectsToRelationship(preBody, postBody, 1); // 1 since there is one known post for this connection
        }
        // add synapses to connection set
        boolean hasPreRel = false;
        boolean hasPostRel = false;
        long preId = preSynapse.getId();
        long postId = postSynapse.getId();

        for (Relationship containsRel : connectionSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            Node containedSynapse = containsRel.getEndNode();
            if (containedSynapse.getId() == preId) {
                hasPreRel = true;
            } else if (containedSynapse.getId() == postId) {
                hasPostRel = true;
            }
        }

        if (!hasPreRel) {
            connectionSet.createRelationshipTo(preSynapse, RelationshipType.withName(CONTAINS));
        }
        if (!hasPostRel) {
            connectionSet.createRelationshipTo(postSynapse, RelationshipType.withName(CONTAINS));
        }

        return connectionSet;
    }

    private Node createConnectionSetNode(String datasetLabel, Node startSegment, Node endSegment, LocalDateTime timeStamp) {
        // create a ConnectionSet node
        final Node connectionSet = dbService.createNode(Label.label(CONNECTION_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + CONNECTION_SET));
        long startNodeBodyId;
        long endNodeBodyId;
        try {
            startNodeBodyId = (long) startSegment.getProperty(BODY_ID);
        } catch (Exception e) {
            log.error("Error retrieving body ID from segment with Neo4j ID " + startSegment.getId() + ": " + e);
            throw new RuntimeException("Error retrieving body ID from segment with Neo4j ID " + startSegment.getId() + ": " + e);
        }
        try {
            endNodeBodyId = (long) endSegment.getProperty(BODY_ID);
        } catch (Exception e) {
            log.error("Error retrieving body ID from segment with Neo4j ID " + endSegment.getId() + ": " + e);
            throw new RuntimeException("Error retrieving body ID from segment with Neo4j ID " + endSegment.getId() + ": " + e);
        }
        connectionSet.setProperty(DATASET_BODY_IDs, datasetLabel + ":" + startNodeBodyId + ":" + endNodeBodyId);

        connectionSet.setProperty(TIME_STAMP, timeStamp);

        // connect it to start and end bodies
        connectionSet.createRelationshipTo(startSegment, RelationshipType.withName(FROM));
        connectionSet.createRelationshipTo(endSegment, RelationshipType.withName(TO));

        return connectionSet;
    }

    public static Relationship addConnectsToRelationship(Node startNode, Node endNode, long weight) {
        // create a ConnectsTo relationship
        Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(CONNECTS_TO));
        relationship.setProperty(WEIGHT, weight);
        return relationship;
    }

    public static void addSynapseToSynapseSet(final Node synapseSet, final Node synapse) {
        synapseSet.createRelationshipTo(synapse, RelationshipType.withName(CONTAINS));
    }

    public static Map<String, Double> getPreAndPostHPThresholdFromMetaNode(Node metaNode) {
        Map<String, Double> thresholdMap = new HashMap<>();
        if (metaNode != null && metaNode.hasProperty(PRE_HP_THRESHOLD)) {
            thresholdMap.put(PRE_HP_THRESHOLD, (Double) metaNode.getProperty(PRE_HP_THRESHOLD));
        } else {
            thresholdMap.put(PRE_HP_THRESHOLD, 0.0);
        }
        if (metaNode != null && metaNode.hasProperty(POST_HP_THRESHOLD)) {
            thresholdMap.put(POST_HP_THRESHOLD, (Double) metaNode.getProperty(POST_HP_THRESHOLD));
        } else {
            thresholdMap.put(POST_HP_THRESHOLD, 0.0);
        }

        return thresholdMap;
    }

    private void acquireWriteLockForSegmentSubgraph(Node segment) {
        // neuron
        if (segment != null) {
            acquireWriteLockForNode(segment);
            // connects to relationships and 1-degree connections
            for (Relationship connectsToRelationship : segment.getRelationships(RelationshipType.withName(CONNECTS_TO))) {
                acquireWriteLockForRelationship(connectsToRelationship);
                acquireWriteLockForNode(connectsToRelationship.getOtherNode(segment));
            }
            // skeleton and synapse set
            for (Relationship containsRelationship : segment.getRelationships(RelationshipType.withName(CONTAINS))) {
                acquireWriteLockForRelationship(containsRelationship);
                Node skeletonOrSynapseSetNode = containsRelationship.getEndNode();
                acquireWriteLockForNode(skeletonOrSynapseSetNode);
                // skel nodes and synapses
                for (Relationship skelNodeOrSynapseRelationship : skeletonOrSynapseSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                    acquireWriteLockForRelationship(skelNodeOrSynapseRelationship);
                    Node skelNodeOrSynapseNode = skelNodeOrSynapseRelationship.getEndNode();
                    acquireWriteLockForNode(skelNodeOrSynapseNode);
                    // first degree relationships to synapses
                    for (Relationship synapsesToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                        acquireWriteLockForRelationship(synapsesToRelationship);
                        acquireWriteLockForNode(synapsesToRelationship.getOtherNode(skelNodeOrSynapseNode));
                    }
                    // links to relationships for skel nodes
                    for (Relationship linksToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName(LINKS_TO), Direction.OUTGOING)) {
                        acquireWriteLockForRelationship(linksToRelationship);
                    }
                }
            }
            // connection sets
            for (Relationship toRelationship : segment.getRelationships(RelationshipType.withName(TO))) {
                acquireWriteLockForRelationship(toRelationship);
                Node connectionSetNode = toRelationship.getStartNode();
                acquireWriteLockForNode(connectionSetNode);
                Relationship fromRelationship = connectionSetNode.getSingleRelationship(RelationshipType.withName(FROM), Direction.OUTGOING);
                acquireWriteLockForRelationship(fromRelationship);
            }
            for (Relationship fromRelationship : segment.getRelationships(RelationshipType.withName(FROM))) {
                acquireWriteLockForRelationship(fromRelationship);
                Node connectionSetNode = fromRelationship.getStartNode();
                Relationship toRelationship = connectionSetNode.getSingleRelationship(RelationshipType.withName(TO), Direction.OUTGOING);
                acquireWriteLockForRelationship(toRelationship);
            }
        }
    }

    private void acquireWriteLockForNode(Node node) {
        if (node != null) {
            try (Transaction tx = dbService.beginTx()) {
                tx.acquireWriteLock(node);
                tx.success();
            }
        }
    }

    private void acquireWriteLockForRelationship(Relationship relationship) {
        if (relationship != null) {
            try (Transaction tx = dbService.beginTx()) {
                tx.acquireWriteLock(relationship);
                tx.success();
            }
        }
    }

}
