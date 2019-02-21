package org.janelia.flyem.neuprintloadprocedures.procedures;

import org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools;
import org.janelia.flyem.neuprintloadprocedures.model.RoiInfoWithHighPrecisionCounts;
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

import java.util.Set;

import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONFIDENCE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONNECTS_TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.CONTAINS;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.FROM;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.POST;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.PRE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.ROI_INFO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.TO;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.TYPE;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.WEIGHT_HP;
import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapseRois;

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
                log.error(String.format("loader.setConnectionSetRoiInfoAndWeightHP: ConnectionSet does not exist: %d to %d in dataset %s. ", preBodyId, postBodyId, datasetLabel));
                throw new RuntimeException(String.format("loader.setConnectionSetRoiInfoAndWeightHP: ConnectionSet does not exist: %d to %d in dataset %s. ", preBodyId, postBodyId, datasetLabel));
            }

            // get all synapses on that connection set
            Set<Node> synapsesForConnectionSet = GraphTraversalTools.getSynapsesForConnectionSet(connectionSet);

            // for each pre/post add to count and check confidence to add to hp count
            RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts();
            // keep track of total postHP count to add to ConnectsTo relationship
            int postHP = 0;

            for (Node synapse : synapsesForConnectionSet) {
                String type;
                Double confidence;
                if (synapse.hasProperty(TYPE)) {
                    type = (String) synapse.getProperty(TYPE);
                } else {
                    type = null;
                    log.error("loader.setConnectionSetRoiInfoAndWeightHP: Synapse has no type property. Not added to roiInfo.");
                }
                if (synapse.hasProperty(CONFIDENCE)) {
                    confidence = (Double) synapse.getProperty(CONFIDENCE);
                } else {
                    confidence = null;
                    log.error("loader.setConnectionSetRoiInfoAndWeightHP: Synapse has no confidence property.");
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
                    for (String roi : synapseRois) {
                        roiInfo.incrementPostForRoi(roi);
                        roiInfo.incrementPostHPForRoi(roi);
                    }
                } else if (type.equals(POST)) {
                    for (String roi : synapseRois) {
                        roiInfo.incrementPostForRoi(roi);
                    }
                }
            }

            // add to connection set node
            connectionSet.setProperty(ROI_INFO, roiInfo.getAsJsonString());

            // add postHP to ConnectsTo
            try {
                addPostHPToConnectsTo(connectionSet, postHP);
            } catch (Exception e) {
                log.error("loader.setConnectionSetRoiInfoAndWeightHP: Error adding weightHP: " + e);
                throw new RuntimeException("loader.setConnectionSetRoiInfoAndWeightHP: Error adding weightHP: " + e);
            }

        } catch (Exception e) {
            log.info("loader.setConnectionSetRoiInfoAndWeightHP: Error adding roiInfo:" + e);
            throw new RuntimeException("loader.setConnectionSetRoiInfoAndWeightHP: Error adding roiInfo:" + e);
        }

        log.info("loader.setConnectionSetRoiInfoAndWeightHP: exit");

    }

    public void addPostHPToConnectsTo(Node connectionSet, int postHP) {
        Node preSynapticNode = connectionSet.getSingleRelationship(RelationshipType.withName(FROM), Direction.OUTGOING).getEndNode();
        long postSynapticNodeId = connectionSet.getSingleRelationship(RelationshipType.withName(TO), Direction.OUTGOING).getEndNodeId();

        Iterable<Relationship> connectsToRelationships = preSynapticNode.getRelationships(RelationshipType.withName(CONNECTS_TO), Direction.OUTGOING);

        for (Relationship connectsToRel : connectsToRelationships) {
            long endNodeIdForRel = connectsToRel.getEndNodeId();
            if (postSynapticNodeId==endNodeIdForRel) {
                connectsToRel.setProperty(WEIGHT_HP,postHP);
            }
        }
    }

}
