package org.janelia.flyem.neuprintprocedures.loading;

import org.janelia.flyem.neuprinter.model.RoiInfoWithHighPrecisionCounts;
import org.janelia.flyem.neuprintprocedures.GraphTraversalTools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Set;

import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.CONFIDENCE;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.POST;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.PRE;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.TYPE;
import static org.janelia.flyem.neuprintprocedures.GraphTraversalTools.getSynapseRois;

public class LoadingProcedures {
    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @Procedure(value = "loader.setConnectionSetRoiInfo", mode = Mode.WRITE)
    @Description("loader.setConnectionSetRoiInfo : Add roiInfo property to ConnectionSet node.")
    public void setConnectionSetRoiInfo(@Name("preBodyId") final Long preBodyId,
                                        @Name("postBodyId") final Long postBodyId,
                                        @Name("datasetLabel") final String datasetLabel,
                                        @Name("preHPThreshold") final Double preHPThreshold,
                                        @Name("postHPThreshold") final Double postHPThreshold) {

        log.info("loader.setConnectionSetRoiInfo: entry");

        if (preBodyId == null || postBodyId == null || datasetLabel == null || preHPThreshold == null || postHPThreshold == null) {
            log.error("loader.setConnectionSetRoiInfo: Missing input arguments.");
            throw new RuntimeException("loader.setConnectionSetRoiInfo: Missing input arguments.");
        }

        // get a connection set
        Node connectionSet = GraphTraversalTools.getConnectionSetNode(dbService, preBodyId, postBodyId, datasetLabel);

        if (connectionSet == null) {
            log.error(String.format("loader.setConnectionSetRoiInfo: ConnectionSet does not exist: %d to %d in dataset %s. ", preBodyId, postBodyId, datasetLabel));
            throw new RuntimeException(String.format("loader.setConnectionSetRoiInfo: ConnectionSet does not exist: %d to %d in dataset %s. ", preBodyId, postBodyId, datasetLabel));
        }

        // get all synapses on that connection set
        Set<Node> synapsesForConnectionSet = GraphTraversalTools.getSynapsesForConnectionSet(connectionSet);

        // for each pre/post add to count and check confidence to add to hp count
        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts();
        for (Node synapse : synapsesForConnectionSet) {
            String type = (String) synapse.getProperty(TYPE);
            if (type == null) {
                log.error("loader.setConnectionSetRoiInfo: Synapse has no type property.");
                throw new RuntimeException("loader.setConnectionSetRoiInfo: Synapse has no type property.");
            }
            Double confidence = (Double) synapse.getProperty(CONFIDENCE);
            if (confidence == null) {
                log.error("loader.setConnectionSetRoiInfo: Synapse has no confidence property.");
                throw new RuntimeException("loader.setConnectionSetRoiInfo: Synapse has no confidence property.");
            }
            Set<String> synapseRois = getSynapseRois(synapse);
            if (type.equals(PRE) && confidence > preHPThreshold) {
                for (String roi : synapseRois) {
                    roiInfo.incrementPreForRoi(roi);
                    roiInfo.incrementPreHPForRoi(roi);
                }
            } else if (type.equals(PRE)) {
                for (String roi : synapseRois) {
                    roiInfo.incrementPreForRoi(roi);
                }
            } else if (type.equals(POST) && confidence > postHPThreshold) {
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
        connectionSet.setProperty("roiInfo", roiInfo.getAsJsonString());

        log.info("loader.setConnectionSetRoiInfo: exit");

    }
}
