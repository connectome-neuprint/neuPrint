package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

class MetaNodeUpdater {

    static void updateMetaNode(Long metaNodeId, GraphDatabaseService dbService, String dataset, boolean shouldMetaNodeSynapseCountsBeUpdated, Log log) {

        try {
            Node metaNode = dbService.getNodeById(metaNodeId);
            getWriteLockForNode(metaNode, dbService);
            metaNode.setProperty("lastDatabaseEdit", LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));

            if (shouldMetaNodeSynapseCountsBeUpdated) {
                long preCount = getTotalPreCount(dbService, dataset);
                long postCount = getTotalPostCount(dbService, dataset);

                // roiInfo not updated

                metaNode.setProperty("totalPreCount", preCount);
                metaNode.setProperty("totalPostCount", postCount);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static long getTotalPreCount(GraphDatabaseService dbService, final String dataset) {
        Result preCountQuery = dbService.execute("MATCH (n:`" + dataset + "-PreSyn`) RETURN count(n)");
        return (long) preCountQuery.next().get("count(n)");
    }

    private static long getTotalPostCount(GraphDatabaseService dbService, final String dataset) {
        Result postCountQuery = dbService.execute("MATCH (n:`" + dataset + "-PostSyn`) RETURN count(n)");
        return (long) postCountQuery.next().get("count(n)");
    }

    private static long getRoiPreCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPreCountQuery = dbService.execute("MATCH (n:`" + dataset + "-PreSyn`{`" + roi + "`:true}) RETURN count(n)");
        return (long) roiPreCountQuery.next().get("count(n)");
    }

    private static long getRoiPostCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPostCountQuery = dbService.execute("MATCH (n:`" + dataset + "-PostSyn`{`" + roi + "`:true}) RETURN count(n)");
        return (long) roiPostCountQuery.next().get("count(n)");
    }

    private static void getWriteLockForNode(Node node, GraphDatabaseService dbService) {
        try (Transaction tx = dbService.beginTx()) {
            tx.acquireWriteLock(node);
            tx.success();
        }
    }
}
