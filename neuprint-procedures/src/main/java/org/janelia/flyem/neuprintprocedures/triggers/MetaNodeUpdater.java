package org.janelia.flyem.neuprintprocedures.triggers;

import org.janelia.flyem.neuprinter.model.SynapseCountsPerRoi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

class MetaNodeUpdater {

    static void updateMetaNode(Long metaNodeId, GraphDatabaseService dbService, String dataset, boolean shouldMetaNodeSynapseCountsBeUpdated) {

        try {
            Node metaNode = dbService.getNodeById(metaNodeId);
            getWriteLockForNode(metaNode, dbService);
            metaNode.setProperty("lastDatabaseEdit", LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));

            if (shouldMetaNodeSynapseCountsBeUpdated) {
                long preCount = getTotalPreCount(dbService, dataset);
                long postCount = getTotalPostCount(dbService, dataset);

                Set<String> roiNameSet = getAllRoisFromSegments(dbService, dataset)
                        .stream()
                        .filter((p) -> (
                                !p.equals("autoName") &&
                                        !p.equals("bodyId") &&
                                        !p.equals("name") &&
                                        !p.equals("post") &&
                                        !p.equals("pre") &&
                                        !p.equals("size") &&
                                        !p.equals("status") &&
                                        !p.equals("roiInfo") &&
                                        !p.equals("timeStamp") &&
                                        !p.equals("type")) &&
                                !p.equals("somaLocation") &&
                                !p.equals("somaRadius"))
                        .collect(Collectors.toSet());
                SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();

                for (String roi : roiNameSet) {
                    long roiPreCount = getRoiPreCount(dbService, dataset, roi);
                    long roiPostCount = getRoiPostCount(dbService, dataset, roi);
                    synapseCountsPerRoi.addSynapseCountsForRoi(roi, Math.toIntExact(roiPreCount), Math.toIntExact(roiPostCount));
                }

                metaNode.setProperty("totalPreCount", preCount);
                metaNode.setProperty("totalPostCount", postCount);
                metaNode.setProperty("roiInfo", synapseCountsPerRoi.getAsJsonString());
                System.out.println(LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS) + " Setting roiInfo on Meta node: " + synapseCountsPerRoi.getAsJsonString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static long getTotalPreCount(GraphDatabaseService dbService, final String dataset) {
        Result preCountQuery = dbService.execute("MATCH (n:`" + dataset + "-Segment`) RETURN sum(n.pre) AS pre");
        return (long) preCountQuery.next().get("pre");
    }

    private static long getTotalPostCount(GraphDatabaseService dbService, final String dataset) {
        Result postCountQuery = dbService.execute("MATCH (n:`" + dataset + "-Segment`) RETURN sum(n.post) AS post");
        return (long) postCountQuery.next().get("post");
    }

    private static long getRoiPreCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPreCountQuery = dbService.execute("MATCH (n:`" + dataset + "-Segment`{`" + roi + "`:true}) WITH apoc.convert.fromJsonMap(n.roiInfo).`" + roi + "`.pre AS preCounts RETURN sum(preCounts) AS pre");
        return (long) roiPreCountQuery.next().get("pre");
    }

    private static long getRoiPostCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPostCountQuery = dbService.execute("MATCH (n:`" + dataset + "-Segment`{`" + roi + "`:true}) WITH apoc.convert.fromJsonMap(n.roiInfo).`" + roi + "`.post AS postCounts RETURN sum(postCounts) AS post");
        return (long) roiPostCountQuery.next().get("post");
    }

    private static Set<String> getAllRoisFromSegments(GraphDatabaseService dbService, final String dataset) {
        Set<String> roiSet = new HashSet<>();
        Result roiLabelQuery = dbService.execute("MATCH (n:`" + dataset + "-Segment`) WITH keys(n) AS props UNWIND props AS prop WITH DISTINCT prop ORDER BY prop RETURN prop");

        while (roiLabelQuery.hasNext()) {
            roiSet.add(roiLabelQuery.next().get("prop").toString());
        }
        return roiSet;
    }

    private static void getWriteLockForNode(Node node, GraphDatabaseService dbService) {
        try (Transaction tx = dbService.beginTx()) {
            tx.acquireWriteLock(node);
            tx.success();
        }
    }
}
