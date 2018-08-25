package org.janelia.flyem.neuprintprocedures;

import org.janelia.flyem.neuprinter.model.SynapseCountsPerRoi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

class MetaNodeUpdater {

    static void updateMetaNode(Long metaNodeId, GraphDatabaseService dbService, String dataset, boolean shouldMetaNodeSynapseCountsBeUpdated) {

        try {
            Node metaNode = dbService.getNodeById(metaNodeId);
            getWriteLockForNode(metaNode, dbService);
            metaNode.setProperty("lastDatabaseEdit", LocalDate.now());

            if (shouldMetaNodeSynapseCountsBeUpdated) {
                long preCount = getTotalPreCount(dbService, dataset);
                long postCount = getTotalPostCount(dbService, dataset);

                Set<String> roiNameSet = getAllRoisFromNeurons(dbService, dataset)
                        .stream()
                        .filter((l) -> (!l.equals("Neuron") && !l.startsWith(dataset)))
                        .collect(Collectors.toSet());
                SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();

                for (String roi : roiNameSet) {
                    long roiPreCount = getRoiPreCount(dbService, dataset, roi);
                    long roiPostCount = getRoiPostCount(dbService, dataset, roi);
                    synapseCountsPerRoi.addSynapseCountsForRoi(roi, Math.toIntExact(roiPreCount), Math.toIntExact(roiPostCount));
                }

                metaNode.setProperty("totalPreCount", preCount);
                metaNode.setProperty("totalPostCount", postCount);
                metaNode.setProperty("synapseCountPerRoi", synapseCountsPerRoi.getAsJsonString());
                System.out.println(LocalDateTime.now() + " Setting synapseCountPerRoi on Meta node: " + synapseCountsPerRoi.getAsJsonString());
            }

        } catch (Exception e) {
            System.out.println(e);
        }

    }

    private static long getTotalPreCount(GraphDatabaseService dbService, final String dataset) {
        Result preCountQuery = dbService.execute("MATCH (n:`" + dataset + "-Neuron`) RETURN sum(n.pre) AS pre");
        return (long) preCountQuery.next().get("pre");
    }

    private static long getTotalPostCount(GraphDatabaseService dbService, final String dataset) {
        Result postCountQuery = dbService.execute("MATCH (n:`" + dataset + "-Neuron`) RETURN sum(n.post) AS post");
        return (long) postCountQuery.next().get("post");
    }

    private static long getRoiPreCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPreCountQuery = dbService.execute("MATCH (n:`" + dataset + "-" + roi + "`) WITH apoc.convert.fromJsonMap(n.synapseCountPerRoi).`" + roi + "`.pre AS preCounts RETURN sum(preCounts) AS pre");
        return (long) roiPreCountQuery.next().get("pre");
    }

    private static long getRoiPostCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPostCountQuery = dbService.execute("MATCH (n:`" + dataset + "-" + roi + "`) WITH apoc.convert.fromJsonMap(n.synapseCountPerRoi).`" + roi + "`.post AS postCounts RETURN sum(postCounts) AS post");
        return (long) roiPostCountQuery.next().get("post");
    }

    private static Set<String> getAllRoisFromNeurons(GraphDatabaseService dbService, final String dataset) {
        Set<String> roiSet = new HashSet<>();
        Result roiLabelQuery = dbService.execute("MATCH (n:`" + dataset + "-Neuron`) WITH labels(n) AS labels UNWIND labels AS label WITH DISTINCT label ORDER BY label RETURN label");

        while (roiLabelQuery.hasNext()) {
            roiSet.add(roiLabelQuery.next().get("label").toString());
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
