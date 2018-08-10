package org.janelia.flyem.neuprintprocedures;

import org.janelia.flyem.neuprinter.model.Roi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MetaNodeUpdater {


    public static void updateMetaNode(Long metaNodeId, GraphDatabaseService dbService, String dataset) {

        try {
            Node metaNode = dbService.getNodeById(metaNodeId);
            getWriteLockForNode(metaNode, dbService);

            long preCount = getTotalPreCount(dbService, dataset);
            long postCount = getTotalPostCount(dbService, dataset);

            List<String> roiNameList = getAllRoisFromNeurons(dbService, dataset)
                    .stream()
                    .filter((l) -> (!l.equals("Neuron") && !l.equals(dataset)))
                    .collect(Collectors.toList());
            Set<Roi> roiSet = new HashSet<>();

            for (String roi : roiNameList) {
                long roiPreCount = getRoiPreCount(dbService, dataset, roi);
                long roiPostCount = getRoiPostCount(dbService, dataset, roi);
                roiSet.add(new Roi(roi, roiPreCount, roiPostCount));
            }


            List<String> roiNameListWithoutNonDisjointRois = roiNameList
                    .stream()
                    .filter((l) -> (!l.equals("seven_column_roi") && !l.equals("kc_alpha_roi")))
                    .collect(Collectors.toList());

            metaNode.setProperty("lastDatabaseEdit", LocalDate.now());
            metaNode.setProperty("totalPreCount", preCount);
            metaNode.setProperty("totalPostCount", postCount);
            metaNode.setProperty("rois", roiNameListWithoutNonDisjointRois
                    .toArray(new String[roiNameListWithoutNonDisjointRois.size()]));


            for (Roi roi : roiSet) {
                System.out.println("setting " + roi.getRoiName() + " pre:" + roi.getPreCount() + " post:" + roi.getPostCount());
                metaNode.setProperty(roi.getRoiName() + "PreCount", roi.getPreCount());
                metaNode.setProperty(roi.getRoiName() + "PostCount", roi.getPostCount());

            }


        } catch (Exception e) {
            System.out.println(e);
        }


    }


    private static long getTotalPreCount(GraphDatabaseService dbService, final String dataset) {
        Result preCountQuery = dbService.execute("MATCH (n:Neuron:" + dataset + ") RETURN sum(n.pre) AS pre");
        return (long) preCountQuery.next().get("pre");
    }

    private static long getTotalPostCount(GraphDatabaseService dbService, final String dataset) {
        Result postCountQuery = dbService.execute("MATCH (n:Neuron:" + dataset + ") RETURN sum(n.post) AS post");
        return (long) postCountQuery.next().get("post");
    }

    private static long getRoiPreCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPreCountQuery = dbService.execute("MATCH (n:Neuron:" + dataset + ":`Neu-" + roi + "`) WITH apoc.convert.fromJsonMap(n.synapseCountPerRoi).`" + roi + "`.pre AS preCounts RETURN sum(preCounts) AS pre");
        return (long) roiPreCountQuery.next().get("pre");
    }

    private static long getRoiPostCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPostCountQuery = dbService.execute("MATCH (n:Neuron:" + dataset + ":`Neu-" + roi + "`) WITH apoc.convert.fromJsonMap(n.synapseCountPerRoi).`" + roi + "`.post AS postCounts RETURN sum(postCounts) AS post");
        return (long) roiPostCountQuery.next().get("post");
    }

    private static List<String> getAllRoisFromNeurons(GraphDatabaseService dbService, final String dataset) {
        List<String> roiList = new ArrayList<>();
        Result roiLabelQuery = dbService.execute("MATCH (n:Neuron:" + dataset + ") WITH labels(n) AS labels UNWIND labels AS label WITH DISTINCT label ORDER BY label RETURN label");

        while (roiLabelQuery.hasNext()) {
            roiList.add(roiLabelQuery.next().get("label").toString().replace("Neu-",""));
        }
        return roiList;
    }

    private static void getWriteLockForNode(Node node, GraphDatabaseService dbService) {
        try (Transaction tx = dbService.beginTx()) {
            tx.acquireWriteLock(node);
            tx.success();
        }
    }
}
