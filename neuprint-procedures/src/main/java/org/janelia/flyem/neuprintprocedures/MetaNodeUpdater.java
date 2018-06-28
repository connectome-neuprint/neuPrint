package org.janelia.flyem.neuprintprocedures;

import com.google.common.graph.Graph;
import org.janelia.flyem.neuprinter.model.Roi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetaNodeUpdater {


    public static void updateMetaNode(Long metaNodeId, GraphDatabaseService dbService, String dataset) {

        try {
            Node metaNode = dbService.getNodeById(metaNodeId);
            long preCount = getTotalPreCount(dbService, dataset);
            long postCount = getTotalPostCount(dbService, dataset);


            metaNode.setProperty("lastDatabaseEdit", LocalDate.now());
            metaNode.setProperty("totalPreCount", preCount);
            metaNode.setProperty("totalPostCount", postCount);

//            List<String> roiNameList = getAllRois(dbService,dataset);
//            Set<Roi> roiSet = new HashSet<>();
//
//            for (String roi : roiNameList) {
//                if (!roi.equals("NeuronPart") && !roi.equals(dataset)) {
//                    long roiPreCount = getRoiPreCount(dbService, dataset, roi);
//                    long roiPostCount = getRoiPostCount(dbService, dataset, roi);
//                    roiSet.add(new Roi(roi,roiPreCount,roiPostCount));
//                }
//            }
//
//            for (Roi roi : roiSet) {
//
//                metaNode.setProperty(roi.getRoiName() + "PreCount", roi.getPreCount());
//                metaNode.setProperty(roi.getRoiName() + "PostCount", roi.getPostCount());
//
//            }
//
//

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
        Result roiPreCountQuery = dbService.execute("MATCH (n:NeuronPart:" + dataset + ":" + roi + ") RETURN sum(n.pre) AS pre");
        return (long) roiPreCountQuery.next().get("pre");
    }

    private static long getRoiPostCount(GraphDatabaseService dbService, final String dataset, final String roi) {
        Result roiPostCountQuery = dbService.execute("MATCH (n:NeuronPart:" + dataset + ":" + roi + ") RETURN sum(n.post)");
        return (long) roiPostCountQuery.next().get("post");
    }

    private static List<String> getAllRois(GraphDatabaseService dbService, final String dataset) {
        List<String> roiList = new ArrayList<>();
        Result roiLabelQuery = dbService.execute("MATCH (n:NeuronPart:" + dataset + ") WITH distinct labels(n) AS labels UNWIND labels AS label RETURN DISTINCT label");

        while (roiLabelQuery.hasNext()) {
            roiList.add((String) roiLabelQuery.next().get("label"));
        }
        return roiList;
    }
}
