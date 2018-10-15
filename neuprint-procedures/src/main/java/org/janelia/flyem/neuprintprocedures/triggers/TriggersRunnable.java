package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TriggersRunnable implements Runnable {

    private static TransactionData transactionData;
    private static GraphDatabaseService dbService;

    TriggersRunnable(TransactionData transactionData, GraphDatabaseService graphDatabaseService) {
        TriggersRunnable.transactionData = transactionData;
        dbService = graphDatabaseService;
    }

    @Override
    public void run() {

        TransactionDataHandler transactionDataHandler = new TransactionDataHandler(transactionData);
        Map<String, Node> datasetToMetaNodeMap = new HashMap<>();

        try (Transaction tx = dbService.beginTx()) {

            // get all datasets in the database from meta nodes
            ResourceIterator<Node> metaNodeIterator = dbService.findNodes(Label.label("Meta"));
            while (metaNodeIterator.hasNext()) {
                Node metaNode = metaNodeIterator.next();
                String dataset = (String) metaNode.getProperty("dataset");
                datasetToMetaNodeMap.put(dataset, metaNode);
            }

            Set<Node> nodesForTimeStamping = transactionDataHandler.getNodesForTimeStamping(datasetToMetaNodeMap.keySet());

            if (transactionDataHandler.shouldTimeStampAndUpdateMetaNodeTimeStamp()) {
                //System.out.println("the following nodes will be time-stamped: " + nodesForTimeStamping);
                TimeStampProcedure.timeStampEmbedded(nodesForTimeStamping, dbService);

                for (String dataset : transactionDataHandler.getDatasetsChanged()) {
                    Node metaNode = datasetToMetaNodeMap.get(dataset);
                    Long metaNodeId = metaNode.getId();
                    MetaNodeUpdater.updateMetaNode(metaNodeId, dbService, dataset, transactionDataHandler.getShouldMetaNodeSynapseCountsBeUpdated());
                }

                tx.success();
                System.out.println(LocalDateTime.now() + " Completed time stamping and updating Meta node.");
            }

        }

    }
}


