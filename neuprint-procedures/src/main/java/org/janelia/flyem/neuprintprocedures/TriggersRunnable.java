package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TriggersRunnable implements Runnable {

    private static TransactionData transactionData;
    private static GraphDatabaseService dbService;

    protected TriggersRunnable(TransactionData transactionData, GraphDatabaseService graphDatabaseService) {
        TriggersRunnable.transactionData = transactionData;
        dbService = graphDatabaseService;
    }

    @Override
    public void run() {

        TransactionDataHandler transactionDataHandler = new TransactionDataHandler(transactionData);
        List<Node> metaNodeList = new ArrayList<>();
        try (Transaction tx = dbService.beginTx()) {

            Set<Long> nodesForTimeStamping = transactionDataHandler.getNodesForTimeStamping();

            if (transactionDataHandler.shouldTimeStampAndUpdateMetaNode()) {
                System.out.println("the following nodes will be time-stamped: " + nodesForTimeStamping);
                TimeStampProcedure.timeStampEmbedded(nodesForTimeStamping, dbService);

                ResourceIterator<Node> metaNodeIterator = dbService.findNodes(Label.label("Meta"));
                while (metaNodeIterator.hasNext()) {
                    metaNodeList.add(metaNodeIterator.next());
                }
                // TODO: check for changes per dataset
                for (Node metaNode : metaNodeList) {
                    Long metaNodeId = metaNode.getId();
                    String dataset = (String) metaNode.getProperty("dataset");
                    MetaNodeUpdater.updateMetaNode(metaNodeId, dbService, dataset);
                    System.out.println("meta node updated: " + metaNode.getAllProperties());
                }

                tx.success();
                System.out.println("Completed time stamping.");
            }

        }

    }
}


