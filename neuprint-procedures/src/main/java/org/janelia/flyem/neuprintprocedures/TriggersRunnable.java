package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;

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
        //TODO: separate class for organizing transaction data, then run time stamp on relevant nodes, keep track of total number of pre and post for the whole volume and per ROI--> update the meta data node

        TransactionDataHandler transactionDataHandler = new TransactionDataHandler(transactionData);

        try (Transaction tx = dbService.beginTx()) {

            Set<Long> nodesForTimeStamping = transactionDataHandler.getNodesForTimeStamping();


            if (nodesForTimeStamping.size() > 0) {
                System.out.println("the following nodes will be time-stamped: " + nodesForTimeStamping);
                TimeStampProcedure.timeStampEmbedded(nodesForTimeStamping, dbService);
                tx.success();
                System.out.println("Completed time stamping.");
            }


        }

        if (!transactionDataHandler.shouldUpdateMetaNode()) {

            try (Transaction tx = dbService.beginTx()) {

                Result metaNodesQueryResult = dbService.execute("MATCH (n:Meta) RETURN n");


                while (metaNodesQueryResult.hasNext()) {
                    Node metaNode = (Node) metaNodesQueryResult.next().get("n");
                    Long metaNodeId = metaNode.getId();
                    String dataset = (String) metaNode.getProperty("dataset");
                    MetaNodeUpdater.updateMetaNode(metaNodeId, dbService, dataset);
                    tx.success();
                    System.out.println("meta node updated: " + metaNode.getAllProperties());

                }

            } catch (Exception e) {
                System.out.println("failed to update meta node: " + e);
            }


        } else {
            System.out.println("did not update meta node");
        }
    }
}


