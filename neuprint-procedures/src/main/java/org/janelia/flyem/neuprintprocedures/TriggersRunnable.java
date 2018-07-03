package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;


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


