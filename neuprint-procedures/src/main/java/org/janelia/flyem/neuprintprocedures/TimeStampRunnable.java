package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.util.HashSet;
import java.util.Set;


public class TimeStampRunnable implements Runnable {

    private static TransactionData transactionData;
    private static GraphDatabaseService dbService;

    protected TimeStampRunnable(TransactionData transactionData, GraphDatabaseService graphDatabaseService) {
        TimeStampRunnable.transactionData = transactionData;
        dbService = graphDatabaseService;
    }

    @Override
    public void run() {
        //TODO: separate class for organizing transaction data, then run time stamp on relevant nodes, keep track of total number of pre and post for the whole volume and per ROI--> update the meta data node

        Boolean timeStampTransaction = false;

        try (Transaction tx = dbService.beginTx()) {

            Set<Long> nodesForTimeStamping = TransactionDataHandler.getNodesForTimeStamping();

            if (nodesForTimeStamping.size() > 0) {
                System.out.println("the following nodes will be time-stamped: " + nodesForTimeStamping);
                // TODO: probably want to batch this.
                TimeStampProcedure.timeStampEmbedded(nodesForTimeStamping, dbService);
                tx.success();
                System.out.println("Completed time stamping.");
            }


        }


    }

}
