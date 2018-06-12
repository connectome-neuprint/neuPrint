package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.TransactionData;


public class TimeStampRunnable {

    private static TransactionData transactionData;
    private static GraphDatabaseService dbService;

    public TimeStampRunnable(TransactionData transactionData, GraphDatabaseService graphDatabaseService) {
        TimeStampRunnable.transactionData = transactionData;
        dbService = graphDatabaseService;
    }


    public void run() {

        final TimeStampProcedure timeStampProcedure = new TimeStampProcedure();

        for (Node node : transactionData.createdNodes()) {
            TimeStampProcedure.timeStampEmbedded(node.getId(), dbService);
        }


    }

}
