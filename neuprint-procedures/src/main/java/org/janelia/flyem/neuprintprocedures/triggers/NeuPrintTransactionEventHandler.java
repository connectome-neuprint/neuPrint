package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.concurrent.ExecutorService;

public class NeuPrintTransactionEventHandler implements TransactionEventHandler {

    private static GraphDatabaseService dbService;
    private static ExecutorService executorService;

    NeuPrintTransactionEventHandler(GraphDatabaseService graphDatabaseService, ExecutorService executorService) {
        dbService = graphDatabaseService;
        NeuPrintTransactionEventHandler.executorService = executorService;
    }

    @Override
    public Object beforeCommit(TransactionData transactionData) {
        return null;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Object o) {
        TriggersRunnable triggersRunnable = new TriggersRunnable(transactionData, dbService);
        executorService.submit(triggersRunnable);
    }

    @Override
    public void afterRollback(TransactionData transactionData, Object o) {

    }

}
