package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
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
        List<Node> metaNodeList = new ArrayList<>();
        try (Transaction tx = dbService.beginTx()) {

            Set<Node> nodesForTimeStamping = transactionDataHandler.getNodesForTimeStamping();

            if (transactionDataHandler.shouldTimeStampAndUpdateMetaNodeTimeStamp()) {
                //System.out.println("the following nodes will be time-stamped: " + nodesForTimeStamping);
                TimeStampProcedure.timeStampEmbedded(nodesForTimeStamping, dbService);

                ResourceIterator<Node> metaNodeIterator = dbService.findNodes(Label.label("Meta"));
                while (metaNodeIterator.hasNext()) {
                    metaNodeList.add(metaNodeIterator.next());
                }
                // TODO: check for changes per dataset
                for (Node metaNode : metaNodeList) {
                    Long metaNodeId = metaNode.getId();
                    String dataset = (String) metaNode.getProperty("dataset");
                    MetaNodeUpdater.updateMetaNode(metaNodeId, dbService, dataset, transactionDataHandler.getShouldMetaNodeSynapseCountsBeUpdated());
                }

                tx.success();
                System.out.println(LocalDateTime.now() + " Completed time stamping and updating Meta node.");
            }

        }

    }
}


