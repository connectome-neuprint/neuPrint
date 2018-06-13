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

    public TimeStampRunnable(TransactionData transactionData, GraphDatabaseService graphDatabaseService) {
        TimeStampRunnable.transactionData = transactionData;
        dbService = graphDatabaseService;
    }

    @Override
    public void run() {

        try (Transaction tx = dbService.beginTx()) {

            System.out.println("entered run");

                Set<Long> nodesForTimeStamping = new HashSet<>();
                for (Node node : transactionData.createdNodes()) {
                    nodesForTimeStamping.add(node.getId());
                    System.out.println("nodes created: " + node);
                }

                for (LabelEntry labelEntry : transactionData.assignedLabels()) {
                    nodesForTimeStamping.add(labelEntry.node().getId());
                    System.out.println("label entries assigned: " + labelEntry);
                }

                for (LabelEntry labelEntry : transactionData.removedLabels()) {
                    nodesForTimeStamping.add(labelEntry.node().getId());
                    System.out.println("label entries removed: " + labelEntry);
                }

                for (PropertyEntry<Node> propertyEntry : transactionData.assignedNodeProperties()) {
                    if (!propertyEntry.key().equals("timeStamp")) {
                    Long assignedPropertiesNodeId = propertyEntry.entity().getId();
                    nodesForTimeStamping.add(assignedPropertiesNodeId);
                    System.out.println("node properties assigned: " + propertyEntry);
                    } else { System.out.println("timestamp added."); }

                }

                for (PropertyEntry<Node> propertyEntry : transactionData.removedNodeProperties()) {
                    if (!propertyEntry.key().equals("timeStamp")) {
                    Long removedPropertiesNodeId = propertyEntry.entity().getId();
                    nodesForTimeStamping.add(removedPropertiesNodeId);
                    System.out.println("node properties removed: " + propertyEntry);
                    } else { System.out.println("timestamp removed."); }
                }
//
//            for (PropertyEntry<Relationship> propertyEntry : transactionData.assignedRelationshipProperties()) {
//                Relationship relationship = propertyEntry.entity();
//                Node[] nodes = relationship.getNodes();
//                for (Node node : nodes) {
//                    TimeStampProcedure.timeStampEmbedded(node.getId(), dbService);
//                    System.out.println("relationship properties added for: " + node );
//                }
//            }
//
//        for (PropertyEntry<Relationship> propertyEntry : transactionData.removedRelationshipProperties()) {
//            Relationship relationship = propertyEntry.entity();
//            Node[] nodes = relationship.getNodes();
//            for (Node node : nodes) {
//                TimeStampProcedure.timeStampEmbedded(node.getId(), dbService);
//                System.out.println("relationship properties removed for: " + node );
//            }
//        }
//
//        for (Relationship relationship : transactionData.createdRelationships()) {
//            Node[] nodes = relationship.getNodes();
//            for (Node node : nodes) {
//                TimeStampProcedure.timeStampEmbedded(node.getId(), dbService);
//                System.out.println("relationship created for: " + node );
//            }
//        }
//
//        for (Relationship relationship : transactionData.deletedRelationships()) {
//            Node[] nodes = relationship.getNodes();
//            for (Node node : nodes) {
//                TimeStampProcedure.timeStampEmbedded(node.getId(), dbService);
//                System.out.println("relationship deleted for: " + node );
//            }
//        }



                System.out.println(nodesForTimeStamping);
            // TODO: probably want to batch this.
                TimeStampProcedure.timeStampEmbedded(nodesForTimeStamping, dbService);

                tx.success();


        }


    }

}
