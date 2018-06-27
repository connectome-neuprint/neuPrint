package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.util.HashSet;
import java.util.Set;

public class TransactionDataHandler {

    static TransactionData transactionData;

    public TransactionDataHandler(TransactionData transactionData) {
        TransactionDataHandler.transactionData = transactionData;
    }

    public static Set<Long> getNodesForTimeStamping() {
        Set<Long> nodesForTimeStamping = new HashSet<>();
        for (Node node : transactionData.createdNodes()) {
            nodesForTimeStamping.add(node.getId());
            System.out.println("node created: " + node);
        }

        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            nodesForTimeStamping.add(labelEntry.node().getId());
            System.out.println("label entry assigned: " + labelEntry);
        }

        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            nodesForTimeStamping.add(labelEntry.node().getId());
            System.out.println("label entry removed: " + labelEntry);
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.assignedNodeProperties()) {
            if (!propertyEntry.key().equals("timeStamp")) {
                Long assignedPropertiesNodeId = propertyEntry.entity().getId();
                nodesForTimeStamping.add(assignedPropertiesNodeId);
                if (propertyEntry.key().equals("pre") || propertyEntry.key().equals("post")) {
                    System.out.println("node properties assigned: " + propertyEntry);
                }
            }
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.removedNodeProperties()) {
            if (!propertyEntry.key().equals("timeStamp")) {
                Long removedPropertiesNodeId = propertyEntry.entity().getId();
                nodesForTimeStamping.add(removedPropertiesNodeId);
                System.out.println("node properties removed: " + propertyEntry);
            }
        }

        for (PropertyEntry<Relationship> propertyEntry : transactionData.assignedRelationshipProperties()) {
            Relationship relationship = propertyEntry.entity();
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                nodesForTimeStamping.add(node.getId());
                System.out.println("relationship properties added for: " + node);
            }
        }

        for (PropertyEntry<Relationship> propertyEntry : transactionData.removedRelationshipProperties()) {
            Relationship relationship = propertyEntry.entity();
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                nodesForTimeStamping.add(node.getId());
                System.out.println("relationship properties removed for: " + node);
            }
        }

        for (Relationship relationship : transactionData.createdRelationships()) {
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                nodesForTimeStamping.add(node.getId());
                System.out.println("relationship created for: " + node);
            }
        }

        for (Relationship relationship : transactionData.deletedRelationships()) {
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                nodesForTimeStamping.add(node.getId());
                System.out.println("relationship deleted for: " + node);
            }
        }

        return nodesForTimeStamping;

    }






}
