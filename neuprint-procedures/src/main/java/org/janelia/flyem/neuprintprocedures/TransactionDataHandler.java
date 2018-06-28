package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransactionDataHandler {

    private TransactionData transactionData;
    private Set<Long> nodesForTimeStamping;

    public TransactionDataHandler(TransactionData transactionData) {
        this.transactionData = transactionData;
        this.nodesForTimeStamping = new HashSet<>();
    }

    public Set<Long> getNodesForTimeStamping() {

        for (Node node : transactionData.createdNodes()) {
            if (!node.hasLabel(Label.label("Meta"))) {
                this.nodesForTimeStamping.add(node.getId());
                System.out.println("node created: " + node);
            }
        }

        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            Node node = labelEntry.node();
            if (!node.hasLabel(Label.label("Meta"))) {
                this.nodesForTimeStamping.add(node.getId());
                System.out.println("label entry assigned: " + labelEntry);
            }
        }

        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            Node node = labelEntry.node();
            if (!node.hasLabel(Label.label("Meta"))) {
                this.nodesForTimeStamping.add(node.getId());
                System.out.println("label entry removed: " + labelEntry);
            }
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.assignedNodeProperties()) {
            if (!propertyEntry.key().equals("timeStamp")) {
                Node node = propertyEntry.entity();
                if (!node.hasLabel(Label.label("Meta"))) {
                    Long assignedPropertiesNodeId = node.getId();
                    this.nodesForTimeStamping.add(assignedPropertiesNodeId);
                    System.out.println("node properties assigned: " + propertyEntry);
                }
            }
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.removedNodeProperties()) {
            if (!propertyEntry.key().equals("timeStamp")) {
                Node node = propertyEntry.entity();
                if (!node.hasLabel(Label.label("Meta"))) {
                    Long removedPropertiesNodeId = propertyEntry.entity().getId();
                    this.nodesForTimeStamping.add(removedPropertiesNodeId);
                    System.out.println("node properties removed: " + propertyEntry);
                }
            }
        }

        for (PropertyEntry<Relationship> propertyEntry : transactionData.assignedRelationshipProperties()) {
            Relationship relationship = propertyEntry.entity();
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                if (!node.hasLabel(Label.label("Meta"))) {
                    this.nodesForTimeStamping.add(node.getId());
                    System.out.println("relationship properties added for: " + node);
                }
            }
        }

        for (PropertyEntry<Relationship> propertyEntry : transactionData.removedRelationshipProperties()) {
            Relationship relationship = propertyEntry.entity();
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                if (!node.hasLabel(Label.label("Meta"))) {
                    this.nodesForTimeStamping.add(node.getId());
                    System.out.println("relationship properties removed for: " + node);
                }
            }
        }

        for (Relationship relationship : transactionData.createdRelationships()) {
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                if (!node.hasLabel(Label.label("Meta"))) {
                    this.nodesForTimeStamping.add(node.getId());
                    System.out.println("relationship created for: " + node);
                }
            }
        }

        for (Relationship relationship : transactionData.deletedRelationships()) {
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                if (!node.hasLabel(Label.label("Meta"))) {
                    this.nodesForTimeStamping.add(node.getId());
                    System.out.println("relationship deleted for: " + node);
                }
            }
        }

        return nodesForTimeStamping;

    }

    public boolean shouldUpdateMetaNode() {
        //if time stamping, means a significant change happened during transaction that wasn't the addition of a time stamp or alteration of the meta node itself
        return (this.nodesForTimeStamping.size() > 0);
    }
}
