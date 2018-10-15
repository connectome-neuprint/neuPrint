package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.util.HashSet;
import java.util.Set;

class TransactionDataHandler {

    // nodes with unique ids
    private static final String DATA_MODEL = "DataModel";
    private static final String NEURON = "Neuron";
    private static final String META = "Meta";
    private static final String SKELETON = "Skeleton";
    private static final String SKEL_NODE = "SkelNode";
    private static final String SYNAPSE = "Synapse";
    private static final String SYNAPSE_SET = "SynapseSet";
    // properties
    private static final String TIME_STAMP = "timeStamp";

    private TransactionData transactionData;
    private Set<Node> nodesForTimeStamping = new HashSet<>();
    private Set<String> datasetsChanged = new HashSet<>();
    private boolean shouldMetaNodeSynapseCountsBeUpdated;

    TransactionDataHandler(TransactionData transactionData) {
        this.transactionData = transactionData;
    }

    Set<Node> getNodesForTimeStamping(Set<String> existingDatasets) {

        shouldMetaNodeSynapseCountsBeUpdated = false;

        for (Node node : transactionData.createdNodes()) {
            addNodeForTimeStamping(node, existingDatasets);
            // synapse counts updated if new synapses are created
            checkIfShouldUpdateMetaNodeSynapseCounts(node);
        }

        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            addNodeForTimeStamping(labelEntry.node(), existingDatasets);
        }

        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            addNodeForTimeStamping(labelEntry.node(), existingDatasets);
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.assignedNodeProperties()) {
            if (!propertyEntry.key().equals(TIME_STAMP)) {
                addNodeForTimeStamping(propertyEntry.entity(), existingDatasets);
            }
            // synapse counts updated if new properties are added to a synapse (indicating an roi has been added; should we anticipate other changes?)
            checkIfShouldUpdateMetaNodeSynapseCounts(propertyEntry.entity());
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.removedNodeProperties()) {
            if (!propertyEntry.key().equals(TIME_STAMP)) {
                addNodeForTimeStamping(propertyEntry.entity(), existingDatasets);
            }
            // synapse counts updated if new properties are removed from a synapse (indicating an roi has been removed; should we anticipate other changes?)
            checkIfShouldUpdateMetaNodeSynapseCounts(propertyEntry.entity());
        }

        for (PropertyEntry<Relationship> propertyEntry : transactionData.assignedRelationshipProperties()) {
            Relationship relationship = propertyEntry.entity();
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                addNodeForTimeStamping(node, existingDatasets);
            }
        }

        for (PropertyEntry<Relationship> propertyEntry : transactionData.removedRelationshipProperties()) {
            Relationship relationship = propertyEntry.entity();
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                addNodeForTimeStamping(node, existingDatasets);
            }
        }

        for (Relationship relationship : transactionData.createdRelationships()) {
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                addNodeForTimeStamping(node, existingDatasets);
            }
        }

        for (Relationship relationship : transactionData.deletedRelationships()) {
            Node[] nodes = relationship.getNodes();
            for (Node node : nodes) {
                addNodeForTimeStamping(node, existingDatasets);
            }
        }

        return nodesForTimeStamping;

    }

    public Set<String> getDatasetsChanged() {
        return this.datasetsChanged;
    }

    private void addNodeForTimeStamping(Node node, Set<String> existingDatasets) {
        if (!node.hasLabel(Label.label(META)) && !transactionData.isDeleted(node)) {
            this.nodesForTimeStamping.add(node);
            for (String dataset : existingDatasets) {
                if (node.hasLabel(Label.label(dataset))) {
                    this.datasetsChanged.add(dataset);
                }
            }
        }
    }

    private void checkIfShouldUpdateMetaNodeSynapseCounts(Node node) {
        if (node.hasLabel(Label.label(SYNAPSE)) && !transactionData.isDeleted(node)) {
            shouldMetaNodeSynapseCountsBeUpdated = true;
        }
    }

    boolean shouldTimeStampAndUpdateMetaNodeTimeStamp() {
        //if time stamping, means a significant change happened during transaction that wasn't the addition of a time stamp or alteration of the meta node itself
        return (this.nodesForTimeStamping.size() > 0);
    }

    boolean getShouldMetaNodeSynapseCountsBeUpdated() {
        return this.shouldMetaNodeSynapseCountsBeUpdated;
    }

}
