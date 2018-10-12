package org.janelia.flyem.neuprintprocedures.proofreading;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.HashSet;
import java.util.Set;

public class ConnectsToRelationship {

    private final Node startNode;
    private final Node endNode;
    private Set<Node> synapsesInConnectionSet = new HashSet<>();

    ConnectsToRelationship(Node startNode, Node endNode) {
        this.startNode = startNode;
        this.endNode = endNode;
    }

    long getWeight() {
        long weight = 0L;
        for (Node synapseNode : this.synapsesInConnectionSet)
            if (synapseNode.hasLabel(Label.label("PostSyn"))) weight++;
        return weight;
    }

    Node getStartNode() {
        return this.startNode;
    }

    Node getEndNode() {
        return this.endNode;
    }

    public Set<Node> getSynapsesInConnectionSet() {
        return this.synapsesInConnectionSet;
    }

    void addPreAndPostSynapseNodesToConnectionSet(Node preSynapseNode, Node postSynapseNode) {
        this.synapsesInConnectionSet.add(preSynapseNode);
        this.synapsesInConnectionSet.add(postSynapseNode);
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof ConnectsToRelationship) {
            final ConnectsToRelationship that = (ConnectsToRelationship) o;
            isEqual = this.startNode.equals(that.startNode) && this.endNode.equals(that.endNode);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + startNode.hashCode();
        result = 31 * result + endNode.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Start Node: " + this.startNode + " ,End Node: " + this.endNode + " ,weight: " + this.getWeight();
    }
}
