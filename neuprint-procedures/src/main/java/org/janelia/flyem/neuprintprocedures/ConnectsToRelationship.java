package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.Node;

public class ConnectsToRelationship {

    private final Node startNode;
    private final Node endNode;
    private Long weight;

    public ConnectsToRelationship (Node startNode, Node endNode) {
        this.startNode = startNode;
        this.endNode = endNode;
        this.weight = new Long(0);
    }

    public Long getWeight() {
        return this.weight;
    }

    public Node getStartNode() {
        return this.startNode;
    }

    public Node getEndNode() {
        return this.endNode;
    }

    public void addWeight(Long addedWeight) {
        this.weight += addedWeight;
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
        return "Start Node: " + this.startNode + " ,End Node: " + this.endNode + " ,weight: " + this.weight;
    }
}
