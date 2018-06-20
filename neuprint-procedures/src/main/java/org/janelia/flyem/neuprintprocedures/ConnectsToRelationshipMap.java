package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;

public class ConnectsToRelationshipMap {

    private Map<String,ConnectsToRelationship> nodeIdToConnectsToRelationshipHashMap = new HashMap<>();

    void insertConnectsToRelationship(Node startNode, Node endNode, Long weight) {
        String stringKey = nodeIdsToStringKey(startNode, endNode);
        if (this.nodeIdToConnectsToRelationshipHashMap.get(stringKey) != null) {
            this.nodeIdToConnectsToRelationshipHashMap.get(stringKey).addWeight(weight);
        } else {
            ConnectsToRelationship newRelationship = new ConnectsToRelationship(startNode, endNode);
            newRelationship.addWeight(weight);
            this.nodeIdToConnectsToRelationshipHashMap.put(stringKey,newRelationship);
        }
    }

    public Long getWeightOfConnection(Node startNode, Node endNode) {
        String stringKey = nodeIdsToStringKey(startNode, endNode);
        if (this.nodeIdToConnectsToRelationshipHashMap.get(stringKey) != null) {
            return this.nodeIdToConnectsToRelationshipHashMap.get(stringKey).getWeight();
        } else {
            return null;
        }
    }

    ConnectsToRelationship getConnectsToRelationshipByKey(String key) {
        return nodeIdToConnectsToRelationshipHashMap.get(key);
    }

    Map<String,ConnectsToRelationship> getNodeIdToConnectsToRelationshipHashMap() {
        return this.nodeIdToConnectsToRelationshipHashMap;
    }

    private String nodeIdsToStringKey(Node startNode, Node endNode) {
        return startNode.getId() + ":" + endNode.getId();
    }

    @Override
    public String toString() {
        return this.nodeIdToConnectsToRelationshipHashMap.toString();
    }

}
