package org.janelia.flyem.neuprintprocedures.proofreading;

import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConnectsToRelationshipMap {

    private Map<String,ConnectsToRelationship> nodeIdToConnectsToRelationshipHashMap = new HashMap<>();

    void insertSynapsesIntoConnectsToRelationship(Node startNode, Node endNode, Node preSynapseNode, Node postSynapseNode) {
        String stringKey = nodeIdsToStringKey(startNode, endNode);
        if (this.nodeIdToConnectsToRelationshipHashMap.get(stringKey) != null) {
            this.nodeIdToConnectsToRelationshipHashMap.get(stringKey).addPreAndPostSynapseNodesToConnectionSet(preSynapseNode, postSynapseNode);
        } else {
            ConnectsToRelationship newRelationship = new ConnectsToRelationship(startNode, endNode);
            newRelationship.addPreAndPostSynapseNodesToConnectionSet(preSynapseNode, postSynapseNode);
            this.nodeIdToConnectsToRelationshipHashMap.put(stringKey,newRelationship);
        }
    }

    Set<String> getSetOfConnectionKeys() {
        return this.nodeIdToConnectsToRelationshipHashMap.keySet();
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
        return this.nodeIdToConnectsToRelationshipHashMap.get(key);
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
