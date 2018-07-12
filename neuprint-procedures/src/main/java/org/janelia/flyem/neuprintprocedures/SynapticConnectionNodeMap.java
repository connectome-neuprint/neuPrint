package org.janelia.flyem.neuprintprocedures;

import com.google.gson.Gson;
import org.neo4j.graphdb.Node;

import java.util.*;

public class SynapticConnectionNodeMap {


    private static Map<String,SynapticConnectionNode> synapticConnectionNodeStore;
    private static Map<Long,Set<SynapticConnectionNode>> groupsOfConnectedSynapticConnectionNodes;
    private static Set<SynapticConnectionEdge> synapticConnectionEdges;

    public SynapticConnectionNodeMap() {
        synapticConnectionNodeStore = new HashMap<>();
        groupsOfConnectedSynapticConnectionNodes = new HashMap<>();
    }

    public void addSynapticConnection(String connectionDescription, Node preSynapseNode, Node postSynapseNode) {

        String[] descriptionTokens = connectionDescription.split("_");
        Long preBodyId = Long.parseLong(descriptionTokens[0]);
        Long postBodyId = Long.parseLong(descriptionTokens[2]);

        if ( synapticConnectionNodeStore.get(connectionDescription) == null ) {
            synapticConnectionNodeStore.put(connectionDescription, new SynapticConnectionNode(connectionDescription));
            synapticConnectionNodeStore.get(connectionDescription).addSynapticConnection(preSynapseNode, postSynapseNode);
        } else {
            synapticConnectionNodeStore.get(connectionDescription).addSynapticConnection(preSynapseNode, postSynapseNode);
        }

        addSynapticConnectionNodeToConnectedGroup(preBodyId, connectionDescription);
        addSynapticConnectionNodeToConnectedGroup(postBodyId, connectionDescription);

    }

    public String getNodesAsJsonObjects() {

        Gson gson = new Gson();
        List<SynapticConnectionNode> synapticConnectionNodeArray = new ArrayList<>();

        for ( String connectionDescription : synapticConnectionNodeStore.keySet() ) {
            SynapticConnectionNode synapticConnectionNode = synapticConnectionNodeStore.get(connectionDescription);
            synapticConnectionNode.setCentroidLocation();
            synapticConnectionNodeArray.add(synapticConnectionNode);
        }

        String json = gson.toJson(synapticConnectionNodeArray);

        return json;
    }

    public String getEdgesAsJsonObjects() {

        Gson gson = new Gson();
        createSynapticConnectionNodeEdges();

        String json = gson.toJson(synapticConnectionEdges);

        return json;

    }

    private void addSynapticConnectionNodeToConnectedGroup(Long bodyId, String connectionDescription) {

        if ( groupsOfConnectedSynapticConnectionNodes.get(bodyId) == null ) {
            groupsOfConnectedSynapticConnectionNodes.put(bodyId,new HashSet<>());
            groupsOfConnectedSynapticConnectionNodes.get(bodyId).add(synapticConnectionNodeStore.get(connectionDescription));
        } else {
            groupsOfConnectedSynapticConnectionNodes.get(bodyId).add(synapticConnectionNodeStore.get(connectionDescription));
        }

    }


    private void createSynapticConnectionNodeEdges() {
        synapticConnectionEdges =  new HashSet<>();

        for (Long bodyId : groupsOfConnectedSynapticConnectionNodes.keySet()) {

            SynapticConnectionNode[] synapticConnectionNodes = groupsOfConnectedSynapticConnectionNodes.get(bodyId).toArray(new SynapticConnectionNode[0]);

            for (int i = 0 ; i < synapticConnectionNodes.length ; i++) {

                for (int j = i+1 ; j < synapticConnectionNodes.length ; j++) {

                    synapticConnectionEdges.add(new SynapticConnectionEdge(synapticConnectionNodes[i], synapticConnectionNodes[j]));

                }
            }
        }



    }







}
