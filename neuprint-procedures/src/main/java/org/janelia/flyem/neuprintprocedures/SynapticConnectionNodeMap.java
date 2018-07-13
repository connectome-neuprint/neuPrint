package org.janelia.flyem.neuprintprocedures;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.neo4j.graphdb.Node;

import java.io.*;
import java.util.*;

public class SynapticConnectionNodeMap {


    private static Map<String,SynapticConnectionNode> synapticConnectionNodeStore;
    private static Map<Long,Set<SynapticConnectionNode>> groupsOfConnectedSynapticConnectionNodes;
    private static Set<SynapticConnectionEdge> synapticConnectionEdges;
    private static final Gson gson = new Gson();
    private static List<SynapticConnectionNode> vertexList;


    public SynapticConnectionNodeMap() {
        synapticConnectionNodeStore = new HashMap<>();
        groupsOfConnectedSynapticConnectionNodes = new HashMap<>();
    }

    public SynapticConnectionNodeMap(String vertexJson) {

        synapticConnectionNodeStore = new HashMap<>();
        groupsOfConnectedSynapticConnectionNodes = new HashMap<>();

        SynapticConnectionNode[] vertexArray = gson.fromJson(vertexJson,SynapticConnectionNode[].class);
        vertexList = Arrays.asList(vertexArray);

        for (SynapticConnectionNode synapticConnectionNode : vertexList ) {

            String connectionDescription = synapticConnectionNode.getConnectionDescription();
            String[] descriptionTokens = connectionDescription.split("_");
            Long preBodyId = Long.parseLong(descriptionTokens[0]);
            Long postBodyId = Long.parseLong(descriptionTokens[2]);

            synapticConnectionNodeStore.put(connectionDescription,synapticConnectionNode);
            addSynapticConnectionNodeToConnectedGroup(preBodyId, connectionDescription);
            addSynapticConnectionNodeToConnectedGroup(postBodyId, connectionDescription);


        }

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

        List<SynapticConnectionNode> synapticConnectionNodeArray = new ArrayList<>();

        for ( String connectionDescription : synapticConnectionNodeStore.keySet() ) {
            SynapticConnectionNode synapticConnectionNode = synapticConnectionNodeStore.get(connectionDescription);
            synapticConnectionNode.setCentroidLocation();
            synapticConnectionNodeArray.add(synapticConnectionNode);
        }

        String json = gson.toJson(synapticConnectionNodeArray);

        return json;
    }

    public void writeEdgesAsJson(String datasetLabel, String roi) {


        createSynapticConnectionNodeEdges();
        System.out.println("Created synaptic connection node edges.");

        try (OutputStream outputFile = new FileOutputStream(datasetLabel + "_" + roi + "_" + "edges.json")) {
            writeEdgeJsonStream(outputFile, synapticConnectionEdges);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void writeEdgeJsonStream(OutputStream out, Set<SynapticConnectionEdge> synapticConnectionEdges) throws IOException {

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
        writer.setIndent("  ");
        writer.beginArray();
        for (SynapticConnectionEdge synapticConnectionEdge : synapticConnectionEdges) {
            gson.toJson(synapticConnectionEdge, SynapticConnectionEdge.class, writer);
        }
        writer.endArray();
        writer.close();
    }


    public void writeVerticesAsJson(String datasetLabel, String roi) {

        try (OutputStream outputFile = new FileOutputStream(datasetLabel + "_" + roi + "_" + "vertices.json")) {
            writeVertexJsonStream(outputFile, vertexList);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void writeVertexJsonStream(OutputStream out, List<SynapticConnectionNode> synapticConnectionNodes) throws IOException {

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
        writer.setIndent("  ");
        writer.beginArray();
        for (SynapticConnectionNode synapticConnectionNode : synapticConnectionNodes) {
            gson.toJson(synapticConnectionNode, SynapticConnectionNode.class, writer);
        }
        writer.endArray();
        writer.close();


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
