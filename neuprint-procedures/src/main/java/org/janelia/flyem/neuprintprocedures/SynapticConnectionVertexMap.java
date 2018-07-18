package org.janelia.flyem.neuprintprocedures;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.neo4j.graphdb.Node;

import java.io.*;
import java.util.*;

public class SynapticConnectionVertexMap {


    private static Map<String,SynapticConnectionVertex> synapticConnectionVertexStore;
    private static Map<Long,Set<SynapticConnectionVertex>> groupsOfConnectedSynapticConnectionVertices;
    private static Set<SynapticConnectionEdge> synapticConnectionEdges;
    private static final Gson gson = new Gson();
    private static List<SynapticConnectionVertex> vertexList;


    public SynapticConnectionVertexMap() {
        synapticConnectionVertexStore = new HashMap<>();
        groupsOfConnectedSynapticConnectionVertices = new HashMap<>();
    }

    public SynapticConnectionVertexMap(String vertexJson) {

        synapticConnectionVertexStore = new HashMap<>();
        groupsOfConnectedSynapticConnectionVertices = new HashMap<>();

        SynapticConnectionVertex[] vertexArray = gson.fromJson(vertexJson,SynapticConnectionVertex[].class);
        vertexList = Arrays.asList(vertexArray);

        for (SynapticConnectionVertex synapticConnectionVertex : vertexList ) {

            String connectionDescription = synapticConnectionVertex.getConnectionDescription();
            String[] descriptionTokens = connectionDescription.split("_");
            Long preBodyId = Long.parseLong(descriptionTokens[0]);
            Long postBodyId = Long.parseLong(descriptionTokens[2]);

            synapticConnectionVertexStore.put(connectionDescription, synapticConnectionVertex);
            addSynapticConnectionVertexToConnectedGroup(preBodyId, connectionDescription);
            addSynapticConnectionVertexToConnectedGroup(postBodyId, connectionDescription);


        }

    }

    public void addSynapticConnection(String connectionDescription, Node preSynapseNode, Node postSynapseNode) {

        String[] descriptionTokens = connectionDescription.split("_");
        Long preBodyId = Long.parseLong(descriptionTokens[0]);
        Long postBodyId = Long.parseLong(descriptionTokens[2]);

        if ( synapticConnectionVertexStore.get(connectionDescription) == null ) {
            synapticConnectionVertexStore.put(connectionDescription, new SynapticConnectionVertex(connectionDescription));
            synapticConnectionVertexStore.get(connectionDescription).addSynapticConnection(preSynapseNode, postSynapseNode);
        } else {
            synapticConnectionVertexStore.get(connectionDescription).addSynapticConnection(preSynapseNode, postSynapseNode);
        }

        addSynapticConnectionVertexToConnectedGroup(preBodyId, connectionDescription);
        addSynapticConnectionVertexToConnectedGroup(postBodyId, connectionDescription);

    }

    public String getVerticesAsJsonObjects() {

        List<SynapticConnectionVertex> synapticConnectionVertexArray = new ArrayList<>();

        for ( String connectionDescription : synapticConnectionVertexStore.keySet() ) {
            SynapticConnectionVertex synapticConnectionVertex = synapticConnectionVertexStore.get(connectionDescription);
            synapticConnectionVertex.setCentroidLocationAndSynapseCounts();
            synapticConnectionVertexArray.add(synapticConnectionVertex);
        }

        String json = gson.toJson(synapticConnectionVertexArray);

        System.out.println("Created vertex json with " + synapticConnectionVertexArray.size() + " nodes.");


        return json;
    }

    public String getVerticesAboveThresholdAsJsonObjects(Long minimumNumberOfSynapses) {

        List<SynapticConnectionVertex> synapticConnectionVertexArray = new ArrayList<>();
        int sizeOfFilteredList = 0;

        for ( String connectionDescription : synapticConnectionVertexStore.keySet() ) {
            SynapticConnectionVertex synapticConnectionVertex = synapticConnectionVertexStore.get(connectionDescription);
            synapticConnectionVertex.setCentroidLocationAndSynapseCounts();
            if ( (synapticConnectionVertex.getPost()+synapticConnectionVertex.getPre()) > minimumNumberOfSynapses ) {
                synapticConnectionVertexArray.add(synapticConnectionVertex);
                sizeOfFilteredList += 1;
            }
        }

        String json = gson.toJson(synapticConnectionVertexArray);

        System.out.println("Created vertex json with " + sizeOfFilteredList + " nodes.");


        return json;
    }

    public String getEdgesAsJsonObjects() {

        createSynapticConnectionVertexEdges();
        String json = gson.toJson(synapticConnectionEdges);
        System.out.println("Created edge json with " + synapticConnectionEdges.size() + " edges.");

        return json;

    }

    public int numberOfVertices() {
        return synapticConnectionVertexStore.keySet().size();
    }

    public void writeEdgesAsJson(String datasetLabel, String roi) {


        createSynapticConnectionVertexEdges();
        System.out.println("Created synaptic connection vertex edges.");

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

    private void writeVertexJsonStream(OutputStream out, List<SynapticConnectionVertex> synapticConnectionVertices) throws IOException {

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
        writer.setIndent("  ");
        writer.beginArray();
        for (SynapticConnectionVertex synapticConnectionVertex : synapticConnectionVertices) {
            gson.toJson(synapticConnectionVertex, SynapticConnectionVertex.class, writer);
        }
        writer.endArray();
        writer.close();


    }

    private void addSynapticConnectionVertexToConnectedGroup(Long bodyId, String connectionDescription) {

        if ( groupsOfConnectedSynapticConnectionVertices.get(bodyId) == null ) {
            groupsOfConnectedSynapticConnectionVertices.put(bodyId,new HashSet<>());
            groupsOfConnectedSynapticConnectionVertices.get(bodyId).add(synapticConnectionVertexStore.get(connectionDescription));
        } else {
            groupsOfConnectedSynapticConnectionVertices.get(bodyId).add(synapticConnectionVertexStore.get(connectionDescription));
        }

    }


    private void createSynapticConnectionVertexEdges() {
        synapticConnectionEdges =  new HashSet<>();

        for (Long bodyId : groupsOfConnectedSynapticConnectionVertices.keySet()) {

            SynapticConnectionVertex[] synapticConnectionVertices = groupsOfConnectedSynapticConnectionVertices.get(bodyId).toArray(new SynapticConnectionVertex[0]);

            for (int i = 0; i < synapticConnectionVertices.length ; i++) {

                for (int j = i+1; j < synapticConnectionVertices.length ; j++) {

                    synapticConnectionEdges.add(new SynapticConnectionEdge(synapticConnectionVertices[i], synapticConnectionVertices[j]));

                }
            }
        }



    }







}