package org.janelia.flyem.neuprintprocedures.analysis;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SynapticConnectionVertexMap {

    private static Map<String, SynapticConnectionVertex> synapticConnectionVertexStore;
    private static Map<Long, Set<SynapticConnectionVertex>> groupsOfConnectedSynapticConnectionVertices;
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

        SynapticConnectionVertex[] vertexArray = gson.fromJson(vertexJson, SynapticConnectionVertex[].class);
        vertexList = Arrays.asList(vertexArray);

        for (SynapticConnectionVertex synapticConnectionVertex : vertexList) {

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

        if (synapticConnectionVertexStore.get(connectionDescription) == null) {
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

        for (String connectionDescription : synapticConnectionVertexStore.keySet()) {
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

        for (String connectionDescription : synapticConnectionVertexStore.keySet()) {
            SynapticConnectionVertex synapticConnectionVertex = synapticConnectionVertexStore.get(connectionDescription);
            synapticConnectionVertex.setCentroidLocationAndSynapseCounts();
            if ((synapticConnectionVertex.getPost() + synapticConnectionVertex.getPre()) > minimumNumberOfSynapses) {
                synapticConnectionVertexArray.add(synapticConnectionVertex);
                sizeOfFilteredList += 1;
            }
        }

        String json = gson.toJson(synapticConnectionVertexArray);

        System.out.println(String.format("Created vertex json with %d nodes.", sizeOfFilteredList));

        return json;
    }

    public String getEdgesAsJsonObjects(Boolean cableDistance, final GraphDatabaseService dbService, final String datasetLabel, final Long bodyId) {

        createSynapticConnectionVertexEdges(cableDistance, dbService, datasetLabel, bodyId);
        String json = gson.toJson(synapticConnectionEdges);
        System.out.println(String.format("Created edge json with %d edges.", synapticConnectionEdges.size()));

        return json;

    }

    public String getGraphJson(String edgeJson, String vertexJson) {

        GraphJson graphJson = new GraphJson(edgeJson, vertexJson);
        String graphJsonString = gson.toJson(graphJson);

        return graphJsonString;

    }

    public int numberOfVertices() {
        return synapticConnectionVertexStore.keySet().size();
    }

    public void writeEdgesAsJson(String datasetLabel, String roi, Boolean cableDistance, final GraphDatabaseService dbService, final Long bodyId) {

        createSynapticConnectionVertexEdges(cableDistance, dbService, datasetLabel, bodyId);
        System.out.println("Created synaptic connection vertex edges.");

        try (OutputStream outputFile = new FileOutputStream(datasetLabel + "_" + roi + "_" + "edges.json")) {
            writeEdgeJsonStream(outputFile, synapticConnectionEdges);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void writeEdgeJsonStream(OutputStream out, Set<SynapticConnectionEdge> synapticConnectionEdges) throws IOException {

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
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

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        writer.setIndent("  ");
        writer.beginArray();
        for (SynapticConnectionVertex synapticConnectionVertex : synapticConnectionVertices) {
            gson.toJson(synapticConnectionVertex, SynapticConnectionVertex.class, writer);
        }
        writer.endArray();
        writer.close();

    }

    private void addSynapticConnectionVertexToConnectedGroup(Long bodyId, String connectionDescription) {

        if (groupsOfConnectedSynapticConnectionVertices.get(bodyId) == null) {
            groupsOfConnectedSynapticConnectionVertices.put(bodyId, new HashSet<>());
            groupsOfConnectedSynapticConnectionVertices.get(bodyId).add(synapticConnectionVertexStore.get(connectionDescription));
        } else {
            groupsOfConnectedSynapticConnectionVertices.get(bodyId).add(synapticConnectionVertexStore.get(connectionDescription));
        }

    }

    private void createSynapticConnectionVertexEdges(Boolean cableDistance, final GraphDatabaseService dbService, final String datasetLabel, final Long bodyId) {
        synapticConnectionEdges = new HashSet<>();

        for (Long bodyIdInGroup : groupsOfConnectedSynapticConnectionVertices.keySet()) {

            SynapticConnectionVertex[] synapticConnectionVertices = groupsOfConnectedSynapticConnectionVertices.get(bodyIdInGroup).toArray(new SynapticConnectionVertex[0]);
            //create edges for every pair of vertices
            for (int i = 0; i < synapticConnectionVertices.length; i++) {
                for (int j = i + 1; j < synapticConnectionVertices.length; j++) {
                    synapticConnectionEdges.add(new SynapticConnectionEdge(synapticConnectionVertices[i], synapticConnectionVertices[j], cableDistance, dbService, datasetLabel, bodyId));

                }
            }
        }

    }

}
