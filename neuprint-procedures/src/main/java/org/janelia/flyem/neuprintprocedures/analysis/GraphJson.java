package org.janelia.flyem.neuprintprocedures.analysis;

import com.google.gson.annotations.SerializedName;

public class GraphJson {

    @SerializedName("Edges")
    private String edges;
    @SerializedName("Vertices")
    private String vertices;

    public GraphJson(String edges, String vertices){
        this.edges = edges;
        this.vertices = vertices;
    }
}
