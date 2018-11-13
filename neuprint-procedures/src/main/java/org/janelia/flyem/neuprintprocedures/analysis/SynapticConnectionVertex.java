package org.janelia.flyem.neuprintprocedures.analysis;

import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprintprocedures.Location;
import org.neo4j.graphdb.Node;

import java.util.HashSet;
import java.util.Set;

public class SynapticConnectionVertex {

    @SerializedName("id")
    private String connectionDescription;
    private Set<Location> preSynapseLocations;
    private Set<Location> postSynapseLocations;
    private Long[] centroidLocation;
    private int pre;
    private int post;

    public SynapticConnectionVertex(String connectionDescription) {
        this.connectionDescription = connectionDescription;
        this.preSynapseLocations = new HashSet<>();
        this.postSynapseLocations = new HashSet<>();
        this.centroidLocation = new Long[3];
        this.pre = 0;
        this.post = 0;
    }

    public void addSynapticConnection(Node preSynapseNode, Node postSynapseNode) {
        Location preSynapseLocation = extractSynapseLocation(preSynapseNode);
        Location postSynapseLocation = extractSynapseLocation(postSynapseNode);
        this.preSynapseLocations.add(preSynapseLocation);
        this.postSynapseLocations.add(postSynapseLocation);
    }

    private Location extractSynapseLocation(Node synapseNode) {
        return AnalysisProcedures.getSkelOrSynapseNodeLocation(synapseNode);
    }

    public Long[] setAndGetCentroid() {
        setCentroidLocationAndSynapseCounts();
        return this.centroidLocation;
    }

    public Long[] getCentroidLocation() {
        return centroidLocation;
    }

    public void setCentroidLocationAndSynapseCounts() {
        this.pre = preSynapseLocations.size();
        this.post = postSynapseLocations.size();

        float totalSynapseCount = getPre() + getPost();

        Set<Location> locationSetUnion = new HashSet<>();
        locationSetUnion.addAll(preSynapseLocations);
        locationSetUnion.addAll(postSynapseLocations);

        Long[] summedLocation = locationSetUnion.stream().reduce(new Location(0L,0L,0L), Location::getSummedLocations).getLocation();

        this.centroidLocation[0] = (long) Math.round(summedLocation[0]/totalSynapseCount);
        this.centroidLocation[1] = (long) Math.round(summedLocation[1]/totalSynapseCount);
        this.centroidLocation[2] = (long) Math.round(summedLocation[2]/totalSynapseCount);
    }

    public String getConnectionDescription() {
        return connectionDescription;
    }

    public Integer getPre() {
        return pre;
    }

    public Integer getPost() {
        return post;
    }

    @Override
    public String toString() {
        return "Synaptic Connection Vertex : " + this.connectionDescription;
    }


    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SynapticConnectionVertex) {
            final SynapticConnectionVertex that = (SynapticConnectionVertex) o;
            isEqual = this.connectionDescription.equals(that.connectionDescription);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.connectionDescription.hashCode();
        return result;
    }
}
