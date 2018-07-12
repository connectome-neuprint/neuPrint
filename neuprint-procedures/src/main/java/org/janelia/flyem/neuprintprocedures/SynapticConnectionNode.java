package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.Node;

import java.util.HashSet;
import java.util.Set;

public class SynapticConnectionNode {

    private String connectionDescription;
    //private transient Long[] centroidLocationSums;
    private transient Set<Location> preSynapseLocations;
    private transient Set<Location> postSynapseLocations;
    private Long[] centroidLocation;
    private int pre;
    private int post;

    public SynapticConnectionNode(String connectionDescription) {
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
        return new Location((Long) synapseNode.getProperty("x"),
                (Long) synapseNode.getProperty("y"),
                (Long) synapseNode.getProperty("z"));
    }

    public Long[] setAndGetCentroid() {
        setCentroidLocation();
        return this.centroidLocation;
    }

    public Long[] getCentroidLocation() {
        return centroidLocation;
    }

    public void setCentroidLocation() {
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
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SynapticConnectionNode) {
            final SynapticConnectionNode that = (SynapticConnectionNode) o;
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
