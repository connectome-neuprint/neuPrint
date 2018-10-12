package org.janelia.flyem.neuprinter.model;

import java.util.HashSet;
import java.util.Set;

/**
 * A class representing a ConnectionSet node, which contains the Synapse nodes connecting a
 * presynaptic body to a postsynaptic body. Each ConnectionSet has a presynaptic bodyId, a
 * postsynaptic bodyId, and a set of synapse locations (represented by string keys in the
 * format "x:y:z").
 */
public class ConnectionSet {

    private long presynapticBodyId;
    private long postsynapticBodyId;
    private Set<String> connectingSynapseLocationStrings = new HashSet<>();

    /**
     * Class constructor.
     *
     * @param presynapticBodyId  bodyId of presynaptic neuron
     * @param postsynapticBodyId bodyId of postsynaptic neuron
     */
    public ConnectionSet(long presynapticBodyId, long postsynapticBodyId) {
        this.presynapticBodyId = presynapticBodyId;
        this.postsynapticBodyId = postsynapticBodyId;
    }

    /**
     * @return presynaptic bodyId
     */
    public long getPresynapticBodyId() {
        return this.presynapticBodyId;
    }

    /**
     * @return postsynaptic bodyId
     */
    public long getPostsynapticBodyId() {
        return this.postsynapticBodyId;
    }

    /**
     * @return set of synapse location strings ("x:y:z")
     */
    public Set<String> getConnectingSynapseLocationStrings() {
        return this.connectingSynapseLocationStrings;
    }

    /**
     * Add a presynaptic and postsynaptic location to this ConnectionSet.
     *
     * @param presynapticLocation  presynaptic location string ("x:y:z")
     * @param postsynapticLocation postsynaptic location string ("x:y:z")
     */
    public void addPreAndPostsynapticLocations(String presynapticLocation, String postsynapticLocation) {
        this.connectingSynapseLocationStrings.add(presynapticLocation);
        this.connectingSynapseLocationStrings.add(postsynapticLocation);
    }

    /**
     * Returns true if this ConnectionSet contains the synaptic density at
     * the specified location.
     *
     * @param synapticLocation synaptic location string ("x:y:z")
     * @return true if synaptic location is in ConnectionSet
     */
    public boolean contains(String synapticLocation) {
        return this.connectingSynapseLocationStrings.contains(synapticLocation);
    }

    /**
     * @return number of synaptic densities in this ConnectionSet
     */
    public int size() {
        return this.connectingSynapseLocationStrings.size();
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof ConnectionSet) {
            final ConnectionSet that = (ConnectionSet) o;
            isEqual = this.presynapticBodyId == that.presynapticBodyId && this.postsynapticBodyId == that.postsynapticBodyId;
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (int) presynapticBodyId;
        result = 31 * result + (int) postsynapticBodyId;
        return result;
    }
}
