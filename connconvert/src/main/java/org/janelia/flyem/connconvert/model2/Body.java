package org.janelia.flyem.connconvert.model2;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;

import java.io.Reader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.janelia.flyem.connconvert.json.JsonUtils;

/**
 * Source attributes for a neuronal body along with methods
 * ( {@link #mapAndCountSynapseConnections} and {@link #setConnections} )
 * to collect relational data for use in a graph database.
 */
public class Body {

    @SerializedName("BodyId")
    private final String bodyId;

    @SerializedName("SynapseSet")
    private final Set<Synapse> synapseSet;

    private transient int totalNumberOfPreSynapticTerminals;
    private transient int totalNumberOfPostSynapticTerminals;

    private transient Map<Body, Integer> connectsToBodyCounts;

    public Body() {
        this.bodyId = null;
        this.synapseSet = new LinkedHashSet<>(); // maintain set order to simplify testing
    }

    public String getBodyId() {
        return bodyId;
    }

    public Set<Synapse> getSynapseSet() {
        return synapseSet;
    }

    /**
     * @return the total number of connections for all pre-synapses associated with this body.
     */
    public int getTotalNumberOfPreSynapticTerminals() {
        return totalNumberOfPreSynapticTerminals;
    }

    /**
     * @return the total number of connections for all post-synapses associated with this body.
     */
    public int getTotalNumberOfPostSynapticTerminals() {
        return totalNumberOfPostSynapticTerminals;
    }

    /**
     * @return the set of bodies to which this body has pre-synaptic connections.
     */
    public Set<Body> getConnectsToBodies() {
        return connectsToBodyCounts.keySet();
    }

    /**
     * @return the number of pre-synaptic connections this body has to the specified body.
     */
    public int getConnectsToWeight(final Body connectsToBody) {
        final Integer count = connectsToBodyCounts.get(connectsToBody);
        return (count == null) ? 0 : count;
    }

    @Override
    public String toString() {
        return bodyId;
    }

    @Override
    public boolean equals(final Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Body) {
            final Body that = (Body) o;
            isEqual = Objects.equals(this.bodyId, that.bodyId);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        //noinspection ConstantConditions
        return bodyId.hashCode();
    }

    /**
     * Loops through this body's synapses to count the number of pre and post synaptic terminals.
     * All the post-synapse locations are added to the specified map.
     *
     * @throws IllegalStateException
     *   if a post-synapse location for this body has already been mapped to another body.
     */
    public void mapAndCountSynapseConnections(final SynapseLocationToBodyMap postSynapseLocationToBodyMap)
            throws IllegalStateException {

        totalNumberOfPreSynapticTerminals = 0;
        totalNumberOfPostSynapticTerminals = 0;

        for (final Synapse synapse : synapseSet) {
            if (synapse.isPreSynapse()) {
                totalNumberOfPreSynapticTerminals += synapse.getNumberOfConnections();
            } else {
                postSynapseLocationToBodyMap.mapLocationToBody(synapse.getLocation(), this);
                totalNumberOfPostSynapticTerminals += synapse.getNumberOfConnections();
            }
        }
    }

    /**
     * Uses the specified map to calculate the number of connections
     * between this body and each of its post-synaptic "partner" bodies.
     */
    public void setConnections(final SynapseLocationToBodyMap postSynapseLocationToBodyMap) {

        connectsToBodyCounts = new HashMap<>();

        for (final Synapse synapse : synapseSet) {
            if (synapse.isPreSynapse()) {
                Integer connectionCount;
                for (final Location location : synapse.getConnections()) {
                    final Body connectedBody = postSynapseLocationToBodyMap.getBody(location);
                    connectionCount = connectsToBodyCounts.get(connectedBody);
                    connectionCount = (connectionCount == null) ? 1 : connectionCount + 1;
                    connectsToBodyCounts.put(connectedBody, connectionCount);
                }
            }
        }

    }

    public static List<Body> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, BODY_LIST_TYPE);
    }

    public static List<Body> fromJsonArray(final Reader jsonReader) {
        return JsonUtils.GSON.fromJson(jsonReader, BODY_LIST_TYPE);
    }

    private static Type BODY_LIST_TYPE = new TypeToken<List<Body>>(){}.getType();
}
