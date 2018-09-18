package org.janelia.flyem.neuprinter.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Map of synaptic density locations to the body ids that contain them.
 */
public class SynapseLocationToBodyIdMap {

    private final Map<String, Long> locationToBodyIdMap = new HashMap<>();

    /**
     * Class constructor.
     */
    public SynapseLocationToBodyIdMap() {
    }

    /**
     *
     * @return number of locations in map
     */
    public int size() {
        return locationToBodyIdMap.size();
    }

    /**
     *
     * @param forLocation location as string in format "x:y:z"
     * @return the bodyId containing the synaptic density at this location
     */
    public Long getBodyId(final String forLocation) {
        return locationToBodyIdMap.get(forLocation);
    }

    /**
     *
     * @return set of all locations in the map in format "x:y:z"
     */
    Set<String> getAllLocationKeys() {
        return locationToBodyIdMap.keySet();
    }

    /**
     * Adds provided SynapseLocationToBodyIdMap to this map.
     *
     * @param otherMap SynapseLocationToBodyIdMap to add to this map
     */
    public void addMap(SynapseLocationToBodyIdMap otherMap) {
        this.locationToBodyIdMap.putAll(otherMap.locationToBodyIdMap);
    }

    /**
     * @throws IllegalStateException if the specified location has already been mapped to another body.
     */
    void mapLocationToBodyId(final String location,
                             final Long bodyId)
            throws IllegalStateException {

        final Long existingBodyId = locationToBodyIdMap.put(location, bodyId);
        if (existingBodyId != null) {
            throw new IllegalStateException("bodies " + existingBodyId + " and " + bodyId +
                    " are both mapped to synapse location " + location);
        }
    }

    @Override
    public String toString() {
        return "{size: " + size() + "}";
    }

}
