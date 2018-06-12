package org.janelia.flyem.neuprinter.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Map of synapse locations to the body ids that contain them.
 */
public class SynapseLocationToBodyIdMap {

    private final Map<String, Long> locationToBodyIdMap;

    public SynapseLocationToBodyIdMap() {
        this.locationToBodyIdMap = new HashMap<>();
    }

    public int size() {
        return locationToBodyIdMap.size();
    }

    public Long getBodyId(final String forLocation) {
        return locationToBodyIdMap.get(forLocation);
    }

    public Set<String> getAllLocationKeys() { return locationToBodyIdMap.keySet(); }

    public void addMap(SynapseLocationToBodyIdMap otherMap) { this.locationToBodyIdMap.putAll(otherMap.locationToBodyIdMap); }

    /**
     * @throws IllegalStateException
     *   if the specified location has already been mapped to another body.
     */
    public void mapLocationToBodyId(final String location,
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
