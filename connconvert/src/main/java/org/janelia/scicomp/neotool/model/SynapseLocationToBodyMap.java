package org.janelia.scicomp.neotool.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Map of synapse locations to the bodies that encompass them.
 */
public class SynapseLocationToBodyMap {

    private final Map<Location, Body> locationToBodyMap;

    public SynapseLocationToBodyMap() {
        this.locationToBodyMap = new HashMap<>();
    }

    public int size() {
        return locationToBodyMap.size();
    }

    public Body getBody(final Location forLocation) {
        return locationToBodyMap.get(forLocation);
    }

    /**
     * @throws IllegalStateException
     *   if the specified location has already been mapped to another body.
     */
    public void mapLocationToBody(final Location location,
                                  final Body body)
            throws IllegalStateException {

        final Body existingBody = locationToBodyMap.put(location, body);
        if (existingBody != null) {
            throw new IllegalStateException("bodies " + existingBody + " and " + body +
                                            " are both mapped to synapse location " + location);
        }
    }

    @Override
    public String toString() {
        return "{size: " + size() + "}";
    }

}
