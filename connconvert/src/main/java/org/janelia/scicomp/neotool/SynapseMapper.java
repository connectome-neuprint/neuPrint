package org.janelia.scicomp.neotool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

import org.janelia.scicomp.neotool.model.Body;
import org.janelia.scicomp.neotool.model.SynapseLocationToBodyMap;

/**
 * Coordinates parsing and mapping JSON formatted connected body synapse data into an in-memory object model.
 */
public class SynapseMapper {

    private final SynapseLocationToBodyMap postSynapseLocationToBodyMap;

    public SynapseMapper() {
        this.postSynapseLocationToBodyMap = new SynapseLocationToBodyMap();
    }

    @Override
    public String toString() {
        return "{ numberOfMappedLocations: " + postSynapseLocationToBodyMap.size() + " }";
    }

    /**
     * Loads bodies from the specified JSON file and then maps their relational data.
     *
     * @return list of loaded bodies with mapped data.
     *
     * @throws FileNotFoundException
     *   if the specified JSON file does not exist.
     */
    public List<Body> loadAndMapBodies(final File bodyJsonFile)
            throws FileNotFoundException {

        final FileReader bodyListReader = new FileReader(bodyJsonFile);
        final List<Body> bodyList = Body.fromJsonArray(bodyListReader);

        mapBodies(bodyList);

        return bodyList;
    }

    /**
     * Maps relational data for all bodies in the specified list.
     * This method has been extracted as an independent method to facilitate testing.
     */
    public void mapBodies(final List<Body> bodyList) {

        for (final Body body : bodyList) {
            body.mapAndCountSynapseConnections(postSynapseLocationToBodyMap);
        }

        for (final Body body : bodyList) {
            body.setConnections(postSynapseLocationToBodyMap);
        }

    }

}
