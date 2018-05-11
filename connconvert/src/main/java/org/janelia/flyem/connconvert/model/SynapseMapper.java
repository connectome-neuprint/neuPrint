package org.janelia.flyem.connconvert.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Coordinates parsing and mapping JSON formatted connected body synapse data into an in-memory object model.
 */

public class SynapseMapper {

    private final SynapseLocationToBodyIdMap synapseLocationToBodyIdMap;

    public SynapseMapper() {
        this.synapseLocationToBodyIdMap = new SynapseLocationToBodyIdMap();
    }

    public SynapseLocationToBodyIdMap getSynapseLocationToBodyIdMap() {
        return synapseLocationToBodyIdMap;
    }

    @Override
    public String toString() {
        return "{ numberOfMappedLocations: " + synapseLocationToBodyIdMap.size() + " }";
    }

    /**
     * Loads bodies from the specified JSON file and then maps their relational data.
     *
     * @return list of loaded bodies with mapped data.
     *
     */
    public List<BodyWithSynapses> loadAndMapBodies(final String filepath) {



        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            final List<BodyWithSynapses> bodyList = BodyWithSynapses.fromJson(reader);
            mapBodies(bodyList);
            return bodyList;

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return new ArrayList<>();
        }






    }


    public List<BodyWithSynapses> loadAndMapBodiesFromJsonString(final String jsonString) {

            final List<BodyWithSynapses> bodyList = BodyWithSynapses.fromJson(jsonString);
            mapBodies(bodyList);
            return bodyList;

    }



    /**
     * Maps relational data for all bodies in the specified list.
     * This method has been extracted as an independent method to facilitate testing.
     */
    public void mapBodies(final List<BodyWithSynapses> bodyList) {


        for (final BodyWithSynapses body : bodyList) {
            body.addSynapseToBodyIdMapAndSetSynapseCounts("post", synapseLocationToBodyIdMap);
        }

        for (final BodyWithSynapses body : bodyList) {
            body.setConnectsTo(synapseLocationToBodyIdMap);
        }

    }



}
