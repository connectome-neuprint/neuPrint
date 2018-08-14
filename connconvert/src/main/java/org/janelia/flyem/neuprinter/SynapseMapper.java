package org.janelia.flyem.neuprinter;

import com.google.common.base.Stopwatch;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.SynapseLocationToBodyIdMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/**
 * Coordinates parsing and mapping JSON formatted connected body synapse data into an in-memory object model.
 */

public class SynapseMapper {

    private final SynapseLocationToBodyIdMap synapseLocationToBodyIdMap;
    private final HashMap<String,List<String>> preToPostMap = new HashMap<>();

    public SynapseMapper() {
        this.synapseLocationToBodyIdMap = new SynapseLocationToBodyIdMap();
    }

    public SynapseLocationToBodyIdMap getSynapseLocationToBodyIdMap() {
        return synapseLocationToBodyIdMap;
    }

    public HashMap<String, List<String>> getPreToPostMap() {
        return preToPostMap;
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

        Stopwatch timer = Stopwatch.createStarted();

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            final List<BodyWithSynapses> bodyList = BodyWithSynapses.fromJson(reader);
            mapBodies(bodyList);
            timer.reset();
            return bodyList;

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return new ArrayList<>();
        }







    }




    /**
     * Maps relational data for all bodies in the specified list.
     * This method has been extracted as an independent method to facilitate testing.
     */
    public void mapBodies(final List<BodyWithSynapses> bodyList) {


        for (final BodyWithSynapses body : bodyList) {
            body.addSynapsesToBodyIdMapAndSetSynapseCounts("post", synapseLocationToBodyIdMap);
        }

        for (final BodyWithSynapses body : bodyList) {
            body.setConnectsTo(synapseLocationToBodyIdMap);
            body.addSynapsesToPreToPostMap(preToPostMap);
        }


    }

    private static final Logger LOG = Logger.getLogger("SynapseMapper.class");




}
