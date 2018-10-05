package org.janelia.flyem.neuprinter;

import com.google.common.base.Stopwatch;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.SynapseLocationToBodyIdMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Coordinates parsing and mapping JSON formatted connected body synapse data into an in-memory object model.
 */

public class SynapseMapper {

    private final SynapseLocationToBodyIdMap synapseLocationToBodyIdMap;
    private final HashMap<String, Set<String>> preToPostMap = new HashMap<>();

    /**
     * Class constructor.
     */
    public SynapseMapper() {
        this.synapseLocationToBodyIdMap = new SynapseLocationToBodyIdMap();
    }

    /**
     * @return map of synaptic density locations to bodyIds
     */
    public SynapseLocationToBodyIdMap getSynapseLocationToBodyIdMap() {
        return this.synapseLocationToBodyIdMap;
    }

    /**
     * @return map of presynaptic density locations to postsynaptic density locations
     */
    public HashMap<String, Set<String>> getPreToPostMap() {
        return this.preToPostMap;
    }

    @Override
    public String toString() {
        return "{ numberOfMappedLocations: " + this.synapseLocationToBodyIdMap.size() + " }";
    }

    /**
     * Loads bodies from the specified JSON file and then maps their relational data.
     *
     * @param filepath to synapse JSON file
     * @return list of loaded bodies with mapped data.
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
     *
     * @param bodyList list of BodyWithSynapses
     */
    private void mapBodies(final List<BodyWithSynapses> bodyList) {

        for (final BodyWithSynapses body : bodyList) {
            body.addSynapsesToBodyIdMapAndSetSynapseCounts("post", synapseLocationToBodyIdMap);
        }

        for (final BodyWithSynapses body : bodyList) {
            body.setConnectsTo(this.synapseLocationToBodyIdMap);
            body.addSynapsesToPreToPostMap(this.preToPostMap);
        }
    }

    private static final Logger LOG = Logger.getLogger("SynapseMapper.class");

}
