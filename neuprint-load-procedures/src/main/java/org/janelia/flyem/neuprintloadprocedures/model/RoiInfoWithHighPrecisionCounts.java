package org.janelia.flyem.neuprintloadprocedures.model;

import com.google.gson.Gson;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A class for counting the the number of synaptic densities per ROI as well as the number
 * of high-precision synaptic densities. Used to produce the roiInfo property on ConnectionSet
 * nodes in neo4j.
 */
public class RoiInfoWithHighPrecisionCounts {

    private Map<String, SynapseCounterWithHighPrecisionCounts> synapseCountsPerRoi = new TreeMap<>();

    /**
     * Class constructor.
     */
    public RoiInfoWithHighPrecisionCounts() {
    }

    /**
     * @return a map of ROIs to {@link SynapseCounterWithHighPrecisionCounts} instances
     */
    Map<String, SynapseCounterWithHighPrecisionCounts> getSynapseCountsPerRoi() {
        return this.synapseCountsPerRoi;
    }

    /**
     * @return the set of all ROIs in this org.janelia.flyem.neuprintloadprocedures.procedures.model.RoiInfoWithHighPrecisionCounts
     */
    public Set<String> getSetOfRois() {
        return this.synapseCountsPerRoi.keySet();
    }

    /**
     * Adds provided ROI to the RoiInfo mapped to a new {@link SynapseCounterWithHighPrecisionCounts} instance
     * initialized with pre, post, preHP, and postHP equal to 0.
     *
     * @param roi ROI name
     */
    private void addSynapseCountsForRoi(String roi) {
        this.synapseCountsPerRoi.put(roi, new SynapseCounterWithHighPrecisionCounts());
    }

    /**
     * Adds provided ROI to the RoiInfo mapped to a new {@link SynapseCounterWithHighPrecisionCounts} instance
     * initialized with pre, post, preHP, and postHP equal to provided values.
     *
     * @param roi    ROI name
     * @param pre    presynaptic density count
     * @param post   postsynaptic density count
     * @param preHP  high-precision presynaptic density count
     * @param postHP high-precision postsynaptic density count
     */
    public void addSynapseCountsForRoi(String roi, int pre, int post, int preHP, int postHP) {
        this.synapseCountsPerRoi.put(roi, new SynapseCounterWithHighPrecisionCounts(pre, post, preHP, postHP));
    }

    /**
     * @param roi ROI name
     * @return {@link SynapseCounterWithHighPrecisionCounts} for provided ROI
     */
    SynapseCounterWithHighPrecisionCounts getSynapseCountsForRoi(String roi) {
        return this.synapseCountsPerRoi.get(roi);
    }

    /**
     * Increments the presynaptic density count for the provided ROI by 1.
     *
     * @param roi ROI name
     */
    public void incrementPreForRoi(String roi) {
        if (!this.synapseCountsPerRoi.containsKey(roi)) {
            addSynapseCountsForRoi(roi);
        }
        this.synapseCountsPerRoi.get(roi).incrementPre();
    }

    /**
     * Increments the postsynaptic density count for the provided ROI by 1.
     *
     * @param roi ROI name
     */
    public void incrementPostForRoi(String roi) {
        if (!this.synapseCountsPerRoi.containsKey(roi)) {
            addSynapseCountsForRoi(roi);
        }
        this.synapseCountsPerRoi.get(roi).incrementPost();
    }

    /**
     * Increments the high-precision presynaptic density count for the provided ROI by 1.
     *
     * @param roi ROI name
     */
    public void incrementPreHPForRoi(String roi) {
        if (!this.synapseCountsPerRoi.containsKey(roi)) {
            addSynapseCountsForRoi(roi);
        }
        this.synapseCountsPerRoi.get(roi).incrementPreHP();
    }

    /**
     * Increments the high-precision postsynaptic density count for the provided ROI by 1.
     *
     * @param roi ROI name
     */
    public void incrementPostHPForRoi(String roi) {
        if (!this.synapseCountsPerRoi.containsKey(roi)) {
            addSynapseCountsForRoi(roi);
        }
        this.synapseCountsPerRoi.get(roi).incrementPostHP();
    }

    /**
     * @return JSON of org.janelia.flyem.neuprintloadprocedures.procedures.model.RoiInfoWithHighPrecisionCounts to be added as an roiInfo property
     * on a node
     */
    public String getAsJsonString() {
        Gson gson = new Gson();
        return gson.toJson(this.synapseCountsPerRoi);
    }

    @Override
    public String toString() {
        return this.synapseCountsPerRoi.toString();
    }

}
