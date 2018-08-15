package org.janelia.flyem.neuprinter.model;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SynapseCountsPerRoi {

    private Map<String, SynapseCounter> synapseCountsPerRoi;

    public SynapseCountsPerRoi() {
        this.synapseCountsPerRoi = new HashMap<>();
    }

    public Map<String, SynapseCounter> getSynapseCountsPerRoi() {
        return this.synapseCountsPerRoi;
    }

    public Set<String> getSetOfRois() {
        return this.synapseCountsPerRoi.keySet();
    }

    public Set<String> getSetOfRoisWithAndWithoutDatasetLabel(String datasetLabel) {
        Set<String> roiPtSet = getSetOfRois();
        Set<String> roiPtSetWithDatasetPrefix = roiPtSet.stream()
                    .map(roi -> datasetLabel + "-" + roi)
                    .collect(Collectors.toSet());
        roiPtSetWithDatasetPrefix.addAll(roiPtSet);
        return roiPtSetWithDatasetPrefix;
    }

    public void addSynapseCountsForRoi(String roi) {
        this.synapseCountsPerRoi.put(roi, new SynapseCounter());
    }

    public void addSynapseCountsForRoi(String roi, int pre, int post) {
        this.synapseCountsPerRoi.put(roi, new SynapseCounter(pre, post));
    }

    public SynapseCounter getSynapseCountsForRoi(String roi) {
        return this.synapseCountsPerRoi.get(roi);
    }

    public void incrementPreForRoi(String roi) {
        if (!this.synapseCountsPerRoi.containsKey(roi)) {
            addSynapseCountsForRoi(roi);
        }
        this.synapseCountsPerRoi.get(roi).incrementPre();
    }

    public void incrementPostForRoi(String roi) {
        if (!this.synapseCountsPerRoi.containsKey(roi)) {
            addSynapseCountsForRoi(roi);
        }
        this.synapseCountsPerRoi.get(roi).incrementPost();
    }

    public String getAsJsonString() {
        Gson gson = new Gson();
        return gson.toJson(this.synapseCountsPerRoi);
    }

    @Override
    public String toString() {
        return this.synapseCountsPerRoi.toString();
    }
}
