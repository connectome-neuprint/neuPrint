package org.janelia.flyem.neuprintprocedures.analysis;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RoiCounts {

    private Set<RoiSynapseCount> roiCountSet;
    private Map<String,RoiSynapseCount> roiSynapseCountMap;


    public RoiCounts() {
        this.roiCountSet = new HashSet<>();
        this.roiSynapseCountMap = new HashMap<>();
    }

    public void addRoiCount(String roiName, Long inputCount, Long outputCount) {
        RoiSynapseCount roiSynapseCount = new RoiSynapseCount(roiName, inputCount, outputCount);
        this.roiCountSet.add(roiSynapseCount);
        this.roiSynapseCountMap.put(roiName,roiSynapseCount);
    }

    @Override
    public String toString() {
        return roiCountSet.toString();
    }

    public String roiCountsToJson() {
        final Gson gson = new Gson();
        String json = gson.toJson(this.roiCountSet);
        return json;
    }

    public RoiSynapseCount getRoiSynapseCount(String roi) {
        return this.roiSynapseCountMap.get(roi);
    }

}
