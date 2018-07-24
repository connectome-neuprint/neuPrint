package org.janelia.flyem.neuprintprocedures;

import com.google.gson.Gson;

import java.util.HashSet;
import java.util.Set;

public class RoiCounts {

    private Set<RoiSynapseCount> roiCountSet;


    public RoiCounts() {
        this.roiCountSet = new HashSet<>();
    }

    public void addRoiCount(String roiName, Long inputCount, Long outputCount) {
        this.roiCountSet.add(new RoiSynapseCount(roiName, inputCount, outputCount));
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



}
