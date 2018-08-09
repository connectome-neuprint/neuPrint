package org.janelia.flyem.neuprinter.model;

import java.util.HashMap;
import java.util.Map;

public class AutoName {

    private static Map<String,Integer> AUTO_NAME_ID_COUNT = new HashMap<>();
    private String maxInputRoi;
    private String maxOutputRoi;
    private int id;
    private long bodyId;

    public AutoName(String maxInputRoi, String maxOutputRoi, long bodyId) {
        this.maxInputRoi = maxInputRoi;
        this.maxOutputRoi = maxOutputRoi;
        this.bodyId = bodyId;
        String nameKey = maxInputRoi + ":" + maxInputRoi;
        if (AUTO_NAME_ID_COUNT.get(nameKey)==null) {
            AUTO_NAME_ID_COUNT.put(nameKey, 0);
            this.id = 0;
        } else {
            Integer currentCount = AUTO_NAME_ID_COUNT.get(nameKey);
            currentCount++;
            this.id = currentCount;
            AUTO_NAME_ID_COUNT.put(nameKey, currentCount);
        }
    }

    public String getAutoName() {
        return this.maxInputRoi.toUpperCase() + "-" + this.maxOutputRoi.toUpperCase() + "-" + id;
    }

    public long getBodyId() {
        return bodyId;
    }

    @Override
    public String toString() {
        return "bodyId: " + this.bodyId + " " + getAutoName();
    }
}
