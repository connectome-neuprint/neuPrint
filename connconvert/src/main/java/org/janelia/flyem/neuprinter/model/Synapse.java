package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.json.JsonUtils;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Synapse {

    @SerializedName("Type")
    private String type;

    @SerializedName("Location")
    private List<Integer> location;

    @SerializedName("Confidence")
    private float confidence;

    @SerializedName("rois")
    public List<String> rois;

    @SerializedName("ConnectsTo")
    private List<List<Integer>> connectsTo;

    @SerializedName("ConnectsFrom")
    private List<List<Integer>> connectsFrom;

    public Synapse(String type, float confidence, List<Integer> location, List<List<Integer>> connections) {
        this.type = type;
        this.confidence = confidence;
        this.location = location;
        if (type.equals("pre")) {
            this.connectsTo = connections;
        } else if (type.equals("post")) {
            this.connectsFrom = connections;
        }
    }

    public Synapse(String type, Integer x, Integer y, Integer z, List<String> roiList) {
        this.type = type;
        this.confidence = 0.0F;
        List<Integer> location = new ArrayList<>();
        location.add(x);
        location.add(y);
        location.add(z);
        this.location = location;
        this.rois = roiList;
    }

    @Override
    public String toString() {
        return "Synapse { " + "type=" + type +
                ", confidence=" + confidence +
                ", location=" + locationToStringKey(location) +
                ", rois=" + rois +
                ", connectsto=" + connectsTo +
                ", connectsfrom=" + connectsFrom +
                " }";

    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Synapse) {
            final Synapse that = (Synapse) o;
            isEqual = this.location.equals(that.location);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + location.hashCode();
        return result;
    }

    public List<String> locationListToStringKeys(List<List<Integer>> locationList) {
        List<String> locationStringList = new ArrayList<>();
        for (List<Integer> location : locationList) {
            locationStringList.add(locationToStringKey(location));

        }
        return locationStringList;
    }

    public String locationToStringKey(List<Integer> location) {
        return location.get(0) + ":" + location.get(1) + ":" + location.get(2);
    }

    public String getLocationString() {
        return locationToStringKey(this.location);
    }

    public List<Integer> getLocation() {
        return this.location;
    }

    public Integer getX() {
        return this.location.get(0);
    }

    public Integer getY() {
        return this.location.get(1);
    }

    public Integer getZ() {
        return this.location.get(2);
    }

    public Point getLocationAsPoint() {
        return Values.point(9157, this.location.get(0), this.location.get(1), this.location.get(2)).asPoint();
    }

    public static Point convertLocationStringToPoint(String locationString) {
        String[] locationArray = locationString.split(":");
        return Values.point(9157, Double.parseDouble(locationArray[0]), Double.parseDouble(locationArray[1]), Double.parseDouble(locationArray[2])).asPoint();
    }

    public float getConfidence() {
        return this.confidence;
    }

    public List<String> getConnectionLocationStrings() {
        List<String> connections = new ArrayList<>();
        switch (this.type) {
            case ("post"):
                connections = locationListToStringKeys(this.connectsFrom);
                break;
            case ("pre"):
                connections = locationListToStringKeys(this.connectsTo);
                break;
            default:
                connections.add("Type not listed.");
                break;
        }
        return connections;

    }

    public List<List<Integer>> getConnectionLocations() {
        List<List<Integer>> connections = new ArrayList<>();
        switch (this.type) {
            case ("post"):
                connections = this.connectsFrom;
                break;
            case ("pre"):
                connections = this.connectsTo;
                break;
            default:
                List<Integer> defaultLoc = new ArrayList<Integer>() {{
                    add(-1);
                    add(-1);
                    add(-1);
                }};

                connections.add(defaultLoc);
                break;
        }
        return connections;

    }

    public String getType() {
        return this.type;
    }

    public List<String> getRois() {
        // remove -lm tag on rois
        if (this.rois != null) {
            List<String> newRoiList = new ArrayList<>();
            for (String roi : rois) {
                if (roi.endsWith("-lm")) {
                    newRoiList.add(roi.replace("-lm", ""));
                } else {
                    newRoiList.add(roi);
                }
            }
            return newRoiList;
        } else {
            return this.rois;
        }
    }

    public List<String> getRoiPts() {
        // remove -lm tag on rois
        if (this.rois != null) {
            List<String> newRoiList = new ArrayList<>();
            for (String roi : rois) {
                if (roi.endsWith("-lm")) {
                    newRoiList.add(roi.replace("-lm", "") + "-pt");
                } else {
                    newRoiList.add(roi + "-pt");
                }
            }
            return newRoiList;
        } else {
            return this.rois;
        }
    }

    public List<String> getRoiPtsWithAndWithoutDatasetPrefix(String dataset) {
        List<String> roiPts = getRoiPts();
        roiPts.addAll(roiPts.stream().map(r -> dataset + "-" + r).collect(Collectors.toList()));
        return roiPts;
    }

    public List<String> getRoisWithAndWithoutDatasetPrefix(String dataset) {
        List<String> roiList = getRois();
        roiList.addAll(roiList.stream().map(r -> dataset + "-" + r).collect(Collectors.toList()));
        return roiList;
    }

    public void addRoiList(List<String> rois) {
        this.rois = rois;
    }

    public static List<Synapse> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, SYNAPSE_LIST_TYPE);
    }

    private static final Type SYNAPSE_LIST_TYPE = new TypeToken<List<Synapse>>() {
    }.getType();

}
