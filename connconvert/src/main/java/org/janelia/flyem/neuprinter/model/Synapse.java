package org.janelia.flyem.neuprinter.model;

import java.lang.reflect.Type;
import java.util.List;
import java.util.ArrayList;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.json.JsonUtils;

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




    public Synapse (String type, float confidence, List<Integer> location, List<List<Integer>> connections) {
        this.type = type;
        this.confidence = confidence;
        this.location = location;
        if (type.equals("pre")) {
            this.connectsTo = connections;
        } else if (type.equals("post")) {
            this.connectsFrom = connections;
        }
    }


    @Override
    public String toString() {
        return "Synapse{" + "type= " + type +
                ", confidence= " + confidence +
                ", location list= " + locationToStringKey(location) +
                ", rois= " + rois +
                ", connectsto= " + connectsTo +
                ", connectsfrom= " + connectsFrom +
                "}";

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
        return this.location.hashCode();
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
        return this.rois;
    }

    public static List<Synapse> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, SYNAPSE_LIST_TYPE);
    }

    private static final Type SYNAPSE_LIST_TYPE = new TypeToken<List<Synapse>>(){}.getType();


}
