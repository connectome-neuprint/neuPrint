package connconvert;

import java.util.List;
import java.util.ArrayList;

public class Synapse {
    public String type;
    public float confidence;
    public List<Integer> location;
    public List<List<Integer>> connectsTo;
    public List<List<Integer>> connectsFrom;
    public List<String> rois;


    public Synapse () {

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



}
