package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.json.JsonUtils;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.janelia.flyem.neuprinter.model.Neuron.removeUnwantedRois;

/**
 * A class representing a synaptic density. A Synapse has a type (pre or post), a three-
 * dimensional location, a confidence, a set of rois, and a set of Synapse locations representing
 * its connections. Presynaptic densities have a set of locations they connect to, while postsynaptic
 * densities have a set of locations they connect from.
 */
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
    private Set<List<Integer>> connectsTo;

    @SerializedName("ConnectsFrom")
    private Set<List<Integer>> connectsFrom;

    /**
     * Class constructor used for testing.
     *
     * @param type        type of synaptic density (pre or post)
     * @param confidence  confidence of prediction
     * @param location    3D location of density
     * @param connections set of connections (connections to if pre, connections from if post)
     */
    public Synapse(String type, float confidence, List<Integer> location, Set<List<Integer>> connections) {
        this.type = type;
        this.confidence = confidence;
        this.location = location;
        if (type.equals("pre")) {
            this.connectsTo = connections;
        } else if (type.equals("post")) {
            this.connectsFrom = connections;
        }
    }

    /**
     * Class constructor used for neo4j stored procedures.
     *
     * @param type   type of synaptic density (pre or post)
     * @param x      x coordinate of location
     * @param y      y coordinate of location
     * @param z      z coordinate of location
     * @param roiSet set of rois that this density is in
     */
    public Synapse(String type, Integer x, Integer y, Integer z, List<String> roiSet) {
        this.type = type;
        this.confidence = 0.0F;
        List<Integer> location = new ArrayList<>();
        location.add(x);
        location.add(y);
        location.add(z);
        this.location = location;
        this.rois = roiSet;
    }

    /**
     * Class constructor used for quickly comparing synapses in neo4j stored procedures.
     *
     * @param x x coordinate of location
     * @param y y coordinate of location
     * @param z z coordinate of location
     */
    public Synapse(Integer x, Integer y, Integer z) {
        List<Integer> location = new ArrayList<>();
        location.add(x);
        location.add(y);
        location.add(z);
        this.location = location;
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

    private Set<String> locationSetToStringKeys(Set<List<Integer>> locationSet) {
        Set<String> locationStringSet = new HashSet<>();
        for (List<Integer> location : locationSet) {
            locationStringSet.add(locationToStringKey(location));

        }
        return locationStringSet;
    }

    private String locationToStringKey(List<Integer> location) {
        return location.get(0) + ":" + location.get(1) + ":" + location.get(2);
    }

    /**
     * @return the location represented as a string in format "x:y:z"
     */
    public String getLocationString() {
        return locationToStringKey(this.location);
    }

    /**
     * @return list of integers representing synaptic density's 3D location
     */
    public List<Integer> getLocation() {
        return this.location;
    }

    /**
     * @return x coordinate of location
     */
    public Integer getX() {
        return this.location.get(0);
    }

    /**
     * @return y coordinate of location
     */
    public Integer getY() {
        return this.location.get(1);
    }

    /**
     * @return z coordinate of location
     */
    public Integer getZ() {
        return this.location.get(2);
    }

    /**
     * Returns location of synaptic density as neo4j {@link Point}.
     *
     * @return {@link Point}
     */
    public Point getLocationAsPoint() {
        return Values.point(9157, this.location.get(0), this.location.get(1), this.location.get(2)).asPoint();
    }

    /**
     * Returns the provided location string ("x:y:z") as a neo4j {@link Point}.
     *
     * @param locationString "x:y:z"
     * @return {@link Point}
     */
    public static Point convertLocationStringToPoint(String locationString) {
        String[] locationArray = locationString.split(":");
        return Values.point(9157, Double.parseDouble(locationArray[0]), Double.parseDouble(locationArray[1]), Double.parseDouble(locationArray[2])).asPoint();
    }

    /**
     * @return confidence of prediction
     */
    public float getConfidence() {
        return this.confidence;
    }

    /**
     * Returns a set of locations that are connected to this synaptic density
     * as strings in the format "x:y:z". Presynaptic densities have connections to
     * postsynaptic densities, and postsynaptic densities have connections from
     * presynaptic densities.
     *
     * @return set of location strings
     */
    public Set<String> getConnectionLocationStrings() {
        Set<String> connections = new HashSet<>();
        switch (this.type) {
            case ("post"):
                connections = locationSetToStringKeys(this.connectsFrom);
                break;
            case ("pre"):
                connections = locationSetToStringKeys(this.connectsTo);
                break;
            default:
                connections.add("Type not listed.");
                break;
        }
        return connections;

    }

    /**
     * Returns a set of locations that are connected to this synaptic density.
     * Presynaptic densities have connections to postsynaptic densities, and
     * postsynaptic densities have connections from presynaptic densities.
     *
     * @return set of locations, which are lists of integers representing 3D locations
     */
    Set<List<Integer>> getConnectionLocations() {
        Set<List<Integer>> connections = new HashSet<>();
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

    /**
     * @return type of synaptic density (pre or post)
     */
    public String getType() {
        return this.type;
    }

    /**
     * @return list of rois in which this synaptic density is located ("-lm" suffix
     * removed if present)
     */
    public List<String> getRois() {
        // remove -lm tag on rois and unwanted rois from mb6 and fib25
        return removeUnwantedRois(this.rois);
    }

    /**
     * @param dataset name of dataset in which this Synapse exists
     * @return list of rois both with and without "dataset-" prefix
     */
    public List<String> getRoisWithAndWithoutDatasetPrefix(String dataset) {
        List<String> roiList = getRois();
        roiList.addAll(roiList.stream().map(r -> dataset + "-" + r).collect(Collectors.toList()));
        return roiList;
    }

    /**
     * Adds a provided set of rois to this Synapse instance.
     *
     * @param rois a set of rois
     */
    public void addRoiList(List<String> rois) {
        this.rois = rois;
    }

    /**
     * Returns a list of {@link Synapse} objects as read from a JSON.
     * Used for testing.
     *
     * @param jsonString JSON string describing synapses
     * @return list of Synapses
     */
    static List<Synapse> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, SYNAPSE_LIST_TYPE);
    }

    private static final Type SYNAPSE_LIST_TYPE = new TypeToken<List<Synapse>>() {
    }.getType();

}
