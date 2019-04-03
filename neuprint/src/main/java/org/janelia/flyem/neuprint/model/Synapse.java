package org.janelia.flyem.neuprint.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprint.json.JsonUtils;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.janelia.flyem.neuprint.model.Neuron.removeUnwantedRois;

/**
 * A class representing a synaptic density. A Synapse has a type (pre or post), a three-
 * dimensional location, a confidence, and a set of rois.
 */
public class Synapse {

    @SerializedName("type")
    private String type;

    @SerializedName("location")
    private Location location;

    @SerializedName("confidence")
    private double confidence;

    @SerializedName("rois")
    public Set<String> rois;

    /**
     * Class constructor used for testing.
     *
     * @param type       type of synaptic density (pre or post)
     * @param confidence confidence of prediction
     * @param location   3D location of density
     * @param roiSet     list of rois synapse is located in
     */
    public Synapse(String type, double confidence, Location location, Set<String> roiSet) {
        this.type = type;
        this.confidence = confidence;
        this.location = location;
        this.rois = roiSet;
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
    public Synapse(String type, Long x, Long y, Long z, Set<String> roiSet) {
        this.type = type;
        this.confidence = 0.0D;
        this.location = new Location(x, y, z);
        this.rois = roiSet;
    }

    public Synapse(String type, double confidence, Location location) {
        this.type = type;
        this.confidence = confidence;
        this.location = location;
        this.rois = new LinkedHashSet<>();
    }

    public Synapse(String type, Long x, Long y, Long z) {
        this.type = type;
        this.confidence = 0.0D;
        this.location = new Location(x, y, z);
        this.rois = new LinkedHashSet<>();
    }

    /**
     * Class constructor used for quickly comparing synapses in neo4j stored procedures.
     *
     * @param x x coordinate of location
     * @param y y coordinate of location
     * @param z z coordinate of location
     */
    public Synapse(Long x, Long y, Long z) {
        this.location = new Location(x, y, z);
    }

    @Override
    public String toString() {
        return "Synapse { " + "type=" + type +
                ", confidence=" + confidence +
                ", location=" + location +
                ", rois=" + rois +
                " }";
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Synapse) {
            final Synapse that = (Synapse) o;
            isEqual = this.location.equals(that.location) && this.type.equals(that.type);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + location.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    private Set<String> locationSetToStringKeys(Set<Location> locationSet) {
        Set<String> locationStringSet = new HashSet<>();
        for (Location location : locationSet) {
            locationStringSet.add(locationToStringKey(location));
        }
        return locationStringSet;
    }

    private String locationToStringKey(Location location) {
        return location.getX() + ":" + location.getY() + ":" + location.getZ();
    }

    /**
     * @return the location represented as a string in format "x:y:z"
     */
    public String getLocationString() {
        return locationToStringKey(this.location);
    }

    /**
     * @return synaptic density's 3D location
     */
    public Location getLocation() {
        return this.location;
    }

    /**
     * Returns location of synaptic density as neo4j {@link Point}.
     *
     * @return {@link Point}
     */
    public Point getLocationAsPoint() {
        return Values.point(9157, this.location.getX(), this.location.getY(), this.location.getZ()).asPoint();
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
    public double getConfidence() {
        return this.confidence;
    }

    /**
     * @return type of synaptic density (pre or post)
     */
    public String getType() {
        return this.type;
    }

    /**
     * @return set of rois in which this synaptic density is located ("-lm" suffix
     * removed if present)
     */
    public Set<String> getRois() {
        // remove -lm tag on rois and unwanted rois from mb6 and fib25
        return removeUnwantedRois(this.rois);
    }

    /**
     * @param dataset name of dataset in which this Synapse exists
     * @return list of rois both with and without "dataset-" prefix
     */
    public Set<String> getRoisWithAndWithoutDatasetPrefix(String dataset) {
        Set<String> roiSet = getRois();
        roiSet.addAll(roiSet.stream().map(r -> dataset + "-" + r).collect(Collectors.toList()));
        return roiSet;
    }

    /**
     * Adds a provided set of rois to this Synapse instance.
     *
     * @param rois a set of rois
     */
    public void addRoiSet(Set<String> rois) {
        this.rois = rois;
    }

    /**
     * Returns a list of {@link Synapse} objects deserialized from a synapses JSON string.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapses JSON format</a>.
     *
     * @param jsonString string containing synapses JSON
     * @return list of {@link Synapse} objects
     */
    public static List<Synapse> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, SYNAPSE_LIST_TYPE);
    }

    /**
     * Returns a list of {@link Synapse} objects deserialized from a {@link BufferedReader} reading from a synapse JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapse JSON format</a>.
     *
     * @param reader {@link BufferedReader}
     * @return list of {@link Synapse} objects
     */
    public static List<Synapse> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader, SYNAPSE_LIST_TYPE);
    }

    /**
     * Returns a list of {@link Synapse} objects as read from a JSON.
     * Used for testing.
     *
     * @param jsonString JSON string describing synapses
     * @return list of {@link Synapse} objects
     */
    static List<Synapse> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, SYNAPSE_LIST_TYPE);
    }

    private static final Type SYNAPSE_LIST_TYPE = new TypeToken<List<Synapse>>() {
    }.getType();

}
