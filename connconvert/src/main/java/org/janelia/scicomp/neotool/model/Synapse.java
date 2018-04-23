package org.janelia.scicomp.neotool.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

import org.janelia.scicomp.neotool.json.JsonUtils;

/**
 * Common attributes and methods for all synapses.
 */
public abstract class Synapse {

    @SerializedName("Confidence")
    private double confidence;

    @SerializedName("Location")
    private Location location;

    @SerializedName("rois")
    private List<String> roiNames;

    protected Synapse() {
        this.confidence = 0.0;
        this.location = null;
        this.roiNames = null;
    }

    public double getConfidence() {
        return confidence;
    }

    public Location getLocation() {
        return location;
    }

    public List<String> getRoiNames() {
        return roiNames;
    }

    @Override
    public boolean equals(final Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Synapse) {
            final Synapse that = (Synapse) o;
            isEqual = Objects.equals(this.location, that.location);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }

    @Override
    public String toString() {
        return "{ type: " + getType() + ", location: " + location +
               ", numberOfConnections: " + getNumberOfConnections() + " }";
    }

    public boolean isPreSynapse() {
        return false;
    }

    public abstract String getType();

    public abstract String getLabelType();

    public abstract List<Location> getConnections();

    public abstract int getNumberOfConnections();

    public static List<Synapse> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, SYNAPSE_LIST_TYPE);
    }

    private static final Type SYNAPSE_LIST_TYPE = new TypeToken<List<Synapse>>(){}.getType();
}
