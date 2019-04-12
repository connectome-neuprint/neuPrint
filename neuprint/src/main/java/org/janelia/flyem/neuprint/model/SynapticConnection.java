package org.janelia.flyem.neuprint.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprint.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.List;

/**
 * A class representing a synaptic connection between a presynaptic density and a postsynaptic density.
 * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">connection JSON format</a>.
 */
public class SynapticConnection {

    @SerializedName("pre")
    private Location preLocation;

    @SerializedName("post")
    private Location postLocation;

    public SynapticConnection(Location preLocation, Location postLocation) {
        this.preLocation = preLocation;
        this.postLocation = postLocation;
    }

    public Location getPreLocation() {
        return preLocation;
    }

    public Location getPostLocation() {
        return postLocation;
    }

    @Override
    public String toString() {
        return this.preLocation + "->" + this.postLocation;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SynapticConnection) {
            final SynapticConnection that = (SynapticConnection) o;
            isEqual = this.preLocation.equals(that.preLocation) && this.postLocation.equals(that.postLocation);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.preLocation.hashCode();
        result = 31 * result + this.postLocation.hashCode();
        return result;
    }

    /**
     * Returns a list of {@link SynapticConnection} objects deserialized from a connections JSON string.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">connections JSON format</a>.
     *
     * @param jsonString string containing connections JSON
     * @return list of {@link SynapticConnection} objects
     */
    public static List<SynapticConnection> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, CONNECTION_LIST_TYPE);
    }

    /**
     * Returns a list of {@link SynapticConnection} objects deserialized from a {@link BufferedReader} reading from a connections JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">connections JSON format</a>.
     *
     * @param reader {@link BufferedReader}
     * @return list of {@link SynapticConnection} objects
     */
    public static List<SynapticConnection> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader, CONNECTION_LIST_TYPE);
    }

    /**
     * Returns a list of {@link SynapticConnection} objects as read from a JSON.
     * Used for testing.
     *
     * @param jsonString JSON string describing connections
     * @return list of {@link SynapticConnection} objects
     */
    static List<SynapticConnection> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, CONNECTION_LIST_TYPE);
    }

    /**
     * Returns a SynapticConnection deserialized from a single JSON object from a connection JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">connection JSON format</a>.
     *
     * @param reader {@link JsonReader}
     * @return SynapticConnection
     */
    public static SynapticConnection fromJsonSingleObject(final JsonReader reader) {
        return JsonUtils.GSON.fromJson(reader, SynapticConnection.class);
    }

    private static final Type CONNECTION_LIST_TYPE = new TypeToken<List<SynapticConnection>>() {
    }.getType();
}
