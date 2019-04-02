package org.janelia.flyem.neuprint.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Soma;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;

/**
 * Shared utilities for working with JSON.
 */
public class JsonUtils {

    /**
     * Default GSON instance used for serializing and de-serializing data.
     */
    public static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Location.class, new LocationAdapter())
            .registerTypeAdapter(Synapse.class, new SynapseAdapter())
            .registerTypeAdapter(SynapticConnection.class, new ConnectionsAdapter())
            .registerTypeAdapter(Soma.class, new SomaAdapter())
            .registerTypeAdapter(Neuron.class, new NeuronAdapter())
            .setPrettyPrinting()
            .create();

    /**
     * Convert a JSON string containing object to pretty print version
     *
     * @param jsonString input json string
     * @return pretty JSON string
     */
    public static String objectToPrettyFormat(String jsonString) {
        JsonParser parser = new JsonParser();
        JsonObject json = parser.parse(jsonString).getAsJsonObject();

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(json);
    }

    /**
     * Convert a JSON string containing array to pretty print version
     *
     * @param jsonString input json string
     * @return pretty JSON string
     */
    public static String arrayToPrettyFormat(String jsonString) {
        JsonParser parser = new JsonParser();
        JsonArray json = parser.parse(jsonString).getAsJsonArray();

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(json);
    }

}
