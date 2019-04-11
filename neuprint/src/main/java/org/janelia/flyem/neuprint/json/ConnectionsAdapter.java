package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.SynapticConnection;

import java.lang.reflect.Type;

/**
 * Gson adapter for json files describing synaptic connections.
 */
public class ConnectionsAdapter implements JsonDeserializer<SynapticConnection>, JsonSerializer<SynapticConnection> {

    private static final String PRE_KEY = "pre";
    private static final String POST_KEY = "post";

    @Override
    public SynapticConnection deserialize(final JsonElement jsonElement,
                                          final Type typeOfT,
                                          final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

        final JsonObject jsonObject = jsonElement.getAsJsonObject();

        if (!jsonObject.has(PRE_KEY)) {
            throw new JsonParseException("Connection must have 'pre' property.");
        }
        if (!jsonObject.has(POST_KEY)) {
            throw new JsonParseException("Connection must have 'post' property.");
        }

        final Location preLocation = jsonDeserializationContext.deserialize(jsonObject.get(PRE_KEY), Location.class);
        final Location postLocation = jsonDeserializationContext.deserialize(jsonObject.get(POST_KEY), Location.class);

        return new SynapticConnection(preLocation, postLocation);

    }

    @Override
    public JsonElement serialize(final SynapticConnection srcConnection,
                                 final Type typeOfT,
                                 final JsonSerializationContext jsonSerializationContext) {

        final JsonObject jsonObject = new JsonObject();

        JsonElement preLocation = jsonSerializationContext.serialize(srcConnection.getPreLocation());
        JsonElement postLocation = jsonSerializationContext.serialize(srcConnection.getPostLocation());

        jsonObject.add(PRE_KEY, preLocation);
        jsonObject.add(POST_KEY, postLocation);

        return jsonObject;
    }

}
