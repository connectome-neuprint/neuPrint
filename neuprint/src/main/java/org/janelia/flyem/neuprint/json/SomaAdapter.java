package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.Soma;

import java.lang.reflect.Type;

public class SomaAdapter implements JsonDeserializer<Soma>, JsonSerializer<Soma> {

    private static final String LOCATION_KEY = "location";
    private static final String RADIUS_KEY = "radius";

    @Override
    public Soma deserialize(final JsonElement jsonElement,
                            final Type typeOfT,
                            final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

        final JsonObject jsonObject = jsonElement.getAsJsonObject();

        if (!jsonObject.has(LOCATION_KEY)) {
            throw new JsonParseException("Soma must have 'location' property.");
        }
        if (!jsonObject.has(RADIUS_KEY)) {
            throw new JsonParseException("Soma must have 'radius' property.");
        }

        final Location location = jsonDeserializationContext.deserialize(jsonObject.get(LOCATION_KEY), Location.class);

        Double radius;
        try {
            radius = jsonObject.get(RADIUS_KEY).getAsDouble();
        } catch (NumberFormatException nfe) {
            throw new JsonParseException("Radius must be a number.");
        }

        return new Soma(location, radius);

    }

    @Override
    public JsonElement serialize(final Soma srcSoma,
                                 final Type typeOfT,
                                 final JsonSerializationContext jsonSerializationContext) {

        final JsonObject jsonObject = new JsonObject();

        JsonElement location = jsonSerializationContext.serialize(srcSoma.getLocation());

        jsonObject.add(LOCATION_KEY, location);
        jsonObject.addProperty(RADIUS_KEY, srcSoma.getRadius());

        return jsonObject;
    }
}
