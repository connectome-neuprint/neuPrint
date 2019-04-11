package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.flyem.neuprint.model.Location;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Gson adapter for 3D locations.
 */
public class LocationAdapter implements JsonDeserializer<Location>, JsonSerializer<Location> {

    @Override
    public Location deserialize(final JsonElement jsonElement,
                                final Type typeOfT,
                                final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

        final JsonArray jsonLocationArray = jsonElement.getAsJsonArray();

        if (jsonLocationArray == null || jsonLocationArray.size() != 3) {
            throw new JsonParseException("Location must be three-dimensional.");
        }

        List<Long> location = new ArrayList<>();

        for (JsonElement element : jsonLocationArray) {
            double d;
            long l;
            try {
                d = element.getAsDouble();
                l = element.getAsLong();
            } catch (NumberFormatException nfe) {
                throw new JsonParseException("Location coordinates must be a number.");
            }
            if (d == l) {
                location.add(l);
            } else {
                throw new JsonParseException("Location coordinates must be integer values.");
            }
        }

        return new Location(location.get(0), location.get(1), location.get(2));

    }

    @Override
    public JsonElement serialize(final Location srcLocation,
                                 final Type typeOfSrc,
                                 final JsonSerializationContext jsonSerializationContext) {

        final JsonArray jsonLocationArray = new JsonArray();

        jsonLocationArray.add(srcLocation.getX());
        jsonLocationArray.add(srcLocation.getY());
        jsonLocationArray.add(srcLocation.getZ());

        return jsonLocationArray;

    }
}
