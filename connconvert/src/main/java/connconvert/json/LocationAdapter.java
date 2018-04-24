package connconvert.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

import org.janelia.scicomp.neotool.model.Location;

/**
 * Serializes and deserializes synapse locations.
 */
public class LocationAdapter
        implements JsonSerializer<Location>, JsonDeserializer<Location> {

    @Override
    public Location deserialize(final JsonElement jsonElement,
                                final Type type,
                                final JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {

        final int[] locationArray = jsonDeserializationContext.deserialize(jsonElement, int[].class);
        return new Location(locationArray[0], locationArray[1], locationArray[2]);
    }

    @Override
    public JsonElement serialize(final Location location,
                                 final Type type,
                                 final JsonSerializationContext jsonSerializationContext) {
        return jsonSerializationContext.serialize(location.toArray());
    }
}
