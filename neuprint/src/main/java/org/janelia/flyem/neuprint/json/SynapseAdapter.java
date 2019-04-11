package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.Synapse;

import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Gson adapter for json files describing synapses.
 */
public class SynapseAdapter implements JsonDeserializer<Synapse>, JsonSerializer<Synapse> {

    private static final String TYPE_KEY = "type";
    private static final String CONFIDENCE_KEY = "confidence";
    private static final String LOCATION_KEY = "location";
    private static final String ROIS_KEY = "rois";

    @Override
    public Synapse deserialize(final JsonElement jsonElement,
                               final Type typeOfT,
                               final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

        final JsonObject jsonObject = jsonElement.getAsJsonObject();

        if (!jsonObject.has(TYPE_KEY)) {
            throw new JsonParseException("Synapse type property missing.");
        }
        final String type = jsonObject.get(TYPE_KEY).getAsString();
        if (type == null || !(type.equals("pre") || type.equals("post"))) {
            throw new JsonParseException("Synapse type must be 'pre' or 'post'.");
        }

        double confidence = 0.0D; // default is 0.0
        if (jsonObject.has(CONFIDENCE_KEY)) {
            confidence = jsonObject.get(CONFIDENCE_KEY).getAsDouble();

            if (confidence < 0 || confidence > 1) {
                throw new JsonParseException("Synapse confidence must be between 0.0 and 1.0.");
            }

        }

        if (!jsonObject.has(LOCATION_KEY)) {
            throw new JsonParseException("Synapse location property missing.");
        }
        final Location location = jsonDeserializationContext.deserialize(jsonObject.get(LOCATION_KEY), Location.class);

        final Set<String> rois = parseRoiJsonArray(jsonObject, ROIS_KEY);

        return new Synapse(type, confidence, location, rois);

    }

    @Override
    public JsonElement serialize(final Synapse srcSynapse,
                                 final Type typeOfT,
                                 final JsonSerializationContext jsonSerializationContext) {

        final JsonObject jsonObject = new JsonObject();

        jsonObject.addProperty(TYPE_KEY, srcSynapse.getType());
        jsonObject.addProperty(CONFIDENCE_KEY, srcSynapse.getConfidence());
        JsonElement location = jsonSerializationContext.serialize(srcSynapse.getLocation());
        jsonObject.add(LOCATION_KEY, location);

        final JsonArray roiArray = new JsonArray();
        for (String roi : srcSynapse.getRois()) {
            roiArray.add(roi);
        }

        jsonObject.add(ROIS_KEY, roiArray);

        return jsonObject;

    }

    public static Set<String> parseRoiJsonArray(JsonObject jsonObject, String roisKey) {
        final Set<String> rois = new LinkedHashSet<>();
        if (jsonObject.has(roisKey)) {
            final JsonArray jsonRoiArray = jsonObject.get(roisKey).getAsJsonArray();
            // order of rois matters for figuring out super-level rois
            for (JsonElement element : jsonRoiArray) {
                rois.add(element.getAsString());
            }
        }
        return rois;
    }

}
