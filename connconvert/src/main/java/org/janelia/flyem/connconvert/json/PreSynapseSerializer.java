package org.janelia.flyem.connconvert.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

import org.janelia.flyem.connconvert.model.PreSynapse;

/**
 * Serializes pre-synapse objects into a generic JSON synapse form.
 */
public class PreSynapseSerializer
        implements JsonSerializer<PreSynapse> {

    @Override
    public JsonElement serialize(final PreSynapse preSynapse,
                                 final Type type,
                                 final JsonSerializationContext jsonSerializationContext) {

        final JsonObject jsonObject = JsonUtils.GSON_WITHOUT_SYNAPSE_ADAPTERS.toJsonTree(preSynapse).getAsJsonObject();
        jsonObject.add(SynapseDeserializer.TYPE_KEY, PRE_PRIMITIVE);
        return jsonObject;
    }

    private static final JsonPrimitive PRE_PRIMITIVE = new JsonPrimitive(PreSynapse.TYPE_VALUE);
}
