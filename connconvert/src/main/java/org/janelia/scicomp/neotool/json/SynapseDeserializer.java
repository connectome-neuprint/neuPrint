package org.janelia.scicomp.neotool.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

import org.janelia.scicomp.neotool.model.PostSynapse;
import org.janelia.scicomp.neotool.model.PreSynapse;
import org.janelia.scicomp.neotool.model.Synapse;

/**
 * Deserializes generic JSON synapses into pre or post synapse objects.
 */
public class SynapseDeserializer
        implements JsonDeserializer<Synapse> {

    public static final String TYPE_KEY = "Type";

    @Override
    public Synapse deserialize(final JsonElement jsonElement,
                               final Type type,
                               final JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {

        final JsonObject jsonObject = jsonElement.getAsJsonObject();
        final String synapseType = jsonObject.get(TYPE_KEY).getAsString();

        final Synapse synapse;
        switch (synapseType) {

            case PreSynapse.TYPE_VALUE:
                synapse = jsonDeserializationContext.deserialize(jsonElement, PreSynapse.class);
                break;

            case PostSynapse.TYPE_VALUE:
                synapse = jsonDeserializationContext.deserialize(jsonElement, PostSynapse.class);
                break;

            default:
                throw new JsonParseException("invalid synapse type '" + synapseType + "', must be '" +
                                             PreSynapse.TYPE_VALUE + "' or '" + PostSynapse.TYPE_VALUE + "'");
        }

        return synapse;
    }

}
