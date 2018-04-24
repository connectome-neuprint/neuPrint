package connconvert.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

import connconvert.model.PostSynapse;

/**
 * Serializes post-synapse objects into a generic JSON synapse form.
 */
public class PostSynapseSerializer
        implements JsonSerializer<PostSynapse> {

    @Override
    public JsonElement serialize(final PostSynapse postSynapse,
                                 final Type type,
                                 final JsonSerializationContext jsonSerializationContext) {

        final JsonObject jsonObject = JsonUtils.GSON_WITHOUT_SYNAPSE_ADAPTERS.toJsonTree(postSynapse).getAsJsonObject();
        jsonObject.add(SynapseDeserializer.TYPE_KEY, POST_PRIMITIVE);
        return jsonObject;
    }

    private static final JsonPrimitive POST_PRIMITIVE = new JsonPrimitive(PostSynapse.TYPE_VALUE);
}
