package connconvert.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.janelia.scicomp.neotool.model.Location;
import org.janelia.scicomp.neotool.model.PostSynapse;
import org.janelia.scicomp.neotool.model.PreSynapse;
import org.janelia.scicomp.neotool.model.Synapse;

/**
 * Shared utilities for working with JSON.
 */
public class JsonUtils {

    /** Default GSON instance used for serializing and de-serializing data. */
    public static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Location.class, new LocationAdapter())
            .registerTypeAdapter(Synapse.class, new SynapseDeserializer())
            .registerTypeAdapter(PostSynapse.class, new PostSynapseSerializer())
            .registerTypeAdapter(PreSynapse.class, new PreSynapseSerializer())
            .setPrettyPrinting()
            .create();

    /** A GSON instance without synapse adapters that can be used by the synapse serializers. */
    public static final Gson GSON_WITHOUT_SYNAPSE_ADAPTERS = new GsonBuilder()
            .registerTypeAdapter(Location.class, new LocationAdapter())
            .setPrettyPrinting()
            .create();

}
