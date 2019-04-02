package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonParseException;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.Synapse;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashSet;

import static org.janelia.flyem.neuprint.json.JsonUtils.GSON;
import static org.janelia.flyem.neuprint.json.JsonUtils.objectToPrettyFormat;

public class SynapseJsonTest {

    @Test
    public void shouldDeserializeAndSerializeSynapse() {

        String testSynapseJson = "{" +
                "\"type\": \"pre\", \"confidence\": 0.5, \"location\": [5,6,7], \"rois\": [\"roiA\",\"roiB\"]" +
                "}";

        Synapse synapse = GSON.fromJson(testSynapseJson, Synapse.class);

        Assert.assertEquals("pre", synapse.getType());
        Assert.assertEquals(0.5, synapse.getConfidence(), .001);
        Assert.assertEquals(new Location(5L, 6L, 7L), synapse.getLocation());

        Assert.assertEquals(new LinkedHashSet<String>() {{
            add("roiA");
            add("roiB");
        }}, synapse.getRois());

        Assert.assertEquals("roiA", synapse.getRois().iterator().next()); // first listed in json is first in synapse roi set

        String resultSynapseJson = GSON.toJson(synapse);
        Assert.assertEquals(objectToPrettyFormat(testSynapseJson), resultSynapseJson);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfSynapseNotPreOrPost() {

        String testSynapseJson = "{" +
                "\"type\": \"sjdfka\", \"confidence\": 0.5, \"location\": [5,6,7], \"rois\": [\"roiA\",\"roiB\"]" +
                "}";

        GSON.fromJson(testSynapseJson, Synapse.class);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfSynapseTypeMissing() {

        String testSynapseJson = "{" +
                "\"confidence\": 0.5, \"location\": [5,6,7], \"rois\": [\"roiA\",\"roiB\"]" +
                "}";

        GSON.fromJson(testSynapseJson, Synapse.class);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfSynapseLocationMissing() {

        String testSynapseJson = "{" +
                "\"type\": \"post\", \"confidence\": 0.5, \"rois\": [\"roiA\",\"roiB\"]" +
                "}";

        GSON.fromJson(testSynapseJson, Synapse.class);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfConfidenceNotBetweenZeroAndOne() {

        String testSynapseJson = "{" +
                "\"type\": \"post\", \"confidence\": 5, \"location\": [5,6,7], \"rois\": [\"roiA\",\"roiB\"]" +
                "}";

        GSON.fromJson(testSynapseJson, Synapse.class);

    }

    @Test
    public void confidenceShouldBeZeroIfNotSupplied() {
        String testSynapseJson = "{" +
                "\"type\": \"post\", \"location\": [5,6,7], \"rois\": [\"roiA\",\"roiB\"]" +
                "}";

        Synapse synapse = GSON.fromJson(testSynapseJson, Synapse.class);

        Assert.assertEquals(0D, synapse.getConfidence(), .00001);

    }

    @Test
    public void shouldHaveEmptyRoiArrayIfNotSupplied() {

        String testSynapseJson = "{" +
                "\"type\": \"pre\", \"confidence\": 0.5, \"location\": [5,6,7]" +
                "}";

        Synapse synapse = GSON.fromJson(testSynapseJson, Synapse.class);

        Assert.assertEquals(new LinkedHashSet<>(), synapse.getRois());

    }

}
