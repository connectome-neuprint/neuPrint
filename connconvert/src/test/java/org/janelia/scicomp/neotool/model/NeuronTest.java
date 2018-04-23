package org.janelia.scicomp.neotool.model;

import java.util.List;

import org.janelia.scicomp.neotool.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link Neuron} class.
 */
public class NeuronTest {

    @Test
    public void testJsonProcessing() throws Exception {

        final List<Neuron> parsedList = Neuron.fromJsonArray(NEURON_JSON);

        Assert.assertEquals("invalid number of neurons parsed",
                            4, parsedList.size());

        final Neuron thirdNeuron = parsedList.get(2);
        Assert.assertEquals("invalid name parsed for " + thirdNeuron,
                            "Dm20-5", thirdNeuron.getName());

        final String serializedJson = JsonUtils.GSON.toJson(parsedList);

        Assert.assertEquals("serialized result does not match original",
                            NEURON_JSON, serializedJson);
    }

    private static final String NEURON_JSON =
            "[\n" +
            "  {\n" +
            "    \"Id\": 100574,\n" +
            "    \"Status\": \"final\",\n" +
            "    \"Size\": 2988\n" +
            "  },\n" +
            "  {\n" +
            "    \"Id\": 1005870,\n" +
            "    \"Status\": \"final\",\n" +
            "    \"Name\": \"out-1005870\",\n" +
            "    \"Size\": 778567\n" +
            "  },\n" +
            "  {\n" +
            "    \"Id\": 100598,\n" +
            "    \"Status\": \"final\",\n" +
            "    \"Name\": \"Dm20-5\",\n" +
            "    \"Size\": 6197623\n" +
            "  },\n" +
            "  {\n" +
            "    \"Id\": 100614,\n" +
            "    \"Status\": \"final\",\n" +
            "    \"Size\": 6691\n" +
            "  }\n" +
            "]";

}
