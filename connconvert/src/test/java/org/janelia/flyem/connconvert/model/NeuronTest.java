package org.janelia.flyem.connconvert.model;

import java.util.List;

import org.janelia.flyem.connconvert.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link Neuron} class.
 */
public class NeuronTest {

    @Test
    public void testJsonProcessing() {

        final List<Neuron> parsedList = Neuron.fromJson(NEURON_JSON);

        Assert.assertEquals("invalid number of synapses parsed",
                            11, parsedList.size());

        final Neuron thirdNeuron = parsedList.get(2);
        Assert.assertEquals("invalid name parsed for " + thirdNeuron,
                            "unknown", thirdNeuron.getName());

        final String serializedJson = JsonUtils.GSON.toJson(parsedList);

        Assert.assertEquals("serialized result does not match original",
                            NEURON_JSON, serializedJson);
    }

    private static final String NEURON_JSON =
            "[\n" +
                    "  {\n" +
                    "    \"Id\": 791527493,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 45757140129,\n" +
                    "    \"Name\": \"unknown\",\n" +
                    "    \"NeuronType\": \"testtype\",\n" +
                    "    \"Soma\": {\n" +
                    "      \"Location\": [\n" +
                    "        32799,\n" +
                    "        10276,\n" +
                    "        20320\n" +
                    "      ],\n" +
                    "      \"Radius\": 345.0\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 363388252,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 13100310191,\n" +
                    "    \"Name\": \"unknown\",\n" +
                    "    \"Soma\": {\n" +
                    "      \"Location\": [\n" +
                    "        2996,\n" +
                    "        23786,\n" +
                    "        20800\n" +
                    "      ],\n" +
                    "      \"Radius\": 900.1\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 673509195,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 5718366677,\n" +
                    "    \"Name\": \"unknown\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 327933027,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 27252645494,\n" +
                    "    \"Name\": \"unknown\",\n" +
                    "    \"Soma\": {\n" +
                    "      \"Location\": [\n" +
                    "        13744,\n" +
                    "        9507,\n" +
                    "        23024\n" +
                    "      ],\n" +
                    "      \"Radius\": 1341.0\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 1135441187,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 9035459547,\n" +
                    "    \"Name\": \"unknown\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 981924393,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 10920707717,\n" +
                    "    \"Name\": \"unknown\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 363051403,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 3087158090,\n" +
                    "    \"Name\": \"unknown\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 550750102,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 4737248436,\n" +
                    "    \"Name\": \"unknown\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 363375408,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 10329171652,\n" +
                    "    \"Name\": \"unknown\",\n" +
                    "    \"Soma\": {\n" +
                    "      \"Location\": [\n" +
                    "        15827,\n" +
                    "        15070,\n" +
                    "        8048\n" +
                    "      ],\n" +
                    "      \"Radius\": 301.0\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 1011758621,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 7303993858,\n" +
                    "    \"Name\": \"unknown\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"Id\": 947573616,\n" +
                    "    \"Status\": \"anchor\",\n" +
                    "    \"Size\": 10540490630,\n" +
                    "    \"Name\": \"unknown\"\n" +
                    "  }\n" +
                    "]";


}
