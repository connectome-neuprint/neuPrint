package org.janelia.flyem.connconvert.model;

import java.util.List;

import org.janelia.flyem.connconvert.json.JsonUtils;
import org.janelia.flyem.connconvert.model2.Synapse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link org.janelia.flyem.connconvert.model2.Synapse} class.
 */
public class SynapseTest {

    @Test
    public void testJsonProcessing() {

        final List<org.janelia.flyem.connconvert.model2.Synapse> parsedList = org.janelia.flyem.connconvert.model2.Synapse.fromJsonArray(SYNAPSE_JSON);

        Assert.assertEquals("invalid number of synapses parsed",
                            3, parsedList.size());

        final Synapse firstSynapse = parsedList.get(0);
        final List<String> roiNames = firstSynapse.getRoiNames();
        Assert.assertNotNull("roiNames not parsed for " + firstSynapse,
                             roiNames);
        Assert.assertEquals("invalid number of roiNames parsed for " + firstSynapse,
                            1, roiNames.size());
        Assert.assertEquals("invalid roiName parsed for " + firstSynapse,
                            "seven_column_roi", roiNames.get(0));

        final String serializedJson = JsonUtils.GSON.toJson(parsedList);

        Assert.assertEquals("serialized result does not match original",
                            SYNAPSE_JSON, serializedJson);
    }

    private static final String SYNAPSE_JSON =
            "[\n" +
            "  {\n" +
            "    \"ConnectsFrom\": [\n" +
            "      [\n" +
            "        4657,\n" +
            "        2648,\n" +
            "        1509\n" +
            "      ]\n" +
            "    ],\n" +
            "    \"Confidence\": 1.0,\n" +
            "    \"Location\": [\n" +
            "      4651,\n" +
            "      2627,\n" +
            "      1498\n" +
            "    ],\n" +
            "    \"rois\": [\n" +
            "      \"seven_column_roi\"\n" +
            "    ],\n" +
            "    \"Type\": \"post\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"ConnectsTo\": [],\n" +
            "    \"Confidence\": 1.0,\n" +
            "    \"Location\": [\n" +
            "      4523,\n" +
            "      2673,\n" +
            "      1495\n" +
            "    ],\n" +
            "    \"rois\": [\n" +
            "      \"seven_column_roi\"\n" +
            "    ],\n" +
            "    \"Type\": \"pre\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"ConnectsTo\": [\n" +
            "      [\n" +
            "        4640,\n" +
            "        2649,\n" +
            "        1512\n" +
            "      ],\n" +
            "      [\n" +
            "        4649,\n" +
            "        2636,\n" +
            "        1512\n" +
            "      ],\n" +
            "      [\n" +
            "        4638,\n" +
            "        2643,\n" +
            "        1498\n" +
            "      ],\n" +
            "      [\n" +
            "        4651,\n" +
            "        2627,\n" +
            "        1498\n" +
            "      ],\n" +
            "      [\n" +
            "        4658,\n" +
            "        2630,\n" +
            "        1512\n" +
            "      ],\n" +
            "      [\n" +
            "        4657,\n" +
            "        2644,\n" +
            "        1520\n" +
            "      ]\n" +
            "    ],\n" +
            "    \"Confidence\": 1.0,\n" +
            "    \"Location\": [\n" +
            "      4657,\n" +
            "      2648,\n" +
            "      1509\n" +
            "    ],\n" +
            "    \"rois\": [\n" +
            "      \"seven_column_roi\"\n" +
            "    ],\n" +
            "    \"Type\": \"pre\"\n" +
            "  }\n" +
            "]";

}
