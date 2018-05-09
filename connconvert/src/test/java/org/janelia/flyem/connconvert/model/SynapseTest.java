package org.janelia.flyem.connconvert.model;

import java.util.List;

import org.janelia.flyem.connconvert.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link Synapse} class.
 */
public class SynapseTest {

    @Test
    public void testJsonProcessing() {

        final List<Synapse> parsedList = Synapse.fromJsonArray(SYNAPSE_JSON);

        Assert.assertEquals("invalid number of synapses parsed",
                            8, parsedList.size());

        final Synapse firstSynapse = parsedList.get(0);
        final List<String> roiNames = firstSynapse.getRois();
        Assert.assertNotNull("roiNames not parsed for " + firstSynapse,
                             roiNames);
        Assert.assertEquals("invalid number of roiNames parsed for " + firstSynapse,
                            2, roiNames.size());
        Assert.assertEquals("invalid roiName parsed for " + firstSynapse,
                            "seven_column_roi", roiNames.get(0));

        final String serializedJson = JsonUtils.GSON.toJson(parsedList);

        Assert.assertEquals("serialized result does not match original",
                            SYNAPSE_JSON, serializedJson);
    }

    private static final String SYNAPSE_JSON =
            "[\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"Location\": [\n" +
                    "          4523,\n" +
                    "          2673,\n" +
                    "          1495\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [\n" +
                    "            4640,\n" +
                    "            2649,\n" +
                    "            1512\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4649,\n" +
                    "            2636,\n" +
                    "            1512\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4638,\n" +
                    "            2643,\n" +
                    "            1498\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4651,\n" +
                    "            2627,\n" +
                    "            1498\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4658,\n" +
                    "            2630,\n" +
                    "            1512\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4657,\n" +
                    "            2644,\n" +
                    "            1520\n" +
                    "          ]\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"Location\": [\n" +
                    "          4657,\n" +
                    "          2648,\n" +
                    "          1509\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [\n" +
                    "            4626,\n" +
                    "            2646,\n" +
                    "            1529\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4647,\n" +
                    "            2643,\n" +
                    "            1529\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4635,\n" +
                    "            2652,\n" +
                    "            1520\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4659,\n" +
                    "            2647,\n" +
                    "            1534\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4634,\n" +
                    "            2648,\n" +
                    "            1548\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4665,\n" +
                    "            2643,\n" +
                    "            1524\n" +
                    "          ]\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"Location\": [\n" +
                    "          4644,\n" +
                    "          2664,\n" +
                    "          1529\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [\n" +
                    "            4519,\n" +
                    "            2688,\n" +
                    "            1517\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4501,\n" +
                    "            2683,\n" +
                    "            1517\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4497,\n" +
                    "            2696,\n" +
                    "            1523\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4515,\n" +
                    "            2696,\n" +
                    "            1500\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4513,\n" +
                    "            2692,\n" +
                    "            1496\n" +
                    "          ]\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"Location\": [\n" +
                    "          4512,\n" +
                    "          2702,\n" +
                    "          1517\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [\n" +
                    "            4537,\n" +
                    "            2685,\n" +
                    "            1517\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4547,\n" +
                    "            2703,\n" +
                    "            1515\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4562,\n" +
                    "            2691,\n" +
                    "            1515\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4531,\n" +
                    "            2674,\n" +
                    "            1525\n" +
                    "          ]\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"Location\": [\n" +
                    "          4550,\n" +
                    "          2688,\n" +
                    "          1521\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [\n" +
                    "            4599,\n" +
                    "            2629,\n" +
                    "            1549\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4591,\n" +
                    "            2645,\n" +
                    "            1550\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4600,\n" +
                    "            2624,\n" +
                    "            1556\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4598,\n" +
                    "            2609,\n" +
                    "            1556\n" +
                    "          ]\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"Location\": [\n" +
                    "          4583,\n" +
                    "          2627,\n" +
                    "          1552\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [\n" +
                    "            4600,\n" +
                    "            2627,\n" +
                    "            1555\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4591,\n" +
                    "            2644,\n" +
                    "            1549\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4601,\n" +
                    "            2632,\n" +
                    "            1550\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4588,\n" +
                    "            2615,\n" +
                    "            1569\n" +
                    "          ],\n" +
                    "          [\n" +
                    "            4599,\n" +
                    "            2630,\n" +
                    "            1572\n" +
                    "          ]\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"Location\": [\n" +
                    "          4601,\n" +
                    "          2645,\n" +
                    "          1555\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [\n" +
                    "            4512,\n" +
                    "            2699,\n" +
                    "            1546\n" +
                    "          ]\n" +
                    "        ],\n" +
                    "        \"Confidence\": 0.5,\n" +
                    "        \"Location\": [\n" +
                    "          4527,\n" +
                    "          2686,\n" +
                    "          1546\n" +
                    "        ],\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ]\n" +
                    "      }\n" +
            "]";

}
