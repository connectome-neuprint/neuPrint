package org.janelia.flyem.neuprinter.model;

import org.janelia.flyem.neuprinter.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Tests the {@link Synapse} class.
 */
public class SynapseTest {

    @Test
    public void testJsonProcessing() {

        final List<Synapse> parsedList = Synapse.fromJsonArray(SYNAPSE_JSON);

        Assert.assertEquals("invalid number of synapses parsed",
                4, parsedList.size());

        final Synapse firstSynapse = parsedList.get(0);
        final Synapse secondSynapse = parsedList.get(1);
        final Synapse thirdSynapse = parsedList.get(3);

        final List<String> roiNames = firstSynapse.getRois();
        final List<Integer> location = firstSynapse.getLocation();

        Assert.assertNotNull("roiNames not parsed for " + firstSynapse,
                roiNames);
        Assert.assertEquals("invalid number of roiNames parsed for " + firstSynapse,
                1, roiNames.size());
        Assert.assertTrue("invalid roiName parsed for " + firstSynapse,
                roiNames.contains("distal"));

        Assert.assertEquals("incorrect synapse type for " + firstSynapse, "pre", firstSynapse.getType());

        Assert.assertEquals("incorrect confidence for " + firstSynapse, 1.0, firstSynapse.getConfidence(), .00001);

        Assert.assertEquals("incorrect location for " + firstSynapse, new Integer(4523), location.get(0));

        Assert.assertEquals("incorrect number of dimensions for location for " + firstSynapse, 3, firstSynapse.getLocation().size());

        Assert.assertEquals("incorrect number of connects to for " + secondSynapse, 6, secondSynapse.getConnectionLocations().size());

        Assert.assertEquals("incorrect number of connects from for " + thirdSynapse, 1, thirdSynapse.getConnectionLocations().size());

        final String serializedJson = JsonUtils.GSON.toJson(parsedList);

        Assert.assertEquals("serialized result does not match original",
                SYNAPSE_JSON.replaceAll("[\\n\\t\\r\\s+]+", " "), serializedJson.replaceAll("[\\n\\t\\r\\s+]+", " "));
    }

    @Test
    public void testEqualsAndHashCode() {

        final List<Synapse> parsedList = Synapse.fromJsonArray(SYNAPSE_JSON);

        Synapse x = parsedList.get(0);
        Synapse y = parsedList.get(1);
        Synapse z = parsedList.get(2);

        Synapse synapse1 = new Synapse(x.getType(), x.getConfidence(), x.getLocation(), x.getConnectionLocations());
        Synapse synapse2 = new Synapse(y.getType(), y.getConfidence(), x.getLocation(), y.getConnectionLocations());
        Synapse synapse3 = new Synapse(z.getType(), z.getConfidence(), x.getLocation(), z.getConnectionLocations());

        //reflexive
        Assert.assertTrue(synapse1.equals(synapse1));
        //symmetric
        Assert.assertTrue(synapse1.equals(synapse2) && synapse2.equals(synapse1));
        //transitive
        Assert.assertTrue(synapse1.equals(synapse2) && synapse2.equals(synapse3) && synapse3.equals(synapse1));
        //consistent
        Assert.assertTrue(synapse2.equals(synapse1) && synapse2.equals(synapse1));
        //not equal to null
        Assert.assertTrue(!synapse2.equals(null));

        Assert.assertNotSame(synapse1, synapse2);
        Assert.assertTrue(synapse1.hashCode() == synapse2.hashCode());

    }

    private static final String SYNAPSE_JSON =
            "[\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"Location\": [\n" +
                    "          4523,\n" +
                    "          2673,\n" +
                    "          1495\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ],\n" +
                    "        \"ConnectsTo\": []\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"Location\": [\n" +
                    "          4657,\n" +
                    "          2648,\n" +
                    "          1509\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ],\n" +
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
                    "        ]\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\",\n" +
                    "        \"Location\": [\n" +
                    "          4644,\n" +
                    "          2664,\n" +
                    "          1529\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ],\n" +
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
                    "        ]\n" +
                    "       },\n" +
                    "      {\n" +
                    "        \"Type\": \"post\",\n" +
                    "        \"Location\": [\n" +
                    "          4644,\n" +
                    "          2664,\n" +
                    "          1529\n" +
                    "        ],\n" +
                    "        \"Confidence\": 1.0,\n" +
                    "        \"rois\": [\n" +
                    "          \"seven_column_roi\",\n" +
                    "          \"distal\"\n" +
                    "        ],\n" +
                    "       \"ConnectsFrom\": [\n" +
                    "          [\n" +
                    "            4626,\n" +
                    "            2646,\n" +
                    "            1529\n" +
                    "          ]\n" +
                    "       ]\n" +
                    "      }\n" +
                    "]";

}
