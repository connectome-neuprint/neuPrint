package org.janelia.flyem.connconvert.model;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link SynapseMapper} class.
 */
public class SynapseMapperTest {

    @Test
    public void testMapBodies() {

        final String bodyJsonFilePath = "src/test/resources/smallBodyList.json";

        final SynapseMapper mapper = new SynapseMapper();
        final List<BodyWithSynapses> parsedBodyList = mapper.loadAndMapBodies(bodyJsonFilePath);

        Assert.assertEquals("invalid number of bodies parsed",
                            4, parsedBodyList.size());

        int totalSynapseCount = 0;
//        for (final BodyWithSynapses body : parsedBodyList) {
//            final int numberOfConnections = body.getNumberOfPreSynapses() +
//                                            body.getNumberOfPostSynapses();
//            Assert.assertTrue("no connections for body " + body,
//                              numberOfConnections > 0);
//            totalSynapseCount += body.getSynapseSet().size();
//        }

//        Assert.assertEquals("invalid number of synapses parsed", 5, totalSynapseCount);


        final SynapseMapper mapper2 = new SynapseMapper();
        final List<BodyWithSynapses> parsedBodyList2 = mapper2.loadAndMapBodiesFromJsonString(BODY_LIST_JSON);

        SynapseLocationToBodyIdMap postToBody = mapper2.getSynapseLocationToBodyIdMap();

        Assert.assertEquals(4,postToBody.getAllLocationKeys().size());
        Assert.assertEquals(new Long(831744),postToBody.getBodyId("4292:2261:1542"));


        // TODO: improve this test
    }


    private static final String BODY_LIST_JSON =
            "[\n" +
                    "  {\n" +
                    "    \"BodyId\": 8426959,\n" +
                    "    \"SynapseSet\": [\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\", \"Location\": [ 4287, 2277, 1542 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\", \"testroi\" ],\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [ 4298, 2294, 1542 ], [ 4301, 2276, 1535 ], [ 4292, 2261, 1542 ]\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      { \"Type\": \"post\", \"Location\": [ 4222, 2402, 1678 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\" ], \"ConnectsFrom\": [ [ 4236, 2394, 1700 ] ]  }\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"BodyId\": 26311,\n" +
                    "    \"SynapseSet\": [\n" +
                    "      { \"Type\": \"post\", \"Location\": [ 4301, 2276, 1535 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\" ], \"ConnectsFrom\": [ [ 4287, 2277, 1542 ] ] },\n" +
                    "        { \"Type\": \"post\", \"Location\": [ 4222, 2402, 1688 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\" ], \"ConnectsFrom\": [ [ 4236, 2394, 1700 ] ]  }\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"BodyId\": 2589725,\n" +
                    "    \"SynapseSet\": [\n" +
                    "      { \"Type\": \"pre\", \"Location\": [ 4298, 2294, 1542 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\" ], \"ConnectsFrom\": [ [ 4287, 2277, 1542 ] ] },\n" +
                    "       {\n" +
                    "        \"Type\": \"pre\", \"Location\": [ 4287, 2277, 1542 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\" ],\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [ 4298, 2294, 1542 ], [ 4301, 2276, 1535 ], [ 4292, 2261, 1542 ]\n" +
                    "        ]\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"BodyId\": 831744,\n" +
                    "    \"SynapseSet\": [\n" +
                    "      { \"Type\": \"post\", \"Location\": [ 4292, 2261, 1542 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\" ], \"ConnectsFrom\": [ [ 4287, 2277, 1542 ] ] }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "]";


}
