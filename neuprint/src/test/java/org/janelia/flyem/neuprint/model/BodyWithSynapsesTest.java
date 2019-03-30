package org.janelia.flyem.neuprint.model;

import org.janelia.flyem.neuprint.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Tests the {@link BodyWithSynapses} class.
 */
public class BodyWithSynapsesTest {

    @Test
    public void testJsonProcessing() {

        final List<BodyWithSynapses> parsedBodyList = BodyWithSynapses.fromJson(BODY_LIST_JSON);

        Assert.assertEquals("invalid number of bodies parsed",
                4, parsedBodyList.size());

        Assert.assertEquals("invalid number of synapses for " + parsedBodyList.get(1), 2, parsedBodyList.get(1).getSynapseSet().size());

        final String serializedJson = JsonUtils.GSON.toJson(parsedBodyList);

        Assert.assertTrue("serialized result is empty",
                serializedJson.length() > 0);

        Assert.assertEquals("serialized result does not match original",
                BODY_LIST_JSON.replaceAll("[\\n\\t\\r\\s+]+", " "), serializedJson.replaceAll("[\\n\\t\\r\\s+]+", " "));

    }

    @Test
    public void testEqualsAndHashCode() {

        final List<BodyWithSynapses> parsedBodyList = BodyWithSynapses.fromJson(BODY_LIST_JSON);

        BodyWithSynapses body1 = new BodyWithSynapses(85L, parsedBodyList.get(0).getSynapseSet());
        BodyWithSynapses body2 = new BodyWithSynapses(85L, parsedBodyList.get(1).getSynapseSet());
        BodyWithSynapses body3 = new BodyWithSynapses(85L, parsedBodyList.get(2).getSynapseSet());

        //reflexive
        Assert.assertEquals(body1, body1);
        //symmetric
        Assert.assertTrue(body1.equals(body2) && body2.equals(body1));
        //transitive
        Assert.assertTrue(body1.equals(body2) && body2.equals(body3) && body3.equals(body1));
        //consistent
        Assert.assertTrue(body2.equals(body1) && body2.equals(body1));
        //not equal to null
        Assert.assertNotNull(body2);

        Assert.assertNotSame(body1, body2);
        Assert.assertEquals(body1.hashCode(), body2.hashCode());

    }

    @Test
    public void testGetSynapticLocations() {

        final List<BodyWithSynapses> parsedBodyList = BodyWithSynapses.fromJson(BODY_LIST_JSON);

        BodyWithSynapses body1 = parsedBodyList.get(0);
        BodyWithSynapses body2 = parsedBodyList.get(1);
        BodyWithSynapses body3 = parsedBodyList.get(2);

        List<String> postloclist = new ArrayList<>();
        List<String> preloclist = new ArrayList<>();
        postloclist.add("4202:2402:1688");
        preloclist.add("4287:2277:1542");

        Assert.assertEquals(body1.getPreLocations(), preloclist);
        Assert.assertEquals(body1.getPostLocations(), postloclist);

        List<String> postloclist2 = new ArrayList<>();
        List<String> emptylist = new ArrayList<>();
        postloclist2.add("4301:2276:1535");
        postloclist2.add("4222:2402:1688");

        Assert.assertEquals(body2.getPostLocations(), postloclist2);
        Assert.assertEquals(body2.getPreLocations(), emptylist);

        List<String> preloclist2 = new ArrayList<>();
        preloclist2.add("4298:2294:1542");
        preloclist2.add("4287:2277:1542");

        Assert.assertEquals(body3.getPreLocations(), preloclist2);
        Assert.assertEquals(body3.getPostLocations(), emptylist);

    }

    @Test
    public void testGetBodyRois() {

        final List<BodyWithSynapses> parsedBodyList = BodyWithSynapses.fromJson(BODY_LIST_JSON);

        List<String> firstBodyRois = parsedBodyList.get(0).getBodyRois();
        List<String> roiList = new ArrayList<>();
        roiList.add("testroi");

        Assert.assertEquals(roiList, firstBodyRois);

    }

    @Test
    public void testAddSynapseToBodyIdMapAndSetSynapseCounts() {

        final List<BodyWithSynapses> parsedBodyList = BodyWithSynapses.fromJson(BODY_LIST_JSON);

        final SynapseLocationToBodyIdMap postSynapseLocationToBodyIdMap = new SynapseLocationToBodyIdMap();
        final SynapseLocationToBodyIdMap preSynapseLocationToBodyIdMap = new SynapseLocationToBodyIdMap();

        BodyWithSynapses body1 = parsedBodyList.get(0);
        BodyWithSynapses body2 = parsedBodyList.get(1);

        body1.addSynapsesToBodyIdMapAndSetSynapseCounts("post", postSynapseLocationToBodyIdMap);
        body1.addSynapsesToBodyIdMapAndSetSynapseCounts("pre", preSynapseLocationToBodyIdMap);

        Assert.assertEquals("Incorrect post-synapse to bodyId for " + body1, new Long(8426959), postSynapseLocationToBodyIdMap.getBodyId("4202:2402:1688"));
        Assert.assertEquals("Incorrect pre-synapse to bodyId mapping for " + body1, new Long(8426959), preSynapseLocationToBodyIdMap.getBodyId("4287:2277:1542"));

        body2.addSynapsesToBodyIdMapAndSetSynapseCounts("post", postSynapseLocationToBodyIdMap);
        body2.addSynapsesToBodyIdMapAndSetSynapseCounts("pre", preSynapseLocationToBodyIdMap);

        Assert.assertEquals("Incorrect number of keys in pre-synaptic location to bodyId map", 1, preSynapseLocationToBodyIdMap.getAllLocationKeys().size());
        Assert.assertEquals("Incorrect number of keys in post-synaptic location to bodyId map", 3, postSynapseLocationToBodyIdMap.getAllLocationKeys().size());

        Assert.assertEquals("Incorrect number of synapses for " + body1, 2, body1.getNumberOfPostSynapses() + body1.getNumberOfPreSynapses());
        Assert.assertEquals("Incorrect number of synapses for " + body2, new Integer(2), body2.getNumberOfPostSynapses());

    }

    @Test
    public void testSetConnectsTo() {

        final List<BodyWithSynapses> parsedBodyList = BodyWithSynapses.fromJson(BODY_LIST_JSON);
        final SynapseLocationToBodyIdMap postSynapseLocationToBodyIdMap = new SynapseLocationToBodyIdMap();

        for (BodyWithSynapses bws : parsedBodyList) {
            bws.addSynapsesToBodyIdMapAndSetSynapseCounts("post", postSynapseLocationToBodyIdMap);
        }

        BodyWithSynapses body1 = parsedBodyList.get(0);
        body1.setConnectsTo(postSynapseLocationToBodyIdMap);

        HashMap<Long, SynapseCounter> body1ConnectsTo = body1.getConnectsTo();

        Assert.assertEquals("Incorrect connections for " + body1, 2, body1ConnectsTo.get(26311L).getPost());
        Assert.assertEquals("Incorrect connections for " + body1, 1, body1.getConnectsTo().get(831744L).getPost());
        Assert.assertEquals("Incorrect connections for " + body1, 1, body1ConnectsTo.get(26311L).getPre());
        Assert.assertEquals("Incorrect connections for " + body1, 1, body1.getConnectsTo().get(831744L).getPre());
        Assert.assertEquals("Incorrect number of postsynaptic partners for " + body1, 2, body1.getConnectsTo().keySet().size());

    }

    @Test
    public void testAddSynapsesToPreToPostMap() {

        final List<BodyWithSynapses> parsedBodyList = BodyWithSynapses.fromJson(BODY_LIST_JSON);

        final HashMap<String, Set<String>> preToPostMap = new HashMap<>();

        BodyWithSynapses body1 = parsedBodyList.get(0);

        body1.addSynapsesToPreToPostMap(preToPostMap);

        Assert.assertEquals(3, preToPostMap.get("4287:2277:1542").size());
        Assert.assertTrue(preToPostMap.get("4287:2277:1542").contains("4292:2261:1542"));

    }

    private static final String BODY_LIST_JSON =
            "[\n" +
                    "  {\n" +
                    "    \"BodyId\": 8426959,\n" +
                    "    \"SynapseSet\": [\n" +
                    "      {\n" +
                    "        \"Type\": \"pre\", \"Location\": [ 4287, 2277, 1542 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\", \"testroi\" ],\n" +
                    "        \"ConnectsTo\": [\n" +
                    "          [ 4292, 2261, 1542 ], [ 4301, 2276, 1535 ], [ 4222, 2402, 1688 ]\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      { \"Type\": \"post\", \"Location\": [ 4202, 2402, 1688 ], \"Confidence\": 1.0, \"rois\": [ \"seven_column_roi\" ], \"ConnectsFrom\": [ [ 4236, 2394, 1700 ] ]  }\n" +
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
