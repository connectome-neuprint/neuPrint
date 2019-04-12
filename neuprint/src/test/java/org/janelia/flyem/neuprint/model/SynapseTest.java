package org.janelia.flyem.neuprint.model;

import org.janelia.flyem.neuprint.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.janelia.flyem.neuprint.json.JsonUtils.arrayToPrettyFormat;

/**
 * Tests the {@link Synapse} class.
 */
public class SynapseTest {

    @Test
    public void testJsonProcessing() {

        final List<Synapse> parsedList = Synapse.fromJsonArray(SYNAPSE_JSON);

        Assert.assertEquals("invalid number of synapses parsed",
                3, parsedList.size());

        final Synapse firstSynapse = parsedList.get(0);

        final Set<String> roiNames = firstSynapse.getRois();
        final Location location = firstSynapse.getLocation();

        Assert.assertNotNull("roi names not parsed for " + firstSynapse,
                roiNames);
        Assert.assertEquals("invalid number of roi names parsed for " + firstSynapse,
                2, roiNames.size());
        Assert.assertTrue("invalid roi name parsed for " + firstSynapse,
                roiNames.contains("roiA"));

        Assert.assertEquals("incorrect synapse type for " + firstSynapse, "pre", firstSynapse.getType());

        Assert.assertEquals("incorrect confidence for " + firstSynapse, .5, firstSynapse.getConfidence(), .00001);

        Assert.assertEquals("incorrect location for " + firstSynapse, new Location(5L,6L,7L), location);

        final String serializedJson = JsonUtils.GSON.toJson(parsedList);

        Assert.assertEquals("serialized result does not match original",
                arrayToPrettyFormat(SYNAPSE_JSON), serializedJson);
    }

    @Test
    public void testEqualsAndHashCode() {

        final List<Synapse> parsedList = Synapse.fromJsonArray(SYNAPSE_JSON);

        Synapse x = parsedList.get(0);
        Synapse y = parsedList.get(1);
        Synapse z = parsedList.get(2);

        Synapse synapse1 = new Synapse("pre", x.getConfidence(), x.getLocation());
        Synapse synapse2 = new Synapse("pre", y.getConfidence(), x.getLocation());
        Synapse synapse3 = new Synapse("pre", z.getConfidence(), x.getLocation());

        //reflexive
        Assert.assertEquals(synapse1, synapse1);
        //symmetric
        Assert.assertTrue(synapse1.equals(synapse2) && synapse2.equals(synapse1));
        //transitive
        Assert.assertTrue(synapse1.equals(synapse2) && synapse2.equals(synapse3) && synapse3.equals(synapse1));
        //consistent
        Assert.assertTrue(synapse2.equals(synapse1) && synapse2.equals(synapse1));
        //not equal to null
        Assert.assertNotNull(synapse2);

        Assert.assertNotSame(synapse1, synapse2);
        Assert.assertEquals(synapse1.hashCode(), synapse2.hashCode());

    }

    private static final String SYNAPSE_JSON = "[" +
            "{" +
            "\"type\": \"pre\", \"confidence\": 0.5, \"location\": [5,6,7], \"rois\": [\"roiA\",\"roiB\"]" +
            "}," +
            "{" +
            "\"type\": \"post\", \"confidence\": 0.3, \"location\": [5,6,7], \"rois\": [\"roiA\",\"roiC\"]" +
            "}," +
            "{" +
            "\"type\": \"pre\", \"confidence\": 0.5, \"location\": [50,60,70], \"rois\": []" +
            "}" +
            "]";

}
