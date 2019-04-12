package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonParseException;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.Neuron;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.janelia.flyem.neuprint.json.JsonUtils.GSON;
import static org.janelia.flyem.neuprint.json.JsonUtils.objectToPrettyFormat;

public class NeuronJsonTest {

    @Test
    public void shouldDeserializeAndSerializeNeuron() {

        String testNeuronString = "{" +
                "\"id\": 1," +
                "\"status\": \"testStatus\"," +
                "\"name\": \"testName\"," +
                "\"type\": \"testType\"," +
                "\"instance\": \"testInstance\"," +
                "\"size\": 80808080," +
                "\"rois\": [\"roiA\",\"roiB\"]," +
                "\"soma\": {\"location\":[8,8,8], \"radius\":5.0}," +
                "\"synapseSet\": [[1,2,3],[4,5,6]]" +
                "}";

        Neuron testNeuron = GSON.fromJson(testNeuronString, Neuron.class);

        Set<String> roiSet = new HashSet<String>() {{
            add("roiA");
            add("roiB");
        }};
        Set<Location> locationSet = new HashSet<Location>() {{
            add(new Location(1L, 2L, 3L));
            add(new Location(4L, 5L, 6L));
        }};

        Assert.assertEquals(1L, (long) testNeuron.getId());
        Assert.assertEquals("testStatus", testNeuron.getStatus());
        Assert.assertEquals("testName", testNeuron.getName());
        Assert.assertEquals("testType", testNeuron.getType());
        Assert.assertEquals("testInstance", testNeuron.getInstance());
        Assert.assertEquals(80808080L, (long) testNeuron.getSize());
        Assert.assertEquals(roiSet, testNeuron.getRois());
        Assert.assertEquals("roiA", testNeuron.getRois().iterator().next());
        Assert.assertEquals(new Location(8L, 8L, 8L), testNeuron.getSoma().getLocation());
        Assert.assertEquals(5.0, testNeuron.getSoma().getRadius(), .0001);
        Assert.assertEquals(locationSet, testNeuron.getSynapseLocationSet());

        String resultJson = GSON.toJson(testNeuron);

        Assert.assertEquals(objectToPrettyFormat(testNeuronString).substring(0, 196), resultJson.substring(0, 196)); // synapse locations may not be in same order

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfIdMissing() {
        String testNeuronString = "{" +
                "\"status\": \"testStatus\"," +
                "\"name\": \"testName\"," +
                "\"type\": \"testType\"," +
                "\"instance\": \"testInstance\"," +
                "\"size\": 80808080," +
                "\"rois\": [\"roiA\",\"roiB\"]," +
                "\"soma\": {\"location\":[8,8,8], \"radius\":5.0}," +
                "\"synapseSet\": [[1,2,3],[4,5,6]]" +
                "}";

        GSON.fromJson(testNeuronString, Neuron.class);
    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfIdNotNumber() {
        String testNeuronString = "{" +
                "\"id\": \"abc\"," +
                "\"status\": \"testStatus\"," +
                "\"name\": \"testName\"," +
                "\"type\": \"testType\"," +
                "\"instance\": \"testInstance\"," +
                "\"size\": 80808080," +
                "\"rois\": [\"roiA\",\"roiB\"]," +
                "\"soma\": {\"location\":[8,8,8], \"radius\":5.0}," +
                "\"synapseSet\": [[1,2,3],[4,5,6]]" +
                "}";

        GSON.fromJson(testNeuronString, Neuron.class);
    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfIdNotIntegerValue() {
        String testNeuronString = "{" +
                "\"id\": 1.90930," +
                "\"status\": \"testStatus\"," +
                "\"name\": \"testName\"," +
                "\"type\": \"testType\"," +
                "\"instance\": \"testInstance\"," +
                "\"size\": 80808080," +
                "\"rois\": [\"roiA\",\"roiB\"]," +
                "\"soma\": {\"location\":[8,8,8], \"radius\":5.0}," +
                "\"synapseSet\": [[1,2,3],[4,5,6]]" +
                "}";

        GSON.fromJson(testNeuronString, Neuron.class);
    }

    @Test
    public void optionalFieldShouldBeNullOrEmptyIfNotProvided() {
        String testNeuronString = "{" +
                "\"id\": 1" +
                "}";

        Neuron testNeuron = GSON.fromJson(testNeuronString, Neuron.class);

        Assert.assertNull(testNeuron.getStatus());
        Assert.assertNull(testNeuron.getName());
        Assert.assertNull(testNeuron.getType());
        Assert.assertNull(testNeuron.getInstance());
        Assert.assertNull(testNeuron.getSize());
        Assert.assertNull(testNeuron.getSoma());

        Assert.assertEquals(0, testNeuron.getRois().size());
        Assert.assertEquals(0, testNeuron.getSynapseLocationSet().size());

    }

}
