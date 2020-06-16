package org.janelia.flyem.neuprint.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests the {@link Neuron} class.
 */
public class NeuronTest {

    @Test
    public void testJsonProcessing() {

        final List<Neuron> parsedList = Neuron.fromJson(NEURON_JSON);

        Assert.assertEquals("invalid number of neurons parsed",
                3, parsedList.size());

        final Neuron thirdNeuron = parsedList.get(2);
        Assert.assertEquals("invalid name parsed for " + thirdNeuron,
                "testName3", thirdNeuron.getName());
    }

    @Test
    public void testEqualsAndHashCode() {

        final List<Neuron> parsedList = Neuron.fromJson(NEURON_JSON);

        Neuron x = parsedList.get(0);
        Neuron y = parsedList.get(1);
        Neuron z = parsedList.get(2);

        Neuron neuron1 = new Neuron(85L, x.getStatus(), x.getName(), x.getType(), x.getInstance(),
                                    x.getPrimaryNeurite(), x.getMajorInput(), x.getMajorOutput(),
                                    x.getClonalUnit(), x.getNeurotransmitter(), x.getProperty(),
                                    x.getSize(), x.getRois(), x.getSoma(), x.getSynapseLocationSet());
        Neuron neuron2 = new Neuron(85L, y.getStatus(), y.getName(), y.getType(), y.getInstance(),
                                    y.getPrimaryNeurite(), y.getMajorInput(), y.getMajorOutput(),
                                    y.getClonalUnit(), y.getNeurotransmitter(), y.getProperty(),
                                    y.getSize(), y.getRois(), y.getSoma(), y.getSynapseLocationSet());
        Neuron neuron3 = new Neuron(85L, z.getStatus(), z.getName(), z.getType(), z.getInstance(),
                                    z.getPrimaryNeurite(), z.getMajorInput(), z.getMajorOutput(),
                                    z.getClonalUnit(), z.getNeurotransmitter(), z.getProperty(),
                                    z.getSize(), z.getRois(), z.getSoma(), z.getSynapseLocationSet());

        //reflexive
        Assert.assertEquals(neuron1, neuron1);
        //symmetric
        Assert.assertTrue(neuron1.equals(neuron2) && neuron2.equals(neuron1));
        //transitive
        Assert.assertTrue(neuron1.equals(neuron2) && neuron2.equals(neuron3) && neuron3.equals(neuron1));
        //consistent
        Assert.assertTrue(neuron2.equals(neuron1) && neuron2.equals(neuron1));
        //not equal to null
        Assert.assertNotNull(neuron2);

        Assert.assertNotSame(neuron1, neuron2);
        Assert.assertEquals(neuron1.hashCode(), neuron2.hashCode());

    }

    private static final String NEURON_JSON = "[" +
            "{" +
            "\"id\": 1," +
            "\"status\": \"testStatus\"," +
            "\"name\": \"testName1\"," +
            "\"type\": \"testType1\"," +
            "\"instance\": \"testInstance1\"," +
            "\"primaryNeurite\": \"testPrimaryNeurite1\"," +
            "\"majorInput\": \"testMajorInput1\"," +
            "\"majorOutput\": \"testMajorOutput1\"," +
            "\"clonalUnit\": \"testClonalUnit1\"," +
            "\"neurotransmitter\": \"testNeurotransmitter1\"," +
            "\"property\": \"testProperty1\"," +
            "\"size\": 808080," +
            "\"rois\": [\"roiA\",\"roiB\"]," +
            "\"soma\": {\"location\":[8,8,8], \"radius\":5.0}," +
            "\"synapseSet\": [[1,2,3],[4,5,6]]" +
            "}," +
            "{" +
            "\"id\": 2," +
            "\"status\": \"testStatus\"," +
            "\"name\": \"testName2\"," +
            "\"type\": \"testType1\"," +
            "\"instance\": \"testInstance2\"," +
            "\"size\": 8080," +
            "\"rois\": [\"roiA\",\"roiC\"]," +
            "\"soma\": {\"location\":[9,9,9], \"radius\":5.0}," +
            "\"synapseSet\": [[7,8,9],[10,11,12]]" +
            "}," +
            "{" +
            "\"id\": 3," +
            "\"status\": \"testStatus\"," +
            "\"name\": \"testName3\"," +
            "\"type\": \"testType2\"," +
            "\"instance\": \"testInstance1\"," +
            "\"size\": 80," +
            "\"rois\": [\"roiA\",\"roiD\"]," +
            "\"soma\": {\"location\":[10,10,10], \"radius\":5.0}," +
            "\"synapseSet\": [[13,14,15],[16,17,18]]" +
            "}" +
            "]";

}
