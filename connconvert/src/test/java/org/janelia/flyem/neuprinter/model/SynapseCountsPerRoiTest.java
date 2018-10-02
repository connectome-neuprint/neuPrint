package org.janelia.flyem.neuprinter.model;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests the {@link SynapseCountsPerRoi} class.
 */
public class SynapseCountsPerRoiTest {

    @Test
    public void shouldIncrementPrePostAndTotalAppropriately() {

        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();

        Assert.assertEquals(0, synapseCountsPerRoi.getSynapseCountsPerRoi().size());

        synapseCountsPerRoi.incrementPreForRoi("testRoi");

        Assert.assertEquals(1, synapseCountsPerRoi.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(0, synapseCountsPerRoi.getSynapseCountsForRoi("testRoi").getPost());

        synapseCountsPerRoi.incrementPostForRoi("testRoi");

        Assert.assertEquals(1, synapseCountsPerRoi.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, synapseCountsPerRoi.getSynapseCountsForRoi("testRoi").getPost());
    }

    @Test
    public void shouldProduceCorrectJsonString() {

        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();

        synapseCountsPerRoi.incrementPreForRoi("testRoi");
        synapseCountsPerRoi.incrementPostForRoi("testRoi");

        String synapseCountsPerRoiJson = synapseCountsPerRoi.getAsJsonString();

        Gson gson = new Gson();
        Map<String, SynapseCounter> deserializedSynapseCountsPerRoi = gson.fromJson(synapseCountsPerRoiJson, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertTrue(deserializedSynapseCountsPerRoi.containsKey("testRoi"));

    }

    @Test
    public void shouldStoreRoisInLexicographicOrder() {

        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();

        synapseCountsPerRoi.incrementPreForRoi("roiA");
        synapseCountsPerRoi.incrementPreForRoi("roiB");
        synapseCountsPerRoi.incrementPreForRoi("RoiA");

        Set<String> roiSet = new TreeSet<>();
        roiSet.add("roiA");
        roiSet.add("roiB");
        roiSet.add("RoiA");

        Assert.assertEquals(roiSet, synapseCountsPerRoi.getSetOfRois());

    }

}
