package org.janelia.flyem.neuprint.model;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests the {@link RoiInfo} class.
 */
public class RoiInfoTest {

    @Test
    public void shouldIncrementAndDecrementPrePostAndTotalAppropriately() {

        RoiInfo roiInfo = new RoiInfo();

        Assert.assertEquals(0, roiInfo.getSynapseCountsPerRoi().size());

        roiInfo.incrementPreForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPost());

        roiInfo.incrementPostForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());

        roiInfo.decrementPreForRoi("testRoi");

        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());

        roiInfo.decrementPostForRoi("testRoi");

        Assert.assertEquals(0, roiInfo.getSetOfRois().size());
    }

    @Test
    public void shouldProduceCorrectJsonString() {

        RoiInfo roiInfo = new RoiInfo();

        roiInfo.incrementPreForRoi("testRoi");
        roiInfo.incrementPostForRoi("testRoi");

        String synapseCountsPerRoiJson = roiInfo.getAsJsonString();

        Gson gson = new Gson();
        Map<String, SynapseCounter> deserializedSynapseCountsPerRoi = gson.fromJson(synapseCountsPerRoiJson, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertTrue(deserializedSynapseCountsPerRoi.containsKey("testRoi"));

    }

    @Test
    public void shouldStoreRoisInLexicographicOrder() {

        RoiInfo roiInfo = new RoiInfo();

        roiInfo.incrementPreForRoi("roiA");
        roiInfo.incrementPreForRoi("roiB");
        roiInfo.incrementPreForRoi("RoiA");

        Set<String> roiSet = new TreeSet<>();
        roiSet.add("roiA");
        roiSet.add("roiB");
        roiSet.add("RoiA");

        Assert.assertEquals(roiSet, roiInfo.getSetOfRois());

    }

}
