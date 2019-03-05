package org.janelia.flyem.neuprintloadprocedures.model;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests the {@link RoiInfoWithHighPrecisionCounts} class.
 */
public class RoiInfoWithHighPrecisionCountsTest {

    @Test
    public void shouldIncrementAndDecrementPrePostAndTotalAppropriately() {

        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts();

        Assert.assertEquals(0, roiInfo.getSynapseCountsPerRoi().size());

        roiInfo.incrementPreForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPost());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPreHP());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPostHP());

        roiInfo.incrementPostForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPreHP());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPostHP());

        roiInfo.incrementPreHPForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPreHP());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPostHP());

        roiInfo.incrementPostHPForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPreHP());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPostHP());

        roiInfo.decrementPreHPForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPreHP());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPostHP());

        roiInfo.decrementPostHPForRoi("testRoi");

        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPreHP());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPostHP());

        roiInfo.decrementPreForRoi("testRoi");

        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPre());
        Assert.assertEquals(1, roiInfo.getSynapseCountsForRoi("testRoi").getPost());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPreHP());
        Assert.assertEquals(0, roiInfo.getSynapseCountsForRoi("testRoi").getPostHP());

        roiInfo.decrementPostForRoi("testRoi");

        Assert.assertEquals(0, roiInfo.getSetOfRois().size());


    }

    @Test
    public void shouldProduceCorrectJsonString() {

        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts();

        roiInfo.incrementPreForRoi("testRoi");
        roiInfo.incrementPostForRoi("testRoi");
        roiInfo.incrementPreHPForRoi("testRoi");
        roiInfo.incrementPostHPForRoi("testRoi");

        String synapseCountsPerRoiJson = roiInfo.getAsJsonString();

        Gson gson = new Gson();
        Map<String, SynapseCounterWithHighPrecisionCounts> deserializedSynapseCountsPerRoi = gson.fromJson(synapseCountsPerRoiJson, new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
        }.getType());

        Assert.assertTrue(deserializedSynapseCountsPerRoi.containsKey("testRoi"));
        Assert.assertEquals("{\"testRoi\":{\"preHP\":1,\"postHP\":1,\"pre\":1,\"post\":1}}", synapseCountsPerRoiJson);
    }

    @Test
    public void shouldStoreRoisInLexicographicOrder() {

        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts();

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
