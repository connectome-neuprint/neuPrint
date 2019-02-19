package org.janelia.flyem.neuprinter.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link SynapseCounterWithHighPrecisionCounts} class.
 */
public class SynapseCounterWithHighPrecisionCountsTest {

    @Test
    public void shouldIncrementPrePostAndTotalAppropriately() {

        SynapseCounterWithHighPrecisionCounts synapseCounter = new SynapseCounterWithHighPrecisionCounts();

        Assert.assertEquals(0,synapseCounter.getPre()+synapseCounter.getPost()+synapseCounter.getPreHP()+synapseCounter.getPostHP());

        synapseCounter.incrementPre();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(0, synapseCounter.getPost());
        Assert.assertEquals(0, synapseCounter.getPreHP());
        Assert.assertEquals(0, synapseCounter.getPostHP());

        synapseCounter.incrementPost();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(1, synapseCounter.getPost());
        Assert.assertEquals(0, synapseCounter.getPreHP());
        Assert.assertEquals(0, synapseCounter.getPostHP());

        synapseCounter.incrementPreHP();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(1, synapseCounter.getPost());
        Assert.assertEquals(1, synapseCounter.getPreHP());
        Assert.assertEquals(0, synapseCounter.getPostHP());

        synapseCounter.incrementPostHP();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(1, synapseCounter.getPost());
        Assert.assertEquals(1, synapseCounter.getPreHP());
        Assert.assertEquals(1, synapseCounter.getPostHP());

    }
}
