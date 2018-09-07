package org.janelia.flyem.neuprinter.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link SynapseCounter} class.
 */
public class SynapseCounterTest {

    @Test
    public void shouldIncrementPrePostAndTotalAppropriately() {

        SynapseCounter synapseCounter = new SynapseCounter();

        Assert.assertEquals(0,synapseCounter.getPre()+synapseCounter.getPost());

        synapseCounter.incrementPre();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(0, synapseCounter.getPost());

        synapseCounter.incrementPost();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(1, synapseCounter.getPost());

    }
}
