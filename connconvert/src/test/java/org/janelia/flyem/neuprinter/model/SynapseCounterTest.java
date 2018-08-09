package org.janelia.flyem.neuprinter.model;

import org.junit.Assert;
import org.junit.Test;

public class SynapseCounterTest {

    @Test
    public void shouldIncrementPrePostAndTotalAppropriately() {

        SynapseCounter synapseCounter = new SynapseCounter();

        Assert.assertEquals(0,synapseCounter.getTotal()+synapseCounter.getPre()+synapseCounter.getPost());

        synapseCounter.incrementPre();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(0, synapseCounter.getPost());
        Assert.assertEquals(1, synapseCounter.getTotal());


        synapseCounter.incrementPost();

        Assert.assertEquals(1, synapseCounter.getPre());
        Assert.assertEquals(1, synapseCounter.getPost());
        Assert.assertEquals(2, synapseCounter.getTotal());

    }
}
