package org.janelia.flyem.connconvert.model;

import java.io.File;
import java.util.List;

import org.janelia.scicomp.neotool.SynapseMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link Body} class.
 */
public class SynapseMapperTest {

    @Test
    public void testMapBodies() throws Exception {

        final File bodyJsonFile = new File("src/test/resources/smallBodyList.json");

        final SynapseMapper mapper = new SynapseMapper();
        final List<Body> parsedBodyList = mapper.loadAndMapBodies(bodyJsonFile);

        Assert.assertEquals("invalid number of bodies parsed",
                            4, parsedBodyList.size());

        int totalSynapseCount = 0;
        for (final Body body : parsedBodyList) {
            final int numberOfConnections = body.getTotalNumberOfPreSynapticTerminals() +
                                            body.getTotalNumberOfPostSynapticTerminals();
            Assert.assertTrue("no connections for body " + body,
                              numberOfConnections > 0);
            totalSynapseCount += body.getSynapseSet().size();
        }

        Assert.assertEquals("invalid number of synapses parsed", 5, totalSynapseCount);

        // TODO: improve this test
    }


}
