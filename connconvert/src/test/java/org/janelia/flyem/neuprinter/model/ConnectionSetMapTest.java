package org.janelia.flyem.neuprinter.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link ConnectionSetMap} class.
 */
public class ConnectionSetMapTest {

    @Test
    public void testAddConnection() {

        ConnectionSetMap connectionSetMap = new ConnectionSetMap();

        connectionSetMap.addConnection(1, 2, "pre1", "post1");
        connectionSetMap.addConnection(1, 2, "pre2", "post2");
        connectionSetMap.addConnection(2, 1, "pre3", "post3");

        Assert.assertTrue(connectionSetMap.getConnectionSetKeys().contains("1:2") &&
                connectionSetMap.getConnectionSetKeys().contains("2:1"));
        Assert.assertTrue(connectionSetMap.getConnectionSetForKey("1:2").contains("pre1") &&
                connectionSetMap.getConnectionSetForKey("1:2").contains("pre2") &&
                connectionSetMap.getConnectionSetForKey("1:2").contains("post1") &&
                connectionSetMap.getConnectionSetForKey("1:2").contains("post2"));
        Assert.assertTrue(connectionSetMap.getConnectionSetForKey("2:1").contains("pre3") &&
                connectionSetMap.getConnectionSetForKey("2:1").contains("post3"));
        Assert.assertEquals(2, connectionSetMap.getConnectionSetForKey("2:1").size());

    }

}
