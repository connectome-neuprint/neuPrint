package org.janelia.flyem.neuprinter.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link ConnectionSet} class.
 */
public class ConnectionSetTest {

    @Test
    public void testEqualsAndHashCode() {

        ConnectionSet connectionSet1 = new ConnectionSet(1, 2);
        ConnectionSet connectionSet2 = new ConnectionSet(1, 2);
        ConnectionSet connectionSet3 = new ConnectionSet(1, 2);

        //reflexive
        Assert.assertTrue(connectionSet1.equals(connectionSet1));
        //symmetric
        Assert.assertTrue(connectionSet1.equals(connectionSet3) && connectionSet3.equals(connectionSet1));
        //transitive
        Assert.assertTrue(connectionSet1.equals(connectionSet2) && connectionSet2.equals(connectionSet3) && connectionSet3.equals(connectionSet1));
        //consistent
        Assert.assertTrue(connectionSet2.equals(connectionSet1) && connectionSet2.equals(connectionSet1));
        //not equal to null
        Assert.assertTrue(!connectionSet1.equals(null));

        Assert.assertNotSame(connectionSet1, connectionSet2);
        Assert.assertTrue(connectionSet1.hashCode() == connectionSet2.hashCode());

    }

    @Test
    public void testAddConnection() {

        ConnectionSet connectionSet1 = new ConnectionSet(1, 2);

        connectionSet1.addPreAndPostsynapticLocations("pre1", "post1");
        connectionSet1.addPreAndPostsynapticLocations("pre2", "post2");

        Assert.assertTrue(connectionSet1.contains("pre1") && connectionSet1.contains("post1") && connectionSet1.contains("pre2") && connectionSet1.contains("post2"));

    }

}
