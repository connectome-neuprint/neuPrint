package org.janelia.flyem.neuprint.model;

import org.janelia.flyem.neuprint.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.janelia.flyem.neuprint.json.JsonUtils.arrayToPrettyFormat;

public class SynapticConnectionTest {

    @Test
    public void testJsonProcessing() {

        final List<SynapticConnection> parsedList = SynapticConnection.fromJsonArray(CONNECTIONS_JSON);

        Assert.assertEquals("invalid number of connections parsed",
                3, parsedList.size());

        final SynapticConnection firstConnection = parsedList.get(0);

        Assert.assertEquals(new Location(1L, 2L, 3L), firstConnection.getPreLocation());
        Assert.assertEquals(new Location(5L, 6L, 7L), firstConnection.getPostLocation());

        final String serializedJson = JsonUtils.GSON.toJson(parsedList);

        Assert.assertEquals("serialized result does not match original",
                arrayToPrettyFormat(CONNECTIONS_JSON), serializedJson);
    }

    @Test
    public void testEqualsAndHashCode() {

        final List<SynapticConnection> parsedList = SynapticConnection.fromJsonArray(CONNECTIONS_JSON);

        SynapticConnection x = parsedList.get(0);

        SynapticConnection synapticConnection1 = new SynapticConnection(x.getPreLocation(), x.getPostLocation());
        SynapticConnection synapticConnection2 = new SynapticConnection(x.getPreLocation(), x.getPostLocation());
        SynapticConnection synapticConnection3 = new SynapticConnection(x.getPreLocation(), x.getPostLocation());

        //reflexive
        Assert.assertEquals(synapticConnection1, synapticConnection1);
        //symmetric
        Assert.assertTrue(synapticConnection1.equals(synapticConnection2) && synapticConnection2.equals(synapticConnection1));
        //transitive
        Assert.assertTrue(synapticConnection1.equals(synapticConnection2) && synapticConnection2.equals(synapticConnection3) && synapticConnection3.equals(synapticConnection1));
        //consistent
        Assert.assertTrue(synapticConnection2.equals(synapticConnection1) && synapticConnection2.equals(synapticConnection1));
        //not equal to null
        Assert.assertNotNull(synapticConnection2);

        Assert.assertNotSame(synapticConnection1, synapticConnection2);
        Assert.assertEquals(synapticConnection1.hashCode(), synapticConnection2.hashCode());

    }

    private static final String CONNECTIONS_JSON = "[" +
            "{\"pre\":[1,2,3], \"post\": [5,6,7]}," +
            "{\"pre\":[9,10,11], \"post\": [9,10,11]}," + // can connect to same location
            "{\"pre\":[90,100,110], \"post\": [90,100,110]}" +
            "]";
}
