package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonParseException;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.junit.Assert;
import org.junit.Test;

import static org.janelia.flyem.neuprint.json.JsonUtils.GSON;
import static org.janelia.flyem.neuprint.json.JsonUtils.objectToPrettyFormat;

public class ConnectionsJsonTest {

    @Test
    public void shouldDeserializeAndSerializeConnection() {

        String testConnectionString = "{\"pre\":[1,2,3], \"post\": [5,6,7]}";

        SynapticConnection synapticConnection = GSON.fromJson(testConnectionString, SynapticConnection.class);

        Assert.assertEquals(new Location(1L, 2L, 3L), synapticConnection.getPreLocation());
        Assert.assertEquals(new Location(5L, 6L, 7L), synapticConnection.getPostLocation());

        String resultJson = GSON.toJson(synapticConnection);

        Assert.assertEquals(objectToPrettyFormat(testConnectionString), resultJson);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfPreMissing() {
        String testConnectionString = "{ \"post\": [5,6,7]}";
        GSON.fromJson(testConnectionString, SynapticConnection.class);
    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfPostMissing() {
        String testConnectionString = "{ \"pre\": [5,6,7]}";
        GSON.fromJson(testConnectionString, SynapticConnection.class);
    }

}
