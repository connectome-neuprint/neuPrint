package org.janelia.flyem.neuprint.json;

import org.janelia.flyem.neuprint.model.Location;
import org.junit.Assert;
import org.junit.Test;

import static org.janelia.flyem.neuprint.json.JsonUtils.GSON;
import static org.janelia.flyem.neuprint.json.JsonUtils.arrayToPrettyFormat;

public class LocationJsonTest {

    @Test
    public void shouldDeserializeAndSerializeLocation() {

        String testLocationString = "[1,2,3]";

        Location testLocation = GSON.fromJson(testLocationString, Location.class);
        Assert.assertEquals(1L, (long) testLocation.getX());
        Assert.assertEquals(2L, (long) testLocation.getY());
        Assert.assertEquals(3L, (long) testLocation.getZ());

        String jsonString = GSON.toJson(testLocation);

        Assert.assertEquals(arrayToPrettyFormat(testLocationString), jsonString);

    }

    @Test(expected = com.google.gson.JsonParseException.class)
    public void shouldErrorIfLocationsAreLetters() {

        GSON.fromJson("[1,a,3]", Location.class);

    }

    @Test(expected = com.google.gson.JsonParseException.class)
    public void shouldErrorIfIncorrectNumberOfCoordinates() {

        GSON.fromJson("[1,2]", Location.class);

    }

    @Test(expected = com.google.gson.JsonParseException.class)
    public void shouldErrorIfLocationsAreNotIntegerValues() {

        GSON.fromJson("[1.445,2,3]", Location.class);

    }
}
