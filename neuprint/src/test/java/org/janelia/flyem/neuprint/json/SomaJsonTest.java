package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonParseException;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.Soma;
import org.junit.Assert;
import org.junit.Test;

import static org.janelia.flyem.neuprint.json.JsonUtils.GSON;
import static org.janelia.flyem.neuprint.json.JsonUtils.objectToPrettyFormat;

public class SomaJsonTest {

    @Test
    public void shouldDeserializeAndSerializeSoma() {

        String testSomaString = "{\"location\":[1,2,3], \"radius\":5.0}";

        Soma testSoma = GSON.fromJson(testSomaString, Soma.class);

        Assert.assertEquals(new Location(1L, 2L, 3L), testSoma.getLocation());
        Assert.assertEquals(5.0, testSoma.getRadius(), .0001);

        String jsonString = GSON.toJson(testSoma);

        Assert.assertEquals(objectToPrettyFormat(testSomaString), jsonString);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfLocationIsMissing() {

        String testSomaString = "{ \"radius\":5.0}";

        GSON.fromJson(testSomaString, Soma.class);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfRadiusIsMissing() {

        String testSomaString = "{ \"location\":[1,2,3]}";

        GSON.fromJson(testSomaString, Soma.class);

    }

    @Test(expected = JsonParseException.class)
    public void shouldErrorIfRadiusIsNotNumber() {

        String testSomaString = "{\"location\":[1,2,3], \"radius\":\"A\"}";
        GSON.fromJson(testSomaString, Soma.class);

    }

}
