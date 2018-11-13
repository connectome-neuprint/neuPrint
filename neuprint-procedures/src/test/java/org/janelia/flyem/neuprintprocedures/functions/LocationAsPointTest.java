package org.janelia.flyem.neuprintprocedures.functions;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;
import org.neo4j.harness.junit.Neo4jRule;

public class LocationAsPointTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(NeuPrintUserFunctions.class);
    }

    @BeforeClass
    public static void before() {
        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());
    }

    @Test
    public void shouldReturnLocationAsPoint() {

        Session session = driver.session();

        Point point = session.readTransaction(tx -> tx.run("WITH neuprint.locationAs3dCartPoint(1,2,3) AS loc RETURN loc")).single().get(0).asPoint();

        Assert.assertEquals(Values.point(9157, 1, 2, 3).asPoint(), point);

    }
}
