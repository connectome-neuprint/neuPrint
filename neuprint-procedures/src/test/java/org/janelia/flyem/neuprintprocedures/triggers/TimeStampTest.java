package org.janelia.flyem.neuprintprocedures.triggers;

import apoc.convert.Json;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.neo4j.driver.v1.Values.parameters;

public class TimeStampTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(TimeStampProcedure.class);
    }

    @BeforeClass
    public static void before() {
        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());
    }

    @Test
    public void shouldAddTimeStampUponCreation() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n:Segment:test{bodyId:1}) RETURN n");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n:Segment{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponAddLabel() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:2}) RETURN n");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:2}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:2}) SET n:test");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n:test{bodyId:2}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponRemoveLabel() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n:test{bodyId:3}) RETURN n");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n:test{bodyId:3}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:3}) REMOVE n:test");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:3}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponPropertiesAssigned() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:4}) RETURN n");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:4}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:4}) SET n.testProperty=\"testValue\"");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:4}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponPropertiesRemoved() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:5}) RETURN id(n)");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:5}) SET n.testProperty=\"testValue\"");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:5}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:5}) REMOVE n.testProperty");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:5}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponRelationshipPropertiesAssigned() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:6})  \n" +
                    "CREATE (m{bodyId:7}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n)-[r:RelatesTo]->() SET r.testProperty=\"testValue\"");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:6}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

        LocalDateTime timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:7}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp2.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponRelationshipPropertiesRemoved() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:8})  \n" +
                    "CREATE (m{bodyId:9}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n)-[r:RelatesTo]-() SET r.testProperty=\"testValue\"");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n)-[r:RelatesTo]->() REMOVE r.testProperty");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

        LocalDateTime timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:9}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp2.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponRelationshipCreated() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:10})  \n" +
                    "CREATE (m{bodyId:11})");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:10}) \n" +
                    "MATCH (m{bodyId:11}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n)-[r:RelatesTo]->() REMOVE r.testProperty");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:10}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

        LocalDateTime timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:11}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp2.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldAddTimeStampUponRelationshipDeleted() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:12})  \n" +
                    "CREATE (m{bodyId:13}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n{bodyId:12})  \n" +
                    "MATCH (m{bodyId:13}) \n" +
                    "MATCH (n)-[r:RelatesTo]->(m) \n" +
                    "DELETE r");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n)-[r:RelatesTo]->() REMOVE r.testProperty");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:12}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp.truncatedTo(ChronoUnit.HOURS));

        LocalDateTime timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:13}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), timeStamp2.truncatedTo(ChronoUnit.HOURS));

    }

    @Test
    public void shouldNotAddTimeStampIfReadOnlyQuery() throws InterruptedException {

        Session session = driver.session();

        session.writeTransaction(tx -> {
            tx.run("CREATE (n{bodyId:14})  \n" +
                    "CREATE (m{bodyId:15}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");
            return 1;
        });

        session.writeTransaction(tx -> {
            tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDateTime.of(2000, 1, 1, 1, 1)));
            return 1;
        });

        session.readTransaction(tx -> tx.run("MATCH (n{bodyId:14}) RETURN n"));

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        LocalDateTime timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:15}) RETURN n.timeStamp").single().get(0).asLocalDateTime());

        Assert.assertEquals(LocalDateTime.of(2000, 1, 1, 1, 1), timeStamp);

    }
}

