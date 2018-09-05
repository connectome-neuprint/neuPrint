package org.janelia.flyem.neuprintprocedures;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.time.LocalDate;

import static org.neo4j.driver.v1.Values.parameters;

public class TimeStampTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(TimeStampProcedure.class);

    @Test
    public void shouldAddTimeStampUponCreation() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n:Neuron:test{bodyId:1}) RETURN n");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n:Neuron{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);
        }

    }

    @Test
    public void shouldAddTimeStampUponAddLabel() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1}) RETURN n");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) SET n:test");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n:test{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);
        }

    }

    @Test
    public void shouldAddTimeStampUponRemoveLabel() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n:test{bodyId:1}) RETURN n");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n:test{bodyId:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) REMOVE n:test");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);
        }

    }

    @Test
    public void shouldAddTimeStampUponPropertiesAssigned() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1}) RETURN n");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) SET n.testProperty=\"testValue\"");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }

    @Test
    public void shouldAddTimeStampUponPropertiesRemoved() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1}) RETURN id(n)");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) SET n.testProperty=\"testValue\"");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) REMOVE n.testProperty");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }

    @Test
    public void shouldAddTimeStampUponRelationshipPropertiesAssigned() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1})  \n" +
                        "CREATE (m{bodyId:2}) \n" +
                        "CREATE (n)-[:RelatesTo]->(m)");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n)-[r:RelatesTo]->() SET r.testProperty=\"testValue\"");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:2}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldAddTimeStampUponRelationshipPropertiesRemoved() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1})  \n" +
                        "CREATE (m{bodyId:2}) \n" +
                        "CREATE (n)-[:RelatesTo]->(m)");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n)-[r:RelatesTo]-() SET r.testProperty=\"testValue\"");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n)-[r:RelatesTo]->() REMOVE r.testProperty");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:2}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldAddTimeStampUponRelationshipCreated() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1})  \n" +
                        "CREATE (m{bodyId:2})");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1}) \n" +
                        "MATCH (m{bodyId:2}) \n" +
                        "CREATE (n)-[:RelatesTo]->(m)");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n)-[r:RelatesTo]->() REMOVE r.testProperty");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:2}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldAddTimeStampUponRelationshipDeleted() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1})  \n" +
                        "CREATE (m{bodyId:2}) \n" +
                        "CREATE (n)-[:RelatesTo]->(m)");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n{bodyId:1})  \n" +
                        "MATCH (m{bodyId:2}) \n" +
                        "MATCH (n)-[r:RelatesTo]->(m) \n" +
                        "DELETE r");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n)-[r:RelatesTo]->() REMOVE r.testProperty");
                return 1;
            });

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:2}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldNotAddTimeStampIfReadOnlyQuery() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.writeTransaction(tx -> {
                tx.run("CREATE (n{bodyId:1})  \n" +
                        "CREATE (m{bodyId:2}) \n" +
                        "CREATE (n)-[:RelatesTo]->(m)");
                return 1;
            });

            session.writeTransaction(tx -> {
                tx.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));
                return 1;
            });

            session.readTransaction(tx -> tx.run("MATCH (n{bodyId:1}) RETURN n"));

            LocalDate timeStamp = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:2}) RETURN n.timeStamp").single().get(0).asLocalDate());

            Assert.assertEquals(LocalDate.of(2000, 1, 1), timeStamp);

        }
    }
}

