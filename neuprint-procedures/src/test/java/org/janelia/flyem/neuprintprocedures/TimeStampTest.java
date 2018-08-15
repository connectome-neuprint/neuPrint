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

            session.run("CREATE (n:Neuron:test{id:1}) RETURN id(n)");

            LocalDate timeStamp = session.run("MATCH (n:Neuron{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);
        }

    }

    @Test
    public void shouldAddTimeStampUponAddLabel() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1}) RETURN id(n)");

            session.run("MATCH (n{id:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n{id:1}) SET n:test");

            LocalDate timeStamp = session.run("MATCH (n:test{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);
        }

    }

    @Test
    public void shouldAddTimeStampUponRemoveLabel() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n:test{id:1}) RETURN id(n)");

            session.run("MATCH (n:test{id:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n{id:1}) REMOVE n:test");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);
        }

    }

    @Test
    public void shouldAddTimeStampUponPropertiesAssigned() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1}) RETURN id(n)");

            session.run("MATCH (n{id:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n{id:1}) SET n.testProperty=\"testValue\"");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }

    @Test
    public void shouldAddTimeStampUponPropertiesRemoved() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1}) RETURN id(n)");

            session.run("MATCH (n{id:1}) SET n.testProperty=\"testValue\"");

            session.run("MATCH (n{id:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n{id:1}) REMOVE n.testProperty");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }

    @Test
    public void shouldAddTimeStampUponRelationshipPropertiesAssigned() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1})  \n" +
                    "CREATE (m{id:2}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");

            session.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n)-[r:RelatesTo]->() SET r.testProperty=\"testValue\"");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.run("MATCH (n{id:2}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldAddTimeStampUponRelationshipPropertiesRemoved() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1})  \n" +
                    "CREATE (m{id:2}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");

            session.run("MATCH (n)-[r:RelatesTo]-() SET r.testProperty=\"testValue\"");

            session.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n)-[r:RelatesTo]->() REMOVE r.testProperty");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.run("MATCH (n{id:2}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldAddTimeStampUponRelationshipCreated() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1})  \n" +
                    "CREATE (m{id:2})");

            session.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n{id:1}) \n" +
                    "MATCH (m{id:2}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.run("MATCH (n{id:2}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldAddTimeStampUponRelationshipDeleted() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1})  \n" +
                    "CREATE (m{id:2}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");

            session.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n{id:1})  \n" +
                    "MATCH (m{id:2}) \n" +
                    "MATCH (n)-[r:RelatesTo]->(m) \n" +
                    "DELETE r");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

            LocalDate timeStamp2 = session.run("MATCH (n{id:2}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp2);

        }

    }

    @Test
    public void shouldNotAddTimeStampIfReadOnlyQuery() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n{id:1})  \n" +
                    "CREATE (m{id:2}) \n" +
                    "CREATE (n)-[:RelatesTo]->(m)");

            session.run("MATCH (n) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("MATCH (n{id:1}) RETURN n");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.of(2000, 1, 1), timeStamp);

        }
    }
}

