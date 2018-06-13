package org.janelia.flyem.neuprintprocedures;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.time.LocalDate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.graphdb.Label.label;

public class TimeStampTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(TimeStampProcedure.class);

    @Test
    public void shouldApplyTimeStampAfterCall() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Long nodeId = session.run("CREATE (n:Neuron:test{id:1}) RETURN id(n)").single().get(0).asLong();

            session.run("MATCH (n:Neuron{id:1}) SET n.timeStamp=$yesterday", parameters("yesterday", LocalDate.of(2000, 1, 1)));

            session.run("CALL neuPrintProcedures.timeStamp($nodeId)", parameters("nodeId", nodeId));

            LocalDate timeStamp = session.run("MATCH (n:Neuron{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }


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

            session.run("MATCH (n{id:1}) SET n.timeStamp=$yesterday",parameters("yesterday",LocalDate.of(2000,1,1)));

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

            session.run("MATCH (n:test{id:1}) SET n.timeStamp=$yesterday",parameters("yesterday",LocalDate.of(2000,1,1)));

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

            session.run("MATCH (n{id:1}) SET n.timeStamp=$yesterday",parameters("yesterday",LocalDate.of(2000,1,1)));

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

            session.run("MATCH (n{id:1}) SET n.timeStamp=$yesterday",parameters("yesterday",LocalDate.of(2000,1,1)));

            session.run("MATCH (n{id:1}) REMOVE n.testProperty");

            LocalDate timeStamp = session.run("MATCH (n{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }
}

