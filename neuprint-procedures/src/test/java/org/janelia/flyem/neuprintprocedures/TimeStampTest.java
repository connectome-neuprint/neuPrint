package org.janelia.flyem.neuprintprocedures;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.time.LocalDate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.neo4j.driver.v1.Values.parameters;

public class TimeStampTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(TimeStampProcedure.class);

    @Test
    public void shouldApplyTimeStampAfterCall() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Long nodeId = session.run("CREATE (n:Neuron{id:1}) RETURN id(n)").single().get(0).asLong();

            session.run("CALL connprocedures.timeStamp($nodeId)", parameters("nodeId", nodeId));

            LocalDate timeStamp = session.run("MATCH (n:Neuron{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }

    @Test
    public void shouldApplyTimeStampUponNodeCreation() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Long nodeId = session.run("CREATE (n:Neuron{id:1}) RETURN id(n)").single().get(0).asLong();

            session.run("CALL connprocedures.timeStamp($nodeId)", parameters("nodeId", nodeId));

            LocalDate timeStamp = session.run("MATCH (n:Neuron{id:1}) RETURN n.timeStamp").single().get(0).asLocalDate();

            Assert.assertEquals(LocalDate.now(), timeStamp);

        }
    }


    @Test
    public void showSimpleEventHandling() {
        GraphDatabaseService database
                = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        database.registerTransactionEventHandler(new MyTransactionEventHandler(database, executorService));

        Long createdNodeId = null;
        LocalDate nodeTimeStamp = null;

        try (Transaction tx = database.beginTx()) {
            Node createdNode = database.createNode();
            createdNodeId = createdNode.getId();
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            nodeTimeStamp = (LocalDate) database.getNodeById(createdNodeId).getProperty("timeStamp");
        }

        Assert.assertEquals(LocalDate.now(), nodeTimeStamp);

    }


}

