package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.convert.Json;
import apoc.create.Create;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrintMain;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;
import org.janelia.flyem.neuprintprocedures.functions.NeuPrintUserFunctions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class RemovePropertiesTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(Create.class)
                .withProcedure(LoadingProcedures.class)
                .withProcedure(ProofreaderProcedures.class)
                .withFunction(NeuPrintUserFunctions.class);
    }

    @BeforeClass
    public static void before() {

        final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        File swcFile1 = new File("src/test/resources/8426959.swc");
        File swcFile2 = new File("src/test/resources/831744.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrintMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

        String neuronsJsonPath = "src/test/resources/neuronList.json";
        List<Neuron> neuronList = NeuPrintMain.readNeuronsJson(neuronsJsonPath);

        String synapseJsonPath = "src/test/resources/synapseList.json";
        List<Synapse> synapseList = NeuPrintMain.readSynapsesJson(synapseJsonPath);

        String connectionsJsonPath = "src/test/resources/connectionsList.json";
        List<SynapticConnection> connectionsList = NeuPrintMain.readConnectionsJson(connectionsJsonPath);

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());

        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

        String dataset = "test";

        NeuPrintMain.runStandardLoadWithoutMetaInfo(neo4jImporter, dataset, synapseList, connectionsList, neuronList, skeletonList, 1.0F, .2D, .8D,5, true, true, timeStamp);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldRemoveSomaRadiusAndLocation() {
        testPropertyRemoval("deleteSoma", 100569, new String[] {"somaRadius", "somaLocation"}, 1);
    }

    @Test
    public void shouldRemoveName() {
        testPropertyRemoval("deleteName", 100554, new String[] {"name"},1);
    }

    @Test
    public void shouldRemoveInstance() {
        testPropertyRemoval("deleteInstance", 100555, new String[] {"instance"}, 1);

        // body 100556 only has an instance property (no status), so removing instance should also remove neuron status
        testPropertyRemoval("deleteInstance", 100556, new String[] {"instance"},0);
    }

    @Test
    public void shouldRemoveStatus() {
        testPropertyRemoval("deleteStatus", 100541, new String[] {"status"}, 0);
    }

    @Test
    public void shouldRemoveType() {
        testPropertyRemoval("deleteType", 8426959, new String[] {"type"}, 1);
    }

    @Test
    public void deleteDuplicateContainsRelationships(){
        Session session = driver.session();

        session.writeTransaction(tx -> tx.run("MERGE (cs:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:26311\"}) \n" +
                "MERGE (s:Synapse{location:point({ x:4301, y:2276, z:1535 })}) \n" +
                "CREATE (cs)-[:Contains]->(s)" ));

        session.writeTransaction(tx -> tx.run("MERGE (cs:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:26311\"}) \n" +
                "MERGE (s:Synapse{location:point({ x:4301, y:2276, z:1535 })}) \n" +
                "CREATE (cs)-[:Contains]->(s)" ));

        int cCount  =session.readTransaction(tx -> tx.run("MATCH (cs:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:26311\"})-[c:Contains]->(s:Synapse{location:point({ x:4301, y:2276, z:1535 })}) RETURN count(c)")).single().get(0).asInt();

        Assert.assertEquals(3, cCount);

        session.writeTransaction(tx -> tx.run("MATCH (cs:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:26311\"}) WITH cs CALL temp.removeDuplicateContainsRelForConnectionSet(cs) RETURN cs.datasetBodyIds" ));

        int cCount2  =session.readTransaction(tx -> tx.run("MATCH (cs:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:26311\"})-[c:Contains]->(s:Synapse{location:point({ x:4301, y:2276, z:1535 })}) RETURN count(c)")).single().get(0).asInt();

        Assert.assertEquals(1, cCount2);

    }

    private void testPropertyRemoval(final String procedureName,
                                     final int testBodyId,
                                     final String[] propertyNames,
                                     final int expectedNeuronCount) throws RuntimeException {

        final String procedureContext = "calling " + procedureName + " with bodyId " + testBodyId;
        final String whereBodyClause = "WHERE n.bodyId=" + testBodyId;
        final String statement = "CALL proofreader." + procedureName + "($bodyId,$datasetLabel)";
        final Value bodyIdAndDatasetValues = parameters("bodyId", testBodyId, "datasetLabel", "test");

        // get neuron and add Neuron label
        final Session session = driver.session();
        Map<String, Object> neuronAsMap = session.readTransaction(tx -> tx.run("MATCH (n:Segment) " + whereBodyClause +
                                                                               " SET n:Neuron, n:`test-Neuron` RETURN n")).single().get(0).asMap();

        for (final String propertyName : propertyNames) {
            Assert.assertTrue(propertyName + " property does not exist before " + procedureContext,
                              neuronAsMap.containsKey(propertyName));
        }

        session.writeTransaction(tx -> tx.run(statement, bodyIdAndDatasetValues));

        final Map<String, Object> segmentsAsMapAfterDelete = session.readTransaction(tx -> tx.run(
                "MATCH (n:Segment) " + whereBodyClause + " RETURN n")).single().get(0).asMap();

        for (final String propertyName : propertyNames) {
            Assert.assertFalse(propertyName + " property still exists after " + procedureContext,
                               segmentsAsMapAfterDelete.containsKey(propertyName));
        }

        // check neuron status after removal ...
        int numNeuronsWithBodyId = session.readTransaction(tx -> tx.run(
                "MATCH (n:Neuron) " + whereBodyClause + " RETURN count(n)")).single().get(0).asInt();
        int numDatasetNeuronsWithBodyId = session.readTransaction(tx -> tx.run(
                "MATCH (n:`test-Neuron`) " + whereBodyClause + " RETURN count(n)")).single().get(0).asInt();

        Assert.assertEquals("invalid number of neurons after " + procedureContext,
                            expectedNeuronCount, numNeuronsWithBodyId);
        Assert.assertEquals("invalid number of dataset neurons after " + procedureContext,
                            expectedNeuronCount, numDatasetNeuronsWithBodyId);

        // should skip deletion of properties quietly if they don't exist
        try {
            session.writeTransaction(tx -> tx.run(statement, bodyIdAndDatasetValues));
        } catch (final Throwable t) {
            throw new RuntimeException("exception thrown after " + procedureContext + " second time", t);
        }
    }

}
