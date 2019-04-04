package org.janelia.flyem.neuprintprocedures.functions;

import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;
import org.junit.AfterClass;
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
import java.util.List;

public class GetNeuronCentroidTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withProcedure(LoadingProcedures.class)
                .withFunction(NeuPrintUserFunctions.class);
    }

    @BeforeClass
    public static void before() {

        final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        String neuronsJsonPath = "src/test/resources/neuronList.json";
        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);

        String synapseJsonPath = "src/test/resources/synapseList.json";
        List<Synapse> synapseList = NeuPrinterMain.readSynapsesJson(synapseJsonPath);

        String connectionsJsonPath = "src/test/resources/connectionsList.json";
        List<SynapticConnection> connectionsList = NeuPrinterMain.readConnectionsJson(connectionsJsonPath);

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());

        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

        String dataset = "test";

        NeuPrinterMain.initializeDatabase(neo4jImporter, dataset, 1.0F, .20D, .80D, true, true, timeStamp);
        neo4jImporter.addSynapsesWithRois("test", synapseList, timeStamp);
        neo4jImporter.addSynapsesTo("test", connectionsList, timeStamp);
        neo4jImporter.addSegments("test", neuronList, true, .20D, .80D, 5, timeStamp);
        neo4jImporter.indexBooleanRoiProperties(dataset);
        neo4jImporter.addAutoNames("test", timeStamp);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldReturnCentroid() {

        Session session = driver.session();

        List<Object> centroid = session.readTransaction(tx -> tx.run("WITH neuprint.getNeuronCentroid(8426959,'test') AS centroid RETURN centroid")).single().get(0).asList();

        Assert.assertEquals(4222L, centroid.get(0));
        Assert.assertEquals(2402L, centroid.get(1));
        Assert.assertEquals(1688L, centroid.get(2));

    }
}
