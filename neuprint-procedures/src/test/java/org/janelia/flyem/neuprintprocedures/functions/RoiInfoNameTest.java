package org.janelia.flyem.neuprintprocedures.functions;

import apoc.convert.Json;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrintMain;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class RoiInfoNameTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(LoadingProcedures.class)
                .withFunction(NeuPrintUserFunctions.class);
    }

    @BeforeClass
    public static void before() {

        final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        String neuronsJsonPath = "src/test/resources/neuronList.json";
        List<Neuron> neuronList = NeuPrintMain.readNeuronsJson(neuronsJsonPath);

        String synapseJsonPath = "src/test/resources/synapseList.json";
        List<Synapse> synapseList = NeuPrintMain.readSynapsesJson(synapseJsonPath);

        String connectionsJsonPath = "src/test/resources/connectionsList.json";
        List<SynapticConnection> connectionsList = NeuPrintMain.readConnectionsJson(connectionsJsonPath);

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());

        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

        String dataset = "test";

        NeuPrintMain.initializeDatabase(neo4jImporter, dataset, 1.0F, .20D, .80D, true, true, timeStamp);
        neo4jImporter.addSynapsesWithRois("test", synapseList, timeStamp);
        neo4jImporter.addSynapsesTo("test", connectionsList, timeStamp);
        neo4jImporter.addSegments("test", neuronList, true, .20D, .80D, 5, timeStamp);
        neo4jImporter.indexBooleanRoiProperties(dataset);
    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldProduceCorrectRoiBasedName() {
        Session session = driver.session();

        List<String> superLevelRois = new ArrayList<>();
        superLevelRois.add("roiA");

        String name5percent = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`{bodyId:8426959}) WITH neuprint.roiInfoAsName(n.roiInfo,n.pre,n.post,.05,$superLevelRois) AS name RETURN name ", parameters("superLevelRois", superLevelRois))).single().get(0).asString();

        Assert.assertEquals("roiA-roiA", name5percent);

        String name100percent = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`{bodyId:8426959}) WITH neuprint.roiInfoAsName(n.roiInfo,n.pre,n.post,1.0,$superLevelRois) AS name RETURN name ", parameters("superLevelRois", superLevelRois))).single().get(0).asString();

        Assert.assertEquals("none-none", name100percent);

    }

    @Test
    public void shouldGetConnectionCategoryCounts() {
        Session session = driver.session();

        String resultJson = session.readTransaction(tx -> tx.run("WITH neuprint.getCategoriesOfConnections(8426959, \"test\") AS result RETURN result")).single().get(0).asString();

        Gson gson = new Gson();

        Map<String, SynapseCounter> resultObject = gson.fromJson(resultJson, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        int sumPre = 0;
        int sumPost = 0;

        for (String category : resultObject.keySet()) {
            sumPre += resultObject.get(category).getPre();
            sumPost += resultObject.get(category).getPost();
        }

        Assert.assertEquals(3, sumPre);
        Assert.assertEquals(2, sumPost);

        Assert.assertEquals(2, resultObject.get("roiA").getPost());

        Assert.assertEquals(3, resultObject.get("roiA").getPre());

        // should error if body id doesn't exist

        boolean throwsException = false;
        try {
            session.readTransaction(tx -> tx.run("WITH neuprint.getCategoriesOfConnections(8123, \"test\") AS result RETURN result")).single().get(0).asString();
        } catch (Exception e) {
            throwsException = true;
        }
        Assert.assertTrue(throwsException);

    }

}
