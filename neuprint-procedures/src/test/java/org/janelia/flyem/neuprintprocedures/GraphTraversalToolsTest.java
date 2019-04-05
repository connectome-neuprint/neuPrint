package org.janelia.flyem.neuprintprocedures;

import apoc.convert.Json;
import apoc.create.Create;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
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
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class GraphTraversalToolsTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(Create.class)
                .withProcedure(LoadingProcedures.class)
                .withFunction(NeuPrintUserFunctions.class);
    }

    @BeforeClass
    public static void before() {

        final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        File swcFile1 = new File("src/test/resources/101.swc");
        File swcFile2 = new File("src/test/resources/102.swc");
        File swcFile3 = new File("src/test/resources/831744.swc");

        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2, swcFile3};

        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

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
        neo4jImporter.addSegments("test", neuronList, true, .20D, .80D, 1, timeStamp);
        neo4jImporter.indexBooleanRoiProperties(dataset);
        neo4jImporter.addSkeletonNodes("test", skeletonList, timeStamp);
    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldReturnSynapseRois() {

        Session session = driver.session();

        List<Object> synapseRois = session.readTransaction(tx -> tx.run("WITH neuprint.getSynapseRois(4292, 2261, 1542,'test') AS roiList RETURN roiList")).single().get(0).asList();

        Assert.assertEquals(3, synapseRois.size());
        Assert.assertTrue(synapseRois.contains("roiA"));
        Assert.assertTrue(synapseRois.contains("roiB"));
        Assert.assertTrue(synapseRois.contains("roi'C"));

    }

    @Test
    public void shouldReturnSegmentRois() {

        Session session = driver.session();

        List<Object> segmentRois = session.readTransaction(tx -> tx.run("WITH neuprint.getSegmentRois(831744,'test') AS roiList RETURN roiList")).single().get(0).asList();

        Assert.assertEquals(3, segmentRois.size());
        Assert.assertTrue(segmentRois.contains("roiA"));
        Assert.assertTrue(segmentRois.contains("roiB"));
        Assert.assertTrue(segmentRois.contains("roi'C"));

        List<Object> segment2Rois = session.readTransaction(tx -> tx.run("WITH neuprint.getSegmentRois(100554,'test') AS roiList RETURN roiList")).single().get(0).asList();

        Assert.assertEquals(0, segment2Rois.size());

    }

}




