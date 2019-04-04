package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounterWithHighPrecisionCounts;
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
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class AddAndRemoveRoiTest {

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

        File swcFile1 = new File("src/test/resources/8426959.swc");
        File swcFile2 = new File("src/test/resources/831744.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

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
        neo4jImporter.addSkeletonNodes("test", skeletonList, timeStamp);
        neo4jImporter.indexBooleanRoiProperties(dataset);
        neo4jImporter.addAutoNames("test", timeStamp);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldAddRoiToSynapse() {

        Session session = driver.session();

        session.writeTransaction(tx -> tx.run("CALL proofreader.addRoiToSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 4287, "y", 2277, "z", 1542, "roiName", "roiX", "dataset", "test")));

        boolean roiX = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN n.roiX", parameters("x", 4287, "y", 2277, "z", 1542))).single().get(0).asBoolean();

        Assert.assertTrue(roiX);

        List<Record> csRecordList = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(cs:ConnectionSet) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN cs.roiInfo", parameters("x", 4287, "y", 2277, "z", 1542))).list();

        Gson gson = new Gson();

        for (Record record : csRecordList) {
            String roiInfo = record.get(0).asString();
            Map<String, SynapseCounterWithHighPrecisionCounts> roiInfoMap = gson.fromJson(roiInfo, ROI_INFO_TYPE);
            Assert.assertEquals(1, roiInfoMap.get("roiX").getPre());
            Assert.assertEquals(0, roiInfoMap.get("roiX").getPreHP());
            Assert.assertEquals(0, roiInfoMap.get("roiX").getPost());
            Assert.assertEquals(0, roiInfoMap.get("roiX").getPostHP());
        }

        String roiInfo = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(:SynapseSet)<-[:Contains]-(s:Segment) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN s.roiInfo", parameters("x", 4287, "y", 2277, "z", 1542))).single().get(0).asString();

        Map<String, SynapseCounter> neuronRoiInfoMap = gson.fromJson(roiInfo, ROI_INFO_TYPE);
        Assert.assertEquals(1, neuronRoiInfoMap.get("roiX").getPre());
        Assert.assertEquals(0, neuronRoiInfoMap.get("roiX").getPost());

        boolean roiXForNeuron = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(:SynapseSet)<-[:Contains]-(s:Segment) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN s.roiX", parameters("x", 4287, "y", 2277, "z", 1542))).single().get(0).asBoolean();
        Assert.assertTrue(roiXForNeuron);

        String metaRoiInfo = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test) RETURN n.roiInfo").single().get(0).asString());

        Map<String, SynapseCounter> roiInfoMap = gson.fromJson(metaRoiInfo, ROI_INFO_TYPE);
        Assert.assertEquals(1, roiInfoMap.get("roiX").getPre());
        Assert.assertEquals(0, roiInfoMap.get("roiX").getPost());

        // should not be able to add twice

        session.writeTransaction(tx -> tx.run("CALL proofreader.addRoiToSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 4287, "y", 2277, "z", 1542, "roiName", "roiX", "dataset", "test")));

        List<Record> csRecordList2 = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(cs:ConnectionSet) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN cs.roiInfo", parameters("x", 4287, "y", 2277, "z", 1542))).list();

        for (Record record : csRecordList2) {
            String roiInfo2 = record.get(0).asString();
            Map<String, SynapseCounterWithHighPrecisionCounts> roiInfoMap2 = gson.fromJson(roiInfo2, ROI_INFO_TYPE);
            Assert.assertEquals(1, roiInfoMap2.get("roiX").getPre());
            Assert.assertEquals(0, roiInfoMap2.get("roiX").getPreHP());
            Assert.assertEquals(0, roiInfoMap2.get("roiX").getPost());
            Assert.assertEquals(0, roiInfoMap2.get("roiX").getPostHP());
        }

    }

    @Test
    public void shouldAddRoiToOrphanSynapse() {
        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"post\", \"Location\": [ 5,22,99 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.addRoiToSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 5, "y", 22, "z", 99, "roiName", "roiXX", "dataset", "test")));

        boolean roiXX = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN exists(n.roiXX)", parameters("x", 5, "y", 22, "z", 99))).single().get(0).asBoolean();

        Assert.assertTrue(roiXX);

        double confidence = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN n.confidence", parameters("x", 5, "y", 22, "z", 99))).single().get(0).asDouble();
        Assert.assertEquals(.88, confidence, .0001);

    }

    @Test
    public void shouldRemoveRoiFromSynapse() {

        Session session = driver.session();

        session.writeTransaction(tx -> tx.run("CALL proofreader.removeRoiFromSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 4287, "y", 2277, "z", 1542, "roiName", "roiA", "dataset", "test")));

        boolean roiA = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN exists(n.roiA)", parameters("x", 4287, "y", 2277, "z", 1542))).single().get(0).asBoolean();

        Assert.assertFalse(roiA);

        List<Record> csRecordList = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(cs:ConnectionSet) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN cs.roiInfo, cs.datasetBodyIds", parameters("x", 4287, "y", 2277, "z", 1542))).list();

        Gson gson = new Gson();

        for (Record record : csRecordList) {
            String roiInfo = record.get(0).asString();
            String datasetBodyIds = record.get(1).asString();
            Map<String, SynapseCounterWithHighPrecisionCounts> roiInfoMap = gson.fromJson(roiInfo, ROI_INFO_TYPE);
            if (datasetBodyIds.equals("test:8426959:2589725")) {
                Assert.assertFalse(roiInfoMap.containsKey("roiA"));
            } else {
                Assert.assertEquals(0, roiInfoMap.get("roiA").getPre());
            }
        }

        String roiInfo = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(:SynapseSet)<-[:Contains]-(s:Segment) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN s.roiInfo", parameters("x", 4287, "y", 2277, "z", 1542))).single().get(0).asString();

        Map<String, SynapseCounter> neuronRoiInfoMap = gson.fromJson(roiInfo, ROI_INFO_TYPE);
        Assert.assertEquals(1, neuronRoiInfoMap.get("roiA").getPre());

        boolean roiAForNeuron = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(:SynapseSet)<-[:Contains]-(s:Segment) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN s.roiA", parameters("x", 4287, "y", 2277, "z", 1542))).single().get(0).asBoolean();
        Assert.assertTrue(roiAForNeuron);

        String metaRoiInfo = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test) RETURN n.roiInfo").single().get(0).asString());

        Map<String, SynapseCounter> roiInfoMap = gson.fromJson(metaRoiInfo, ROI_INFO_TYPE);
        Assert.assertEquals(3, roiInfoMap.get("roiA").getPre());
        Assert.assertEquals(6, roiInfoMap.get("roiA").getPost());

        // should not be able to delete twice
        session.writeTransaction(tx -> tx.run("CALL proofreader.removeRoiFromSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 4287, "y", 2277, "z", 1542, "roiName", "roiA", "dataset", "test")));
        String metaRoiInfo2 = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test) RETURN n.roiInfo").single().get(0).asString());
        Map<String, SynapseCounter> roiInfoMap2 = gson.fromJson(metaRoiInfo2, ROI_INFO_TYPE);
        Assert.assertEquals(3, roiInfoMap2.get("roiA").getPre());
        Assert.assertEquals(6, roiInfoMap2.get("roiA").getPost());

        session.writeTransaction(tx -> tx.run("CALL proofreader.removeRoiFromSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 4287, "y", 2277, "z", 1502, "roiName", "roiA", "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.removeRoiFromSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 4222, "y", 2402, "z", 1688, "roiName", "roiA", "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.removeRoiFromSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 8000, "y", 7000, "z", 6000, "roiName", "roiA", "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.removeRoiFromSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 4000, "y", 5000, "z", 6000, "roiName", "roiA", "dataset", "test")));

        String roiInfo2 = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`)<-[:Contains]-(:SynapseSet)<-[:Contains]-(s:Segment) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN s.roiInfo", parameters("x", 4287, "y", 2277, "z", 1502))).single().get(0).asString();

        Map<String, SynapseCounter> neuronRoiInfoMap2 = gson.fromJson(roiInfo2, ROI_INFO_TYPE);
        Assert.assertFalse(neuronRoiInfoMap2.containsKey("roiA"));

    }

    @Test
    public void shouldRemoveRoiFromOrphanSynapse() {
        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"post\", \"Location\": [ 50,22,99 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.removeRoiFromSynapse($x,$y,$z,$roiName,$dataset)", parameters("x", 50, "y", 22, "z", 99, "roiName", "test2", "dataset", "test")));

        boolean test2 = session.readTransaction(tx -> tx.run("MATCH (n:`test-Synapse`) WHERE n.location=point({x:$x,y:$y,z:$z, srid:9157}) RETURN exists(n.test2)", parameters("x", 50, "y", 22, "z", 99))).single().get(0).asBoolean();

        Assert.assertFalse(test2);

    }

    private static Type ROI_INFO_TYPE = new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
    }.getType();
}
