package org.janelia.flyem.neuprintprocedures.triggers;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
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
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetaNodeUpdaterTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(LoadingProcedures.class)
                .withProcedure(Create.class);
    }

    @BeforeClass
    public static void before() {

        final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        String neuronsJsonPath = "src/test/resources/shortestPathNeuronList.json";
        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);

        String synapseJsonPath = "src/test/resources/shortestPathSynapseList.json";
        List<Synapse> synapseList = NeuPrinterMain.readSynapsesJson(synapseJsonPath);

        String connectionsJsonPath = "src/test/resources/shortestPathConnectionsList.json";
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
        neo4jImporter.createMetaNodeWithDataModelNode("otherDataset", 1.0F, .20F, .80F, true, timeStamp);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void metaNodeLastDatabaseEditShouldUpdateButNotSynapseCountsUponNonSynapseChangesAndSynapseChangeShouldTriggerCompleteMetaNodeUpdate() throws InterruptedException {

        Session session = driver.session();

        LocalDateTime metaNodeUpdateTimeBefore = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n.lastDatabaseEdit").single().get(0).asLocalDateTime());
        LocalDateTime otherDatasetMetaNodeTimeBefore = session.readTransaction(tx -> tx.run("MATCH (n:Meta:otherDataset{dataset:\"otherDataset\"}) RETURN n.lastDatabaseEdit").single().get(0).asLocalDateTime());

        //delay to move time stamp
        TimeUnit.SECONDS.sleep(3);

        session.writeTransaction(tx -> {
            tx.run("MERGE (n:`test-Segment`{bodyId:50}) ON CREATE SET n:test, n:Segment, n.roiInfo=\"{'roiA':{'pre':5,'post':2},'newRoi':{'pre':5,'post':2}}\", " +
                    "n.pre=10, " +
                    "n.post=4, " +
                    "n.roiA=TRUE, " +
                    "n.newRoi=TRUE " +
                    "RETURN n");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        Node metaNodeAfter = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode());
        LocalDateTime otherDatasetMetaNodeTimeAfter = session.readTransaction(tx -> tx.run("MATCH (n:Meta:otherDataset{dataset:\"otherDataset\"}) RETURN n.lastDatabaseEdit").single().get(0).asLocalDateTime());

        LocalDateTime metaNodeUpdateTimeAfter = (LocalDateTime) metaNodeAfter.asMap().get("lastDatabaseEdit");

        Assert.assertTrue(metaNodeUpdateTimeBefore.isBefore(metaNodeUpdateTimeAfter));

        Assert.assertEquals(15L, metaNodeAfter.asMap().get("totalPostCount"));
        Assert.assertEquals(6L, metaNodeAfter.asMap().get("totalPreCount"));

        String metaSynapseCountPerRoi = (String) metaNodeAfter.asMap().get("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> metaSynapseCountPerRoiMap = gson.fromJson(metaSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertEquals(6L, metaSynapseCountPerRoiMap.get("roiA").getPre());
        Assert.assertEquals(10L, metaSynapseCountPerRoiMap.get("roiA").getPost());

        Assert.assertEquals(5, metaSynapseCountPerRoiMap.keySet().size());
        Assert.assertTrue(metaSynapseCountPerRoiMap.containsKey("roiA")
                && metaSynapseCountPerRoiMap.containsKey("roiB")
                && metaSynapseCountPerRoiMap.containsKey("roi'C")
            && metaSynapseCountPerRoiMap.containsKey("roi1")
                && metaSynapseCountPerRoiMap.containsKey("roi2")
        );

        LocalDateTime metaNodeUpdateTimeBefore2 = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n.lastDatabaseEdit").single().get(0).asLocalDateTime());

        //delay to move time stamp
        TimeUnit.SECONDS.sleep(3);

        // disabled for now
        //note that update is based on synapse count per roi on neurons, not info from synapse nodes directly
//        session.writeTransaction(tx -> {
//            tx.run("CREATE (n:Synapse:test:`test-Synapse`) SET n.`roiA`=TRUE, n.`newRoi`=TRUE RETURN n");
//            return 1;
//        });
//
//        //delay to allow for update
//        TimeUnit.SECONDS.sleep(5);
//
//        Node metaNode = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode());
//
//        LocalDateTime metaNodeUpdateTimeAfter2 = (LocalDateTime) metaNode.asMap().get("lastDatabaseEdit");
//
//        Assert.assertTrue(metaNodeUpdateTimeBefore2.isBefore(metaNodeUpdateTimeAfter2));
//
//        Assert.assertEquals(11L, metaNode.asMap().get("totalPostCount"));
//        Assert.assertEquals(14L, metaNode.asMap().get("totalPreCount"));
//
//        String metaSynapseCountPerRoi2 = (String) metaNode.asMap().get("roiInfo");
//        Map<String, SynapseCounter> metaSynapseCountPerRoiMap2 = gson.fromJson(metaSynapseCountPerRoi2, new TypeToken<Map<String, SynapseCounter>>() {
//        }.getType());
//
//        Assert.assertEquals(9L, metaSynapseCountPerRoiMap2.get("roiA").getPre());
//        Assert.assertEquals(8L, metaSynapseCountPerRoiMap2.get("roiA").getPost());
//
//        Assert.assertEquals(4, metaSynapseCountPerRoiMap2.keySet().size());
//        Assert.assertTrue(metaSynapseCountPerRoiMap2.containsKey("roiA")
//                && metaSynapseCountPerRoiMap2.containsKey("roiB")
//                && metaSynapseCountPerRoiMap2.containsKey("roi'C")
//                && metaSynapseCountPerRoiMap2.containsKey("newRoi"));
//
//        // only relevant meta node time stamp should be updated
//        Assert.assertEquals(otherDatasetMetaNodeTimeBefore,otherDatasetMetaNodeTimeAfter);

    }
}


