package org.janelia.flyem.neuprintprocedures.triggers;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MetaNodeUpdaterTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(Create.class);
    }

    @BeforeClass
    public static void before() {

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());

        String dataset = "test";

        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
        neo4jImporter.prepDatabase(dataset);

        neo4jImporter.addSegments(dataset, neuronList);

        neo4jImporter.addConnectsTo(dataset, bodyList);
        neo4jImporter.addSynapsesWithRois(dataset, bodyList);
        neo4jImporter.addSynapsesTo(dataset, preToPost);
        neo4jImporter.addSegmentRois(dataset, bodyList);
        neo4jImporter.addSynapseSets(dataset, bodyList);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
        neo4jImporter.createMetaNodeWithDataModelNode("otherDataset", 1.0F);


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

        Assert.assertEquals(7L, metaNodeAfter.asMap().get("totalPostCount"));
        Assert.assertEquals(4L, metaNodeAfter.asMap().get("totalPreCount"));

        String metaSynapseCountPerRoi = (String) metaNodeAfter.asMap().get("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> metaSynapseCountPerRoiMap = gson.fromJson(metaSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertEquals(4L, metaSynapseCountPerRoiMap.get("roiA").getPre());
        Assert.assertEquals(6L, metaSynapseCountPerRoiMap.get("roiA").getPost());

        Assert.assertEquals(3, metaSynapseCountPerRoiMap.keySet().size());
        Assert.assertTrue(metaSynapseCountPerRoiMap.containsKey("roiA")
                && metaSynapseCountPerRoiMap.containsKey("roiB")
                && metaSynapseCountPerRoiMap.containsKey("roi'C"));

        LocalDateTime metaNodeUpdateTimeBefore2 = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n.lastDatabaseEdit").single().get(0).asLocalDateTime());

        //delay to move time stamp
        TimeUnit.SECONDS.sleep(3);

        //note that update is based on synapse count per roi on neurons, not info from synapse nodes directly
        session.writeTransaction(tx -> {
            tx.run("CREATE (n:Synapse:test:`test-Synapse`) SET n.`roiA`=TRUE, n.`newRoi`=TRUE RETURN n");
            return 1;
        });

        //delay to allow for update
        TimeUnit.SECONDS.sleep(5);

        Node metaNode = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode());

        LocalDateTime metaNodeUpdateTimeAfter2 = (LocalDateTime) metaNode.asMap().get("lastDatabaseEdit");

        Assert.assertTrue(metaNodeUpdateTimeBefore2.isBefore(metaNodeUpdateTimeAfter2));

        Assert.assertEquals(11L, metaNode.asMap().get("totalPostCount"));
        Assert.assertEquals(14L, metaNode.asMap().get("totalPreCount"));

        String metaSynapseCountPerRoi2 = (String) metaNode.asMap().get("roiInfo");
        Map<String, SynapseCounter> metaSynapseCountPerRoiMap2 = gson.fromJson(metaSynapseCountPerRoi2, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertEquals(9L, metaSynapseCountPerRoiMap2.get("roiA").getPre());
        Assert.assertEquals(8L, metaSynapseCountPerRoiMap2.get("roiA").getPost());

        Assert.assertEquals(4, metaSynapseCountPerRoiMap2.keySet().size());
        Assert.assertTrue(metaSynapseCountPerRoiMap2.containsKey("roiA")
                && metaSynapseCountPerRoiMap2.containsKey("roiB")
                && metaSynapseCountPerRoiMap2.containsKey("roi'C")
                && metaSynapseCountPerRoiMap2.containsKey("newRoi"));

        // only relevant meta node time stamp should be updated
        Assert.assertEquals(otherDatasetMetaNodeTimeBefore,otherDatasetMetaNodeTimeAfter);

    }
}


