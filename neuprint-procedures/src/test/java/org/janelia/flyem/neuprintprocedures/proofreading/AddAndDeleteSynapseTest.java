package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
import org.janelia.flyem.neuprint.SynapseMapper;
import org.janelia.flyem.neuprint.model.BodyWithSynapses;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.SynapseCounter;
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
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

public class AddAndDeleteSynapseTest {

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

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRoisForShortestPath.json");
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
        neo4jImporter.addConnectionSets(dataset, bodyList, mapper.getSynapseLocationToBodyIdMap(), .2F, .8F, true);
        neo4jImporter.addSynapseSets(dataset, bodyList);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F, .20F, .80F, true);
        neo4jImporter.addNeuronLabels(dataset, 1);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldAddSynapseNodeAndUpdateMetaNode() {

        Session session = driver.session();

        // get original meta node roi info for comparison
        String origMetaNodeRoiInfoString = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n.roiInfo")).single().get(0).asString();
        Gson gson = new Gson();
        Map<String, SynapseCounter> origMetaRoiInfo = gson.fromJson(origMetaNodeRoiInfoString, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        String synapseJson = "{ \"Type\": \"post\", \"Location\": [ 1,1,1 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));

        int synapseCount = session.readTransaction(tx -> tx.run("WITH point({ x:1, y:1, z:1 }) AS loc MATCH (n:`test-Synapse`:Synapse{location:loc}) RETURN count(n)")).single().get(0).asInt();
        // a single synapse node should exist
        Assert.assertEquals(1, synapseCount);

        Node synapseNode = session.readTransaction(tx -> tx.run("WITH point({ x:1, y:1, z:1 }) AS loc MATCH (n:`test-Synapse`:Synapse{location:loc}) RETURN n")).single().get(0).asNode();
        Map<String, Object> synapseNodeAsMap = synapseNode.asMap();

        // synapse has properties and rois expected
        Assert.assertEquals("post", synapseNodeAsMap.get("type"));
        Assert.assertEquals(.88, (double) synapseNodeAsMap.get("confidence"), 0.001);
        Assert.assertTrue(synapseNodeAsMap.containsKey("test1"));
        Assert.assertTrue(synapseNodeAsMap.containsKey("test2"));

        // synapse has :PostSyn label (would be :PreSyn if a pre synapse)
        Assert.assertTrue(synapseNode.hasLabel("PostSyn"));
        Assert.assertTrue(synapseNode.hasLabel("test-PostSyn"));
        Assert.assertFalse(synapseNode.hasLabel("PreSyn"));
        Assert.assertFalse(synapseNode.hasLabel("test-PreSyn"));

        // synapse has dataset label
        Assert.assertTrue(synapseNode.hasLabel("test"));

        String synapseJson2 = "{ \"Type\": \"pre\", \"Location\": [ 8,50,9 ], \"Confidence\": .88, \"rois\": [ \"roiA\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson2, "dataset", "test")));

        // meta node total pre and post count should match database
        long preSynapseCount = session.readTransaction(tx -> tx.run("MATCH (n:PreSyn) RETURN count(n)")).single().get(0).asLong();
        long postSynapseCount = session.readTransaction(tx -> tx.run("MATCH (n:PostSyn) RETURN count(n)")).single().get(0).asLong();
        Map<String, Object> metaNodeProps = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n.totalPreCount, n.totalPostCount, n.roiInfo")).single().asMap();

        Assert.assertEquals(preSynapseCount, metaNodeProps.get("n.totalPreCount"));
        Assert.assertEquals(postSynapseCount, metaNodeProps.get("n.totalPostCount"));

        Map<String, SynapseCounter> metaRoiInfo = gson.fromJson((String) metaNodeProps.get("n.roiInfo"), new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertEquals(origMetaRoiInfo.get("roiA").getPre() + 1, metaRoiInfo.get("roiA").getPre());
        Assert.assertEquals(origMetaRoiInfo.get("roiA").getPost(), metaRoiInfo.get("roiA").getPost());
        if (origMetaRoiInfo.containsKey("test1")) {
            Assert.assertEquals(origMetaRoiInfo.get("test1").getPre(), metaRoiInfo.get("test1").getPre());
            Assert.assertEquals(origMetaRoiInfo.get("test1").getPost() + 1, metaRoiInfo.get("test1").getPost());
        } else {
            Assert.assertEquals(1, metaRoiInfo.get("test1").getPost());
            Assert.assertEquals(0, metaRoiInfo.get("test1").getPre());
        }
        if (origMetaRoiInfo.containsKey("test2")) {
            Assert.assertEquals(origMetaRoiInfo.get("test2").getPre() + 1, metaRoiInfo.get("test2").getPre());
            Assert.assertEquals(origMetaRoiInfo.get("test2").getPost() + 1, metaRoiInfo.get("test2").getPost());
        } else {
            Assert.assertEquals(1, metaRoiInfo.get("test2").getPre());
            Assert.assertEquals(1, metaRoiInfo.get("test2").getPost());
        }

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldThrowExceptionIfSynapseAtLocationAlreadyExists() {

        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"post\", \"Location\": [ 4301, 2276, 1535 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));
    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldThrowExceptionIfSynapseDoesNotHaveType() {

        Session session = driver.session();

        String synapseJson = "{ \"Location\": [ 0,1,2 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));
    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldThrowExceptionIfSynapseHasTypeOtherThanPreOrPost() {

        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"other\", \"Location\": [ 5,60,7 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));
    }

    @Test
    public void confidenceShouldBe0IfNotSpecified() {

        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"pre\", \"Location\": [ 5,60,7], \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));

        Node synapseNode = session.readTransaction(tx -> tx.run("WITH point({ x:5, y:60, z:7 }) AS loc MATCH (n:`test-Synapse`:Synapse{location:loc}) RETURN n")).single().get(0).asNode();
        Map<String, Object> synapseNodeAsMap = synapseNode.asMap();

        Assert.assertEquals(0.0, (double) synapseNodeAsMap.get("confidence"), 0.0001);

    }

    @Test
    public void shouldAddSynapseConnection() {

        Session session = driver.session();

        String postSynapseJson = "{ \"Type\": \"post\", \"Location\": [ 88,89,90 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJson, "dataset", "test")));

        String preSynapseJson = "{ \"Type\": \"pre\", \"Location\": [ 98,99,100 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", preSynapseJson, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(98,99,100,88,89,90,\"test\")"));

        Map<String, Object> resultMap = session.readTransaction(tx -> tx.run("WITH point({ x:98, y:99, z:100 }) AS loc MATCH (n:`test-Synapse`:Synapse:PreSyn{location:loc})-[:SynapsesTo]->(m) RETURN m.location.x,m.location.y,m.location.z")).single().asMap();

        Assert.assertEquals(88.0D, (double) resultMap.get("m.location.x"), .0001);
        Assert.assertEquals(89.0D, (double) resultMap.get("m.location.y"), .0001);
        Assert.assertEquals(90.0D, (double) resultMap.get("m.location.z"), .0001);

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfPreSynapseDoesNotExist() {

        Session session = driver.session();

        String postSynapseJson = "{ \"Type\": \"post\", \"Location\": [ 87,88,89 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJson, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(8,9,8,87,88,89,\"test\")"));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfPostSynapseDoesNotExist() {

        Session session = driver.session();
        String preSynapseJson = "{ \"Type\": \"pre\", \"Location\": [ 97,98,99 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", preSynapseJson, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(97,98,99,8,9,8,\"test\")"));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfFirstLocationIsNotPre() {

        Session session = driver.session();

        String postSynapseJson = "{ \"Type\": \"post\", \"Location\": [ 10,9,8 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJson, "dataset", "test")));

        String postSynapseJson2 = "{ \"Type\": \"post\", \"Location\": [ 11,10,9 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJson2, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(10,9,8,11,10,9,\"test\")"));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfSecondLocationIsNotPost() {

        Session session = driver.session();

        String preSynapseJson = "{ \"Type\": \"pre\", \"Location\": [ 50,50,50 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", preSynapseJson, "dataset", "test")));

        String preSynapseJson2 = "{ \"Type\": \"pre\", \"Location\": [ 6,6,6 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", preSynapseJson2, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(50,50,50,6,6,6,\"test\")"));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfPreSynapseBelongsToBody() {

        Session session = driver.session();

        String postSynapseJson = "{ \"Type\": \"post\", \"Location\": [ 0,1,2 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJson, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(4300,2000,1500,0,1,2,\"test\")"));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfPostSynapseBelongsToBody() {

        Session session = driver.session();

        String preSynapseJson = "{ \"Type\": \"pre\", \"Location\": [ 0,1,3 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", preSynapseJson, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(0,1,3,4301,2276,1535,\"test\")"));

    }

    @Test
    public void shouldDeleteOrphanSynapses() {

        Session session = driver.session();

        // get original meta node roi info for comparison
        String origMetaNodeRoiInfoString = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n.roiInfo")).single().get(0).asString();
        Gson gson = new Gson();
        Map<String, SynapseCounter> origMetaRoiInfo = gson.fromJson(origMetaNodeRoiInfoString, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        String preSynapseJson = "{ \"Type\": \"pre\", \"Location\": [ 2,22,222 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", preSynapseJson, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSynapse($x,$y,$z,$dataset)", parameters("x", 2, "y", 22, "z", 222, "dataset", "test")));

        // meta node total pre and post count should match database
        long preSynapseCount = session.readTransaction(tx -> tx.run("MATCH (n:PreSyn) RETURN count(n)")).single().get(0).asLong();
        long postSynapseCount = session.readTransaction(tx -> tx.run("MATCH (n:PostSyn) RETURN count(n)")).single().get(0).asLong();
        Map<String, Object> metaNodeProps = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n.totalPreCount, n.totalPostCount, n.roiInfo")).single().asMap();

        Assert.assertEquals(preSynapseCount, metaNodeProps.get("n.totalPreCount"));
        Assert.assertEquals(postSynapseCount, metaNodeProps.get("n.totalPostCount"));

        Map<String, SynapseCounter> metaRoiInfo = gson.fromJson((String) metaNodeProps.get("n.roiInfo"), new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        if (origMetaRoiInfo.containsKey("test1")) {
            Assert.assertEquals(origMetaRoiInfo.get("test1").getPre(), metaRoiInfo.get("test1").getPre());
            Assert.assertEquals(origMetaRoiInfo.get("test1").getPost(), metaRoiInfo.get("test1").getPost());
        } else {
            Assert.assertFalse(metaRoiInfo.containsKey("test1"));
        }
        if (origMetaRoiInfo.containsKey("test2")) {
            Assert.assertEquals(origMetaRoiInfo.get("test2").getPre(), metaRoiInfo.get("test2").getPre());
            Assert.assertEquals(origMetaRoiInfo.get("test2").getPost(), metaRoiInfo.get("test2").getPost());
        } else {
            Assert.assertFalse(metaRoiInfo.containsKey("test2"));
        }

        // synapse is deleted
        int synapseCount = session.readTransaction(tx -> tx.run("WITH point({ x:2, y:22, z:222 }) AS loc MATCH (n:`test-Synapse`{location:loc}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, synapseCount);

        // should continue quietly if synapse doesn't exist
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSynapse($x,$y,$z,$dataset)", parameters("x", 3, "y", 33, "z", 333, "dataset", "test")));

    }

    @Test
    public void shouldDeleteNonOrphanedSynapse() {

        Session session = driver.session();

        // post synapse
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSynapse($x,$y,$z,$dataset)", parameters("x", 4301, "y", 2276, "z", 1535, "dataset", "test")));

        int synapseCount = session.readTransaction(tx -> tx.run("WITH point({ x:4301, y:2276, z:1535 }) AS loc MATCH (n:`test-Synapse`{location:loc}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, synapseCount);

        // check connection sets
        int connectionSetCount = session.readTransaction(tx -> tx.run("MATCH (n:ConnectionSet{datasetBodyIds:\"test:8426959:26311\"}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, connectionSetCount);

        // check connects to relationships
        int connectsToCount = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})-[c:ConnectsTo]->(m{bodyId:26311}) RETURN count(c)")).single().get(0).asInt();
        Assert.assertEquals(0, connectsToCount);
        int reverseConnectsToCount = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})<-[c:ConnectsTo]-(m{bodyId:26311}) RETURN count(c)")).single().get(0).asInt();
        Assert.assertEquals(1, reverseConnectsToCount);

        // check affected neuron
        Node neuron = session.readTransaction(tx -> tx.run("MATCH (m:Segment:`test-Segment`{bodyId:26311}) RETURN m")).single().get(0).asNode();
        // should still be neuron
        Assert.assertTrue(neuron.hasLabel("Neuron"));
        Assert.assertTrue(neuron.hasLabel("test-Neuron"));
        // check properties
        Map<String, Object> neuronMap = neuron.asMap();
        Assert.assertEquals(1L, neuronMap.get("pre"));
        Assert.assertEquals(5L, neuronMap.get("post"));

        String roiInfoString = (String) neuronMap.get("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> roiInfo = gson.fromJson(roiInfoString, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
        Assert.assertEquals(1, roiInfo.keySet().size());
        Assert.assertTrue(roiInfo.containsKey("roiA"));
        Assert.assertEquals(1, roiInfo.get("roiA").getPre());
        Assert.assertEquals(5, roiInfo.get("roiA").getPost());

        // check rois on neuron
        List<Object> segmentRois = session.readTransaction(tx -> tx.run("WITH neuprint.getSegmentRois(26311,'test') AS roiList RETURN roiList")).single().get(0).asList();
        Assert.assertEquals(1, segmentRois.size());
        Assert.assertTrue(segmentRois.contains("roiA"));

        // pre synapse (goes to multiple posts)
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSynapse($x,$y,$z,$dataset)", parameters("x", 4287, "y", 2277, "z", 1542, "dataset", "test")));

        int synapseCount2 = session.readTransaction(tx -> tx.run("WITH point({ x:4287, y:2277, z:1542 }) AS loc MATCH (n:`test-Synapse`{location:loc}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, synapseCount2);

        // check connection sets
        int connectionSetCount2 = session.readTransaction(tx -> tx.run("MATCH (n:ConnectionSet{datasetBodyIds:\"test:8426959:26311\"}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, connectionSetCount2);
        int connectionSetCount3 = session.readTransaction(tx -> tx.run("MATCH (n:ConnectionSet{datasetBodyIds:\"test:8426959:2589725\"}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, connectionSetCount3);

        Map<String, Object> connectionSet = session.readTransaction(tx -> tx.run("MATCH (n:ConnectionSet{datasetBodyIds:\"test:8426959:1\"}) RETURN n")).single().get(0).asMap();
        String roiInfoString2 = (String) connectionSet.get("roiInfo");
        Map<String, SynapseCounterWithHighPrecisionCounts> roiInfo2 = gson.fromJson(roiInfoString2, new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
        }.getType());
        Assert.assertEquals(2, roiInfo2.size());
        Assert.assertTrue(roiInfo2.containsKey("roiA"));
        Assert.assertTrue(roiInfo2.containsKey("roiB"));
        Assert.assertEquals(1, roiInfo2.get("roiA").getPre());
        Assert.assertEquals(1, roiInfo2.get("roiA").getPreHP());
        Assert.assertEquals(0, roiInfo2.get("roiA").getPost());
        Assert.assertEquals(0, roiInfo2.get("roiB").getPre());
        Assert.assertEquals(1, roiInfo2.get("roiB").getPost());
        Assert.assertEquals(1, roiInfo2.get("roiB").getPostHP());

        Map<String, Object> connectionSet2 = session.readTransaction(tx -> tx.run("MATCH (n:ConnectionSet{datasetBodyIds:\"test:8426959:2\"}) RETURN n")).single().get(0).asMap();
        String roiInfoString3 = (String) connectionSet2.get("roiInfo");
        Map<String, SynapseCounterWithHighPrecisionCounts> roiInfo3 = gson.fromJson(roiInfoString3, new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
        }.getType());
        Assert.assertEquals(2, roiInfo3.size());
        Assert.assertTrue(roiInfo3.containsKey("roiA"));
        Assert.assertTrue(roiInfo3.containsKey("roiB"));
        Assert.assertEquals(1, roiInfo3.get("roiA").getPre());
        Assert.assertEquals(1, roiInfo3.get("roiA").getPreHP());
        Assert.assertEquals(0, roiInfo3.get("roiA").getPost());
        Assert.assertEquals(0, roiInfo3.get("roiB").getPre());
        Assert.assertEquals(1, roiInfo3.get("roiB").getPost());
        Assert.assertEquals(1, roiInfo3.get("roiB").getPostHP());

        // check connects to relationships
        int connectsToCount2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})-[c:ConnectsTo]->(m{bodyId:26311}) RETURN count(c)")).single().get(0).asInt();
        Assert.assertEquals(0, connectsToCount2);
        int reverseConnectsToCount2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})<-[c:ConnectsTo]-(m{bodyId:26311}) RETURN count(c)")).single().get(0).asInt();
        Assert.assertEquals(1, reverseConnectsToCount2);
        int connectsToCount3 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})-[c:ConnectsTo]->(m{bodyId:2589725}) RETURN count(c)")).single().get(0).asInt();
        Assert.assertEquals(0, connectsToCount3);
        int connectsToWeight = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})-[c:ConnectsTo]->(m{bodyId:1}) RETURN c.weight")).single().get(0).asInt();
        Assert.assertEquals(1, connectsToWeight);
        int connectsToWeightHP = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})-[c:ConnectsTo]->(m{bodyId:1}) RETURN c.weightHP")).single().get(0).asInt();
        Assert.assertEquals(1, connectsToWeightHP);
        int connectsToWeight2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})-[c:ConnectsTo]->(m{bodyId:2}) RETURN c.weight")).single().get(0).asInt();
        Assert.assertEquals(1, connectsToWeight2);
        int connectsToWeightHP2 = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959})-[c:ConnectsTo]->(m{bodyId:2}) RETURN c.weightHP")).single().get(0).asInt();
        Assert.assertEquals(1, connectsToWeightHP2);

        // check affected neuron
        Node neuron2 = session.readTransaction(tx -> tx.run("MATCH (m:Segment:`test-Segment`{bodyId:8426959}) RETURN m")).single().get(0).asNode();
        // should still be neuron
        Assert.assertTrue(neuron2.hasLabel("Neuron"));
        Assert.assertTrue(neuron2.hasLabel("test-Neuron"));
        // check properties
        Map<String, Object> neuronMap2 = neuron2.asMap();
        Assert.assertEquals(1L, neuronMap2.get("pre"));
        Assert.assertEquals(3L, neuronMap2.get("post"));

        String roiInfoString4 = (String) neuronMap2.get("roiInfo");
        Map<String, SynapseCounter> roiInfo4 = gson.fromJson(roiInfoString4, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
        Assert.assertEquals(2, roiInfo4.keySet().size());
        Assert.assertTrue(roiInfo4.containsKey("roiA"));
        Assert.assertTrue(roiInfo4.containsKey("roiB"));
        Assert.assertEquals(1, roiInfo4.get("roiA").getPre());
        Assert.assertEquals(3, roiInfo4.get("roiA").getPost());
        Assert.assertEquals(0, roiInfo4.get("roiB").getPre());
        Assert.assertEquals(1, roiInfo4.get("roiB").getPost());

        // check rois on neuron
        List<Object> segmentRois2 = session.readTransaction(tx -> tx.run("WITH neuprint.getSegmentRois(8426959,'test') AS roiList RETURN roiList")).single().get(0).asList();
        Assert.assertEquals(2, segmentRois2.size());
        Assert.assertTrue(segmentRois2.contains("roiA"));
        Assert.assertTrue(segmentRois2.contains("roiB"));

        // this body shouldn't be a neuron after synapse deletion
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSynapse($x,$y,$z,$dataset)", parameters("x", 4298, "y", 2294, "z", 1542, "dataset", "test")));
        Node neuron3 = session.readTransaction(tx -> tx.run("MATCH (m:Segment:`test-Segment`{bodyId:2589725}) RETURN m")).single().get(0).asNode();
        Assert.assertFalse(neuron3.hasLabel("Neuron"));
        Assert.assertFalse(neuron3.hasLabel("test-Neuron"));

    }

    @Test
    public void shouldOrphanButNotDeleteSynapse() {

        Session session = driver.session();

        session.writeTransaction(tx -> tx.run("CALL proofreader.orphanSynapse($x,$y,$z,$dataset)", parameters("x", 9, "y", 9, "z", 9, "dataset", "test")));

        int synapseCount = session.readTransaction(tx -> tx.run("WITH point({ x:9, y:9, z:9 }) AS loc MATCH (n:`test-Synapse`:Synapse:PreSyn{location:loc}) RETURN count(n)")).single().get(0).asInt();
        // synapse should still exist
        Assert.assertEquals(1, synapseCount);

        int connectedSynapseCount = session.readTransaction(tx -> tx.run("WITH point({ x:9, y:9, z:9 }) AS loc MATCH (n:`test-Synapse`:Synapse:PreSyn{location:loc})-[:SynapsesTo]->(m) RETURN count(m)")).single().get(0).asInt();
        // synapse should still have SynapsesTo relationships
        Assert.assertEquals(2, connectedSynapseCount);

        int containsCount = session.readTransaction(tx -> tx.run("WITH point({ x:9, y:9, z:9 }) AS loc MATCH (n:`test-Synapse`:Synapse:PreSyn{location:loc})<-[:Contains]-(c) RETURN count(c)")).single().get(0).asInt();
        // synapse should not be contained by any SynapseSet or ConnectionSet
        Assert.assertEquals(0, containsCount);

        // connection sets and connects to should be updated
        int connectionSetCount = session.readTransaction(tx -> tx.run("MATCH (n:ConnectionSet{datasetBodyIds:\"test:2:26311\"}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, connectionSetCount);
        int connectsToCount = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:2})-[c:ConnectsTo]->(m{bodyId:26311}) RETURN count(c)")).single().get(0).asInt();
        Assert.assertEquals(0, connectsToCount);

        // containing neuron should be updated
        Node neuron = session.readTransaction(tx -> tx.run("MATCH (m:Segment:`test-Segment`{bodyId:2}) RETURN m")).single().get(0).asNode();
        // check properties
        Map<String, Object> neuronMap = neuron.asMap();
        Assert.assertEquals(0L, neuronMap.get("pre"));

        String roiInfoString = (String) neuronMap.get("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> roiInfo = gson.fromJson(roiInfoString, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
        Assert.assertEquals(1, roiInfo.keySet().size());
        Assert.assertTrue(roiInfo.containsKey("roiB"));

        // should do nothing if already orphaned
        session.writeTransaction(tx -> tx.run("CALL proofreader.orphanSynapse($x,$y,$z,$dataset)", parameters("x", 9, "y", 9, "z", 9, "dataset", "test")));

    }

    @Test
    public void shouldAddOrphanSynapseToSegment() {

        Session session = driver.session();

        Node beforeNeuronNode = session.readTransaction(tx -> tx.run("MATCH (m:Segment:`test-Segment`{bodyId:831744}) RETURN m")).single().get(0).asNode();
        Map<String, Object> beforeNeuronNodeMap = beforeNeuronNode.asMap();
        String beforeRoiInfoString = (String) beforeNeuronNodeMap.get("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> beforeRoiInfo = gson.fromJson(beforeRoiInfoString, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        String postSynapseJson = "{ \"Type\": \"pre\", \"Location\": [ 876,876,876 ], \"Confidence\": .88, \"rois\": [ \"test3\", \"test4\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJson, "dataset", "test")));

        String postSynapseJson2 = "{ \"Type\": \"post\", \"Location\": [ 792,792,792 ], \"Confidence\": .88, \"rois\": [ \"test5\", \"test3\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJson2, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(876,876,876,792,792,792,\"test\")"));

        String postSynapseJsonb = "{ \"Type\": \"post\", \"Location\": [ 79,79,79 ], \"Confidence\": .88, \"rois\": [ \"test5\", \"test3\" ] }";
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", postSynapseJsonb, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(876,876,876,79,79,79,\"test\")"));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapseToSegment($x,$y,$z,$bodyId,$dataset)", parameters("x", 876, "y", 876, "z", 876, "bodyId", 831744, "dataset", "test")));

        int synapseOnSynapseSetCount = session.readTransaction(tx -> tx.run("WITH point({ x:876, y:876, z:876 }) AS loc MATCH (m:Segment:`test-Segment`{bodyId:831744})-[:Contains]->(:SynapseSet)-[:Contains]->(p:PreSyn{location:loc}) RETURN count(p)")).single().get(0).asInt();
        Assert.assertEquals(1, synapseOnSynapseSetCount);

        Node neuronNode = session.readTransaction(tx -> tx.run("MATCH (m:Segment:`test-Segment`{bodyId:831744}) RETURN m")).single().get(0).asNode();
        Assert.assertTrue(neuronNode.hasLabel("Neuron"));
        Map<String, Object> neuronNodeMap = neuronNode.asMap();
        // has correct rois
        Assert.assertTrue(neuronNodeMap.containsKey("test3"));
        Assert.assertTrue(neuronNodeMap.containsKey("test4"));
        // has correct pre, post, roiInfo
        Assert.assertEquals((Long) beforeNeuronNodeMap.get("pre") + 1L, neuronNodeMap.get("pre"));
        Assert.assertEquals(beforeNeuronNodeMap.get("post"), neuronNodeMap.get("post"));
        String roiInfoString = (String) neuronNodeMap.get("roiInfo");
        Map<String, SynapseCounter> roiInfo = gson.fromJson(roiInfoString, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
        Assert.assertEquals(beforeRoiInfo.keySet().size() + 2, roiInfo.keySet().size());
        Assert.assertEquals(1, roiInfo.get("test3").getPre());
        Assert.assertEquals(1, roiInfo.get("test4").getPre());

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapseToSegment($x,$y,$z,$bodyId,$dataset)", parameters("x", 792, "y", 792, "z", 792, "bodyId", 100569, "dataset", "test")));

        int synapseOnSynapseSetCount2 = session.readTransaction(tx -> tx.run("WITH point({ x:792, y:792, z:792 }) AS loc MATCH (m:Segment:`test-Segment`{bodyId:100569})-[:Contains]->(:SynapseSet:`test-SynapseSet`:test)-[:Contains]->(p:PostSyn{location:loc}) RETURN count(p)")).single().get(0).asInt();
        Assert.assertEquals(1, synapseOnSynapseSetCount2);

        Node neuronNode2 = session.readTransaction(tx -> tx.run("MATCH (m:Segment:`test-Segment`{bodyId:100569}) RETURN m")).single().get(0).asNode();
        Assert.assertTrue(neuronNode2.hasLabel("Neuron"));
        Map<String, Object> neuronNodeMap2 = neuronNode2.asMap();
        // has correct rois
        Assert.assertTrue(neuronNodeMap2.containsKey("test3"));
        Assert.assertTrue(neuronNodeMap2.containsKey("test5"));
        // has correct pre, post, roiInfo
        Assert.assertEquals(1L, neuronNodeMap2.get("post"));
        String roiInfoString2 = (String) neuronNodeMap2.get("roiInfo");
        Map<String, SynapseCounter> roiInfo2 = gson.fromJson(roiInfoString2, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
        Assert.assertEquals(2, roiInfo2.keySet().size());
        Assert.assertEquals(1, roiInfo2.get("test3").getPost());
        Assert.assertEquals(1, roiInfo2.get("test5").getPost());

        // check connections and connection sets
        Map<String, Object> resultMap = session.readTransaction(tx -> tx.run("MATCH (m:Segment{bodyId:100569})<-[c:ConnectsTo]-(n:Segment{bodyId:831744}) RETURN c.weight, c.weightHP")).single().asMap();
        Assert.assertEquals(1L, resultMap.get("c.weight"));
        Assert.assertEquals(1L, resultMap.get("c.weightHP"));

        Node connectionSetNode = session.readTransaction(tx -> tx.run("MATCH (m:Segment{bodyId:100569})<-[:To]-(cs:ConnectionSet:`test-ConnectionSet`:test{datasetBodyIds:'test:831744:100569'})-[:From]->(n:Segment{bodyId:831744}) RETURN cs")).single().get(0).asNode();
        Map<String, Object> connectionSetMap = connectionSetNode.asMap();
        String csRoiInfoString = (String) connectionSetMap.get("roiInfo");
        Map<String, SynapseCounterWithHighPrecisionCounts> csRoiInfo = gson.fromJson(csRoiInfoString, new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
        }.getType());

        Assert.assertEquals(3, csRoiInfo.size());
        Assert.assertEquals(1, csRoiInfo.get("test3").getPre());
        Assert.assertEquals(1, csRoiInfo.get("test3").getPreHP());
        Assert.assertEquals(1, csRoiInfo.get("test3").getPost());
        Assert.assertEquals(1, csRoiInfo.get("test3").getPostHP());

        Assert.assertEquals(1, csRoiInfo.get("test4").getPre());
        Assert.assertEquals(1, csRoiInfo.get("test4").getPreHP());
        Assert.assertEquals(0, csRoiInfo.get("test4").getPost());
        Assert.assertEquals(0, csRoiInfo.get("test4").getPostHP());

        Assert.assertEquals(0, csRoiInfo.get("test5").getPre());
        Assert.assertEquals(0, csRoiInfo.get("test5").getPreHP());
        Assert.assertEquals(1, csRoiInfo.get("test5").getPost());
        Assert.assertEquals(1, csRoiInfo.get("test5").getPostHP());


        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapseToSegment($x,$y,$z,$bodyId,$dataset)", parameters("x", 79, "y", 79, "z", 79, "bodyId", 100569, "dataset", "test")));

        // check for duplicate connection set contains relationships
        int dupContainsCount = session.readTransaction(tx -> tx.run("MATCH (cs:`test-ConnectionSet`)-[:Contains]->(s:Synapse) WITH cs,s \n" +
                "MATCH path=(cs)-[:Contains]->(s:Synapse)\n" +
                "WITH COUNT(path) as cp,cs,s WHERE cp>1 RETURN count(cs)")).single().get(0).asInt();
        Assert.assertEquals(0, dupContainsCount);



    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfNonexistentSynapseAddedToSegment() {
        Session session = driver.session();

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapseToSegment($x,$y,$z,$bodyId,$dataset)", parameters("x", 909, "y", 909, "z", 909, "bodyId", 8426959, "dataset", "test")));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfSegmentDoesNotExist() {
        Session session = driver.session();

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapseToSegment($x,$y,$z,$bodyId,$dataset)", parameters("x", 9, "y", 9, "z", 9, "bodyId", 800800, "dataset", "test")));

    }

}
