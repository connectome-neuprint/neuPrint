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
import org.janelia.flyem.neuprint.model.Skeleton;
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
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Point;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.neo4j.driver.v1.Values.parameters;

public class AddAndUpdateNeuronsTest {

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
    public static void before() throws InterruptedException {

        File swcFile1 = new File("src/test/resources/8426959.swc");
        File swcFile2 = new File("src/test/resources/831744.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

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
        neo4jImporter.addConnectionSets(dataset, bodyList, mapper.getSynapseLocationToBodyIdMap(), .2F, .8F, true);
        neo4jImporter.addSynapseSets(dataset, bodyList);
        neo4jImporter.addSkeletonNodes(dataset, skeletonList);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F, .20F, .80F, true);
        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 1);

        String updateJson =
                "{" +
                        "\"Id\": 8426959," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"28841c8277e044a7b187dda03e18da13\"," +
                        "\"MutationID\": 1000057479," +
                        "\"Status\": \"updated\"," +
                        "\"Soma\": {" +
                        "\"Location\": [14067, 10777, 15040]," +
                        "\"Radius\": 15040.0 }," +
                        "\"Name\": \"new name\", " +
                        "\"SynapseSources\": [831744,2589725]," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "{" +
                        "\"Location\": [4287, 2277, 1542]," +
                        "\"Type\": \"pre\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4222, 2402, 1688]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4287, 2277, 1502]," +
                        "\"Type\": \"pre\"" +
                        "}," +
                        "{" +
                        "\"Location\": [8000,7000,6000]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4000,5000,6000]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4298, 2294, 1542]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4292, 2261, 1542]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [1000, 2000, 3000]," +
                        "\"Type\": \"pre\"" +
                        "}" +
                        "]" +
                        "}";

        Session session = driver.session();

        long[] bodyIdsToDelete = new long[]{831744L, 2589725L, 8426959L};

        for (int i = 0; i < bodyIdsToDelete.length; i++) {
            int finalI = i;
            session.writeTransaction(tx -> tx.run("CALL proofreader.deleteNeuron($bodyId, $dataset)", parameters("bodyId", bodyIdsToDelete[finalI], "dataset", "test")));
        }

        TimeUnit.SECONDS.sleep(5);

        Gson gson = new Gson();
        NeuronAddition neuronAddition = gson.fromJson(updateJson, NeuronAddition.class);
        String neuronUpdateJson = gson.toJson(neuronAddition);
        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson, $dataset)", parameters("updateJson", neuronUpdateJson, "dataset", "test")));

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldDeleteNeuronsAndAssociatedNodes() {

        Session session = driver.session();

        // make sure neurons are deleted
        int deletedNeuronCount = session.readTransaction(tx -> tx.run("MATCH (n:Neuron) WHERE n.bodyId=2589725 OR n.bodyId=831744 RETURN count(n)").single().get(0).asInt());
        Assert.assertEquals(0, deletedNeuronCount);
        // connection sets are deleted
        int connectionSetsCount = session.readTransaction(tx -> tx.run("MATCH (n:ConnectionSet) WHERE n.datasetBodyIds=\"test:8426959:2589725\" OR n.datasetBodyIds=\"test:8426959:831744\" RETURN count(n)").single().get(0).asInt());
        Assert.assertEquals(0, connectionSetsCount);
        // skeletons are deleted
        int skeletonCount = session.readTransaction(tx -> tx.run("MATCH (n:Skeleton) RETURN count(n)").single().get(0).asInt());
        Assert.assertEquals(0, skeletonCount);
        // skelnodes are deleted
        int skelNodeCount = session.readTransaction(tx -> tx.run("MATCH (n:SkelNode) RETURN count(n)").single().get(0).asInt());
        Assert.assertEquals(0, skelNodeCount);
    }

    @Test
    public void shouldCreateANewNeuron() {

        Session session = driver.session();

        // new body should exist
        int newBodyCount = session.readTransaction(tx -> tx.run("MATCH (n:Segment:`test-Segment`:test{bodyId:8426959}) RETURN count(n)").single().get(0).asInt());
        Assert.assertEquals(1, newBodyCount);
    }

    @Test
    public void shouldContainANewSynapseSetThatCombinesNecessarySynapseSets() {
        Session session = driver.session();

        // with new synapse set that combines the necessary synapse sets
        int newSynapseSetCount = session.readTransaction(tx -> tx.run("MATCH (n:Segment{bodyId:8426959})-[:Contains]->(s:SynapseSet{datasetBodyId:\"test:8426959\"}) RETURN count(s)").single().get(0).asInt());
        Assert.assertEquals(1, newSynapseSetCount);
        List<Record> newSynapses = session.readTransaction(tx -> tx.run("MATCH (s:SynapseSet{datasetBodyId:\"test:8426959\"})-[:Contains]->(p:Synapse) RETURN p").list());
        Set<Point> locationPointSet = new HashSet<>();
        locationPointSet.add(Values.point(9157, 4287, 2277, 1542).asPoint());
        locationPointSet.add(Values.point(9157, 4222, 2402, 1688).asPoint());
        locationPointSet.add(Values.point(9157, 4287, 2277, 1502).asPoint());
        locationPointSet.add(Values.point(9157, 8000, 7000, 6000).asPoint());
        locationPointSet.add(Values.point(9157, 4000, 5000, 6000).asPoint());
        locationPointSet.add(Values.point(9157, 4298, 2294, 1542).asPoint());
        locationPointSet.add(Values.point(9157, 4292, 2261, 1542).asPoint());
        locationPointSet.add(Values.point(9157, 1000, 2000, 3000).asPoint());

        for (Record synapse : newSynapses) {
            Point synapseLocation = (Point) synapse.get("p").asMap().get("location");
            Assert.assertTrue(locationPointSet.contains(synapseLocation));
            locationPointSet.remove(synapseLocation);
        }
        Assert.assertTrue(locationPointSet.isEmpty());
    }

    @Test
    public void allSynapsesShouldBeAssigned() {

        Session session = driver.session();
        int unassignedSynapseCount = session.readTransaction(tx -> tx.run("MATCH (n:Synapse) WHERE NOT (n)<-[:Contains]-(:SynapseSet) RETURN count(n)").single().get(0).asInt());
        Assert.assertEquals(0, unassignedSynapseCount);
    }

    @Test
    public void shouldHaveAppropriateConnectsToRelationships() {

        Session session = driver.session();

        // should have appropriate connects to

        int weight_8426959To8426959 = session.readTransaction(tx -> tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})<-[r:ConnectsTo]-(s{bodyId:8426959}) RETURN r.weight")).single().get(0).asInt();
        Assert.assertEquals(3, weight_8426959To8426959);

        int weight_8426959To26311 = session.readTransaction(tx -> tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:26311})<-[r:ConnectsTo]-(s{bodyId:8426959}) RETURN r.weight")).single().get(0).asInt();
        Assert.assertEquals(2, weight_8426959To26311);

        int weight_26311To8426959 = session.readTransaction(tx -> tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:26311})-[r:ConnectsTo]->(s{bodyId:8426959}) RETURN r.weight")).single().get(0).asInt();
        Assert.assertEquals(2, weight_26311To8426959);

        // weight should be equal to the number of psds per connection (assuming no many pre to one post connections)
        List<Record> connections = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`)-[c:ConnectsTo]->(m), (cs:ConnectionSet)-[:Contains]->(s:PostSyn) WHERE cs.datasetBodyIds=\"test:\" + n.bodyId + \":\" + m.bodyId RETURN n.bodyId, m.bodyId, c.weight, cs.datasetBodyIds, count(s)")).list();
        for (Record record : connections) {
            Assert.assertEquals(record.asMap().get("c.weight"), record.asMap().get("count(s)"));
        }

        // weightHP should be equal to the number of high-precision psds per connection (assuming no many pre to one post connections)
        List<Record> connectionsHP = session.run("MATCH (n:`test-Segment`)-[c:ConnectsTo]->(m), (n)<-[:From]-(cs:ConnectionSet)-[:To]->(m), (cs)-[:Contains]->(s:PostSyn) WHERE s.confidence>.81 RETURN n.bodyId, m.bodyId, c.weightHP, count(s)").list();
        for (Record record : connectionsHP) {
            Assert.assertSame(record.asMap().get("c.weightHP"), record.asMap().get("count(s)"));
        }
    }

    @Test
    public void shouldHaveAppropriateConnectionSets() {

        Session session = driver.session();

        // should have appropriate connectionsets

        List<Record> synapseCS_8426959_26311 = session.readTransaction(tx -> tx.run("MATCH (t:ConnectionSet:test:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:26311\"})-[:Contains]->(s) RETURN s")).list();
        Assert.assertEquals(4, synapseCS_8426959_26311.size());
        Set<Node> connectionSet = synapseCS_8426959_26311.stream().map(Record::asMap).map(m -> (Node) m.get("s")).collect(Collectors.toSet());
        Set<Point> locationSet = connectionSet.stream().map(Node::asMap).map(m -> (Point) m.get("location")).collect(Collectors.toSet());
        Set<Point> expectedLocationSet = new HashSet<>();
        expectedLocationSet.add(Values.point(9157, 4287, 2277, 1542).asPoint());
        expectedLocationSet.add(Values.point(9157, 4301, 2276, 1535).asPoint());
        expectedLocationSet.add(Values.point(9157, 9000, 8000, 7000).asPoint());
        expectedLocationSet.add(Values.point(9157, 1000, 2000, 3000).asPoint());

        Assert.assertEquals(locationSet, expectedLocationSet);

        int connectionSetFromCount = session.readTransaction(tx -> tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})<-[:From]-(c:ConnectionSet) WITH DISTINCT c AS cs RETURN count(cs)")).single().get(0).asInt();

        Assert.assertEquals(2, connectionSetFromCount);

        int connectionSetToCount = session.readTransaction(tx -> tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})<-[:To]-(c:ConnectionSet) WITH DISTINCT c AS cs RETURN count(cs)")).single().get(0).asInt();

        Assert.assertEquals(2, connectionSetToCount);

        // check connection set roiInfo
        int countOfConnectionSetsWithoutRoiInfo = session.run("MATCH (t:ConnectionSet) WHERE NOT exists(t.roiInfo) RETURN count(t)").single().get("count(t)").asInt();

        Assert.assertEquals(0, countOfConnectionSetsWithoutRoiInfo);

        String roiInfoString = session.readTransaction(tx -> tx.run("MATCH (t:ConnectionSet:test:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:26311\"}) RETURN t.roiInfo")).single().get("t.roiInfo").asString();

        Assert.assertNotNull(roiInfoString);

        Gson gson = new Gson();
        Map<String, SynapseCounterWithHighPrecisionCounts> roiInfo = gson.fromJson(roiInfoString, new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
        }.getType());

        Assert.assertEquals(1, roiInfo.size());

        Assert.assertEquals(2, roiInfo.get("roiA").getPre());
        Assert.assertEquals(1, roiInfo.get("roiA").getPreHP());
        Assert.assertEquals(2, roiInfo.get("roiA").getPost());
        Assert.assertEquals(1, roiInfo.get("roiA").getPostHP());

    }

    @Test
    public void shouldHaveAppropriateRoisAndRoiInfo() {

        Session session = driver.session();

        // should have appropriate rois and roiInfo
        Node newNeuron = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:8426959}) RETURN n")).single().get(0).asNode();

        Assert.assertTrue(newNeuron.asMap().containsKey("roiA"));
        Assert.assertTrue(newNeuron.asMap().containsKey("roiB"));
        Assert.assertTrue(newNeuron.asMap().containsKey("roi'C"));

        Gson gson = new Gson();
        Map<String, SynapseCounter> roiInfo = gson.fromJson((String) newNeuron.asMap().get("roiInfo"), new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertEquals(3, roiInfo.keySet().size());
        Assert.assertEquals(3, roiInfo.get("roiA").getPre());
        Assert.assertEquals(4, roiInfo.get("roiA").getPost());
        Assert.assertEquals(0, roiInfo.get("roiB").getPre());
        Assert.assertEquals(3, roiInfo.get("roiB").getPost());
        Assert.assertEquals(0, roiInfo.get("roi'C").getPre());
        Assert.assertEquals(1, roiInfo.get("roi'C").getPost());
    }

    @Test
    public void shouldHaveAppropriateProperties() {

        Session session = driver.session();

        Node newNeuron = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:8426959}) RETURN n")).single().get(0).asNode();

        // should have appropriate pre post counts and other props
        Assert.assertEquals(3L, newNeuron.asMap().get("pre"));
        Assert.assertEquals(5L, newNeuron.asMap().get("post"));
        Assert.assertEquals(12L, newNeuron.asMap().get("size"));
        Assert.assertEquals("28841c8277e044a7b187dda03e18da13:1000057479:8426959", newNeuron.asMap().get("mutationUuidAndId"));
        Assert.assertEquals("updated", newNeuron.asMap().get("status"));
        Assert.assertEquals("new name", newNeuron.asMap().get("name"));
        Assert.assertEquals(15040.0, newNeuron.asMap().get("somaRadius"));
        Assert.assertEquals(Values.point(9157, 14067, 10777, 15040).asPoint(), newNeuron.asMap().get("somaLocation"));

    }

    @Test
    public void shouldBeLabeledNeuron() {

        Session session = driver.session();

        int neuronCount = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`:Neuron{bodyId:8426959}) RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(1, neuronCount);
    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldNotBeAbleToAddSameMutationTwice() {

        Session session = driver.session();

        String updateJson =
                "{" +
                        "\"Id\": 8426959," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"28841c8277e044a7b187dda03e18da13\"," +
                        "\"MutationID\": 1000057479," +
                        "\"Status\": \"updated\"," +
                        "\"Soma\": {" +
                        "\"Location\": [14067, 10777, 15040]," +
                        "\"Radius\": 15040.0 }," +
                        "\"Name\": \"new name\", " +
                        "\"SynapseSources\": [831744,2589725]," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "{" +
                        "\"Location\": [4287, 2277, 1542]," +
                        "\"Type\": \"pre\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4222, 2402, 1688]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4287, 2277, 1502]," +
                        "\"Type\": \"pre\"" +
                        "}," +
                        "{" +
                        "\"Location\": [8000,7000,6000]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4000,5000,6000]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4298, 2294, 1542]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [4292, 2261, 1542]," +
                        "\"Type\": \"post\"" +
                        "}," +
                        "{" +
                        "\"Location\": [1000, 2000, 3000]," +
                        "\"Type\": \"pre\"" +
                        "}" +
                        "]" +
                        "}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson, "dataset", "test")));

    }

    @Test
    public void shouldNotErrorWhenUpdatingNonexistentNeurons() {

        Session session = driver.session();

        String updateJson =
                "{" +
                        "\"Id\": 1," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"4\"," +
                        "\"MutationID\": 4," +
                        "\"Status\": \"updated\"," +
                        "\"Soma\": {" +
                        "\"Location\": [14067, 10777, 15040]," +
                        "\"Radius\": 15040.0 }," +
                        "\"Name\": \"new name\", " +
                        "\"SynapseSources\": []," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "]" +
                        "}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson, "dataset", "test")));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfSynapseNotFound() {

        Session session = driver.session();

        String updateJson =
                "{" +
                        "\"Id\": 2," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"6\"," +
                        "\"MutationID\": 6," +
                        "\"Status\": \"updated\"," +
                        "\"Soma\": {" +
                        "\"Location\": [14067, 10777, 15040]," +
                        "\"Radius\": 15040.0 }," +
                        "\"Name\": \"new name\", " +
                        "\"SynapseSources\": [8426959]," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "{" +
                        "\"Location\": [4,5,6]," +
                        "\"Type\": \"pre\"" +
                        "}" +
                        "]" +
                        "}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson, "dataset", "test")));

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldErrorIfBodyIdAlreadyExists() {

        Session session = driver.session();

        String updateJson =
                "{" +
                        "\"Id\": 8426959," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"67\"," +
                        "\"MutationID\": 67," +
                        "\"Status\": \"updated\"," +
                        "\"Soma\": {" +
                        "\"Location\": [14067, 10777, 15040]," +
                        "\"Radius\": 15040.0 }," +
                        "\"Name\": \"new name\", " +
                        "\"SynapseSources\": [8426959]," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "]" +
                        "}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson, "dataset", "test")));

    }

    @Test
    public void shouldAddMutationIdAndUuidToMetaNode() {

        Session session = driver.session();

        String updateJson =
                "{" +
                        "\"Id\": 234," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"20\"," +
                        "\"MutationID\": 21," +
                        "\"Status\": \"updated\"," +
                        "\"SynapseSources\": []," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "]" +
                        "}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson, "dataset", "test")));

        List<Record> metaRecords = session.readTransaction(tx -> tx.run("MATCH (n:`Meta`{dataset:\"test\"}) RETURN n.latestMutationId, n.uuid")).list();

        for (Record r : metaRecords) {
            Assert.assertEquals(21L, r.asMap().get("n.latestMutationId"));
            Assert.assertEquals("20", r.asMap().get("n.uuid"));
        }

    }

    @Test
    public void allNodesShouldHaveDatasetLabel() {

        Session session = driver.session();

        int noDatasetLabelCount = session.readTransaction(tx -> tx.run("MATCH (n) WHERE NOT n:test AND NOT n:Meta AND NOT n:DataModel RETURN count(n)")).single().get(0).asInt();
        Assert.assertEquals(0, noDatasetLabelCount);

    }

    @Test
    public void propertiesShouldBeUpdatedUponProcedureCall() {

        Session session = driver.session();

        String neuronObjectJson = "{ \"Id\":222, \"Status\":\"Partially Roughly traced\", \"Name\":\"KB(a)\", \"Size\": 346576}";

        session.writeTransaction(tx -> tx.run("CREATE (n:`test-Segment`:Segment:test) SET n.bodyId=222, n.pre=2, n.post=5, n.roiInfo=\"{'roiA':{'pre':2,'post':0},'roiB':{'pre':0,'post':5}}\"", parameters("neuronObjectJson", neuronObjectJson, "dataset", "test")));

        session.writeTransaction(tx -> tx.run("CALL proofreader.updateProperties($neuronObjectJson,$dataset)", parameters("neuronObjectJson", neuronObjectJson, "dataset", "test")));

        Node neuronNode = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:222}) RETURN n")).single().get(0).asNode();

        Assert.assertEquals("Partially Roughly traced", neuronNode.asMap().get("status"));
        Assert.assertEquals("KB(a)", neuronNode.asMap().get("name"));
        Assert.assertEquals(346576L, neuronNode.asMap().get("size"));

        Assert.assertTrue(neuronNode.hasLabel("Neuron"));
        Assert.assertTrue(neuronNode.hasLabel("test-Neuron"));
        Assert.assertTrue(neuronNode.asMap().containsKey("clusterName"));

        //soma addition
        String neuronObjectJson2 = "{ \"Id\":222, \"Soma\": { \"Location\":[1,2,3],\"Radius\":5.0}}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.updateProperties($neuronObjectJson,$dataset)", parameters("neuronObjectJson", neuronObjectJson2, "dataset", "test")));
        Node neuronNode2 = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:222}) RETURN n")).single().get(0).asNode();

        Assert.assertEquals(5.0D, neuronNode2.asMap().get("somaRadius"));
        Assert.assertEquals(Values.point(9157, 1, 2, 3).asPoint(), neuronNode2.asMap().get("somaLocation"));

        //type addition
        String neuronObjectJson3 = "{ \"Id\":222, \"NeuronType\": \"testType\"}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.updateProperties($neuronObjectJson,$dataset)", parameters("neuronObjectJson", neuronObjectJson3, "dataset", "test")));
        Node neuronNode3 = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:222}) RETURN n")).single().get(0).asNode();

        Assert.assertEquals("testType", neuronNode3.asMap().get("type"));

    }

    @Test
    public void shouldDoNothingIfNeuronNotInDatabase() {

        Session session = driver.session();

        String neuronObjectJson = "{ \"Id\":15, \"Status\":\"Partially Roughly traced\", \"Name\":\"KB(a)\", \"Size\": 346576}";

        Assert.assertFalse(session.writeTransaction(tx -> tx.run("CALL proofreader.updateProperties($neuronObjectJson,$dataset)", parameters("neuronObjectJson", neuronObjectJson, "dataset", "test"))).hasNext());

    }

    @Test
    public void shouldBeAbleToAddSegmentNodesWithoutSynapsesOrMutationId() {

        Session session = driver.session();

        String updateJson =
                "{" +
                        "\"Id\": 999," +
                        "\"Size\": 120," +
                        "\"MutationUUID\": \"auniqueid\"," +
                        "\"Status\": \"updated\"," +
                        "\"Soma\": {" +
                        "\"Location\": [14067, 10777, 15040]," +
                        "\"Radius\": 15040.0 }," +
                        "\"Name\": \"new name\" " +
                        "}";

        Gson gson = new Gson();
        NeuronAddition neuronAddition = gson.fromJson(updateJson, NeuronAddition.class);
        String neuronUpdateJson = gson.toJson(neuronAddition);
        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson, $dataset)", parameters("updateJson", neuronUpdateJson, "dataset", "test")));

        Node synapselessSegment = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:999}) RETURN n")).single().get(0).asNode();

        // should have neuron label because has soma, name, and status
        Assert.assertTrue(synapselessSegment.hasLabel("Neuron"));
        Assert.assertTrue(synapselessSegment.hasLabel("test-Neuron"));

        Assert.assertEquals(120L, (long) synapselessSegment.asMap().get("size"));
        Assert.assertEquals(15040.0, (double) synapselessSegment.asMap().get("somaRadius"), .00001);
        Assert.assertEquals("new name", synapselessSegment.asMap().get("name"));
        Assert.assertEquals("auniqueid:0:999", synapselessSegment.asMap().get("mutationUuidAndId"));

        int synapselessSegmentRelCount = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:999})-->(k) RETURN count(k)")).single().get(0).asInt();

        Assert.assertEquals(1, synapselessSegmentRelCount);

        // all nodes contain a synapse set
        int synapselessSegmentSynapseSetCount = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:999})-[:Contains]->(s:SynapseSet) RETURN count(s)")).single().get(0).asInt();

        Assert.assertEquals(1, synapselessSegmentSynapseSetCount);

        //but no synapses on synapse set
        int synapselessSegmentSynapseCount = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:999})-[:Contains]->(s:SynapseSet)-[:Contains]->(syn) RETURN count(syn)")).single().get(0).asInt();

        Assert.assertEquals(0, synapselessSegmentSynapseCount);

        //should be able to delete this neuron
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteNeuron($bodyId, $dataset)", parameters("bodyId", 999, "dataset", "test")));

        int synapselessSegmentCount = session.readTransaction(tx -> tx.run("MATCH (n:`test-Segment`{bodyId:999}) RETURN count(n)")).single().get(0).asInt();

        Assert.assertEquals(0, synapselessSegmentCount);

    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void synapseShouldNotComeFromAnExistingBody() {

        Session session = driver.session();

        String updateJson =
                "{" +
                        "\"Id\": 678," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"30\"," +
                        "\"MutationID\": 31," +
                        "\"Status\": \"updated\"," +
                        "\"SynapseSources\": []," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "{" +
                        "\"Location\": [1000, 2000, 3000]," +
                        "\"Type\": \"pre\"" +
                        "}" +
                        "]" +
                        "}";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson, "dataset", "test")));

    }

    @Test
    public void shouldBeAbleToAssignOrphanSynapsesToNeuron() {

        Session session = driver.session();

        // tests that synapses created through the addSynapse/addConnection procedures can be added to new neurons
        String updateJson =
                "{" +
                        "\"Id\": 1010102," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"30\"," +
                        "\"MutationID\": 31," +
                        "\"Status\": \"updated\"," +
                        "\"SynapseSources\": []," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "{" +
                        "\"Location\": [5, 22, 99]," +
                        "\"Type\": \"post\"" +
                        "}" +
                        "]" +
                        "}";

        String updateJson2 =
                "{" +
                        "\"Id\": 1012," +
                        "\"Size\": 12," +
                        "\"MutationUUID\": \"30\"," +
                        "\"MutationID\": 31," +
                        "\"Status\": \"updated\"," +
                        "\"SynapseSources\": []," +
                        "\"CurrentSynapses\": " +
                        "[" +
                        "{" +
                        "\"Location\": [5, 22, 9]," +
                        "\"Type\": \"pre\"" +
                        "}" +
                        "]" +
                        "}";

        String synapseJson = "{ \"Type\": \"post\", \"Location\": [ 5,22,99 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";
        String synapseJson2 = "{ \"Type\": \"pre\", \"Location\": [ 5,22,9 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson, "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson2, "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.addConnectionBetweenSynapseNodes(5,22,9,5,22,99,\"test\")"));

        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson, "dataset", "test")));
        session.writeTransaction(tx -> tx.run("CALL proofreader.addNeuron($updateJson,$dataset)", parameters("updateJson", updateJson2, "dataset", "test")));

    }

}
