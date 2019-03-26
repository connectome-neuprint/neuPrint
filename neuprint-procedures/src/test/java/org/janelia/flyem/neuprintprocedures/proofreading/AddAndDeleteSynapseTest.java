package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.convert.Json;
import apoc.create.Create;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
import org.janelia.flyem.neuprint.SynapseMapper;
import org.janelia.flyem.neuprint.model.BodyWithSynapses;
import org.janelia.flyem.neuprint.model.Neuron;
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
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F, .20F, .80F, true);
        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 1);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldAddSynapseNode() {

        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"post\", \"Location\": [ 1,2,3 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson ,"dataset", "test")));

        int synapseCount = session.readTransaction(tx -> tx.run("WITH point({ x:1, y:2, z:3 }) AS loc MATCH (n:`test-Synapse`:Synapse{location:loc}) RETURN count(n)")).single().get(0).asInt();
        // a single synapse node should exist
        Assert.assertEquals(1,synapseCount);

        Node synapseNode = session.readTransaction(tx -> tx.run("WITH point({ x:1, y:2, z:3 }) AS loc MATCH (n:`test-Synapse`:Synapse{location:loc}) RETURN n")).single().get(0).asNode();
        Map<String,Object> synapseNodeAsMap = synapseNode.asMap();

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


    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldThrowExceptionIfSynapseAtLocationAlreadyExists() {

        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"post\", \"Location\": [ 4301, 2276, 1535 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson ,"dataset", "test")));
    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldThrowExceptionIfSynapseDoesNotHaveType() {

        Session session = driver.session();

        String synapseJson = "{ \"Location\": [ 0,1,2 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson ,"dataset", "test")));
    }

    @Test(expected = org.neo4j.driver.v1.exceptions.ClientException.class)
    public void shouldThrowExceptionIfSynapseHasTypeOtherThanPreOrPost() {

        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"other\", \"Location\": [ 5,6,7 ], \"Confidence\": .88, \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson ,"dataset", "test")));
    }

    @Test
    public void confidenceShouldBe0IfNotSpecified() {

        Session session = driver.session();

        String synapseJson = "{ \"Type\": \"pre\", \"Location\": [ 5,6,7], \"rois\": [ \"test1\", \"test2\" ] }";

        session.writeTransaction(tx -> tx.run("CALL proofreader.addSynapse($synapseJson,$dataset)", parameters("synapseJson", synapseJson ,"dataset", "test")));

        Node synapseNode = session.readTransaction(tx -> tx.run("WITH point({ x:5, y:6, z:7 }) AS loc MATCH (n:`test-Synapse`:Synapse{location:loc}) RETURN n")).single().get(0).asNode();
        Map<String,Object> synapseNodeAsMap = synapseNode.asMap();

        Assert.assertEquals(0.0, (double) synapseNodeAsMap.get("confidence"), 0.0001);

    }

}
