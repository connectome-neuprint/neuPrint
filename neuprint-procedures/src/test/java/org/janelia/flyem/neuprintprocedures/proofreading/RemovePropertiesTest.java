package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.convert.Json;
import apoc.create.Create;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
import org.janelia.flyem.neuprint.SynapseMapper;
import org.janelia.flyem.neuprint.model.BodyWithSynapses;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Skeleton;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldRemoveSomaRadiusAndLocation() {
        Session session = driver.session();

        // get neuron and add Neuron label since has status and soma
        Map<String, Object> neuronAsMap = session.writeTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=100569 SET n:Neuron, n:`test-Neuron` RETURN n")).single().get(0).asMap();

        Assert.assertTrue(neuronAsMap.containsKey("somaRadius"));
        Assert.assertTrue(neuronAsMap.containsKey("somaLocation"));

        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSoma($bodyId,$datasetLabel)", parameters("bodyId", 100569, "datasetLabel", "test")));

        Map<String, Object> neuronAsMapAfterDelete = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=100569 RETURN n")).single().get(0).asMap();

        Assert.assertFalse(neuronAsMapAfterDelete.containsKey("somaRadius"));
        Assert.assertFalse(neuronAsMapAfterDelete.containsKey("somaLocation"));

        // should not lose neuron status since still has status
        int numNeuronsWithBodyId = session.readTransaction(tx -> tx.run("MATCH (n:Neuron) WHERE n.bodyId=100569 RETURN count(n)")).single().get(0).asInt();
        int numDatasetNeuronsWithBodyId = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`) WHERE n.bodyId=100569 RETURN count(n)")).single().get(0).asInt();

        Assert.assertEquals(1, numNeuronsWithBodyId);
        Assert.assertEquals(1, numDatasetNeuronsWithBodyId);

        // should skip deletion of soma quietly if the soma doesn't exist
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSoma($bodyId,$datasetLabel)", parameters("bodyId", 100569, "datasetLabel", "test")));

    }

    @Test
    public void shouldRemoveName() {
        Session session = driver.session();

        // get neuron and add Neuron label since has name and status
        Map<String, Object> neuronAsMap = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=100554 SET n:Neuron, n:`test-Neuron` RETURN n")).single().get(0).asMap();

        Assert.assertTrue(neuronAsMap.containsKey("name"));

        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteName($bodyId,$datasetLabel)", parameters("bodyId", 100554, "datasetLabel", "test")));

        Map<String, Object> neuronAsMapAfterDelete = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=100554 RETURN n")).single().get(0).asMap();

        Assert.assertFalse(neuronAsMapAfterDelete.containsKey("name"));

        // should not lose neuron status since still has status
        int numNeuronsWithBodyId = session.readTransaction(tx -> tx.run("MATCH (n:Neuron) WHERE n.bodyId=100554 RETURN count(n)")).single().get(0).asInt();
        int numDatasetNeuronsWithBodyId = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`) WHERE n.bodyId=100554 RETURN count(n)")).single().get(0).asInt();

        Assert.assertEquals(1, numNeuronsWithBodyId);
        Assert.assertEquals(1, numDatasetNeuronsWithBodyId);

        // should skip deletion of name quietly if name doesn't exist
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteName($bodyId,$datasetLabel)", parameters("bodyId",  100554, "datasetLabel", "test")));

    }

    @Test
    public void shouldRemoveStatus() {
        Session session = driver.session();

        // get neuron and add Neuron label since has status
        Map<String, Object> neuronAsMap = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=100541 SET n:Neuron, n:`test-Neuron` RETURN n")).single().get(0).asMap();

        Assert.assertTrue(neuronAsMap.containsKey("status"));

        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteStatus($bodyId,$datasetLabel)", parameters("bodyId", 100541, "datasetLabel", "test")));

        Map<String, Object> neuronAsMapAfterDelete = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=100541 RETURN n")).single().get(0).asMap();

        Assert.assertFalse(neuronAsMapAfterDelete.containsKey("status"));

        // should lose neuron status
        int numNeuronsWithBodyId = session.readTransaction(tx -> tx.run("MATCH (n:Neuron) WHERE n.bodyId=100541 RETURN count(n)")).single().get(0).asInt();
        int numDatasetNeuronsWithBodyId = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`) WHERE n.bodyId=100541 RETURN count(n)")).single().get(0).asInt();

        Assert.assertEquals(0, numNeuronsWithBodyId);
        Assert.assertEquals(0, numDatasetNeuronsWithBodyId);

        // should skip deletion of status quietly if status doesn't exist
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteStatus($bodyId,$datasetLabel)", parameters("bodyId",  100541, "datasetLabel", "test")));

    }

    @Test
    public void shouldRemoveType() {
        Session session = driver.session();

        // get neuron and add Neuron label since has status
        Map<String, Object> neuronAsMap = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=8426959 RETURN n")).single().get(0).asMap();

        Assert.assertTrue(neuronAsMap.containsKey("type"));

        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteType($bodyId,$datasetLabel)", parameters("bodyId", 8426959, "datasetLabel", "test")));

        Map<String, Object> neuronAsMapAfterDelete = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=8426959 RETURN n")).single().get(0).asMap();

        Assert.assertFalse(neuronAsMapAfterDelete.containsKey("type"));

        // should skip deletion of type quietly if type doesn't exist
        session.writeTransaction(tx -> tx.run("CALL proofreader.deleteStatus($bodyId,$datasetLabel)", parameters("bodyId",  8426959, "datasetLabel", "test")));

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
}
