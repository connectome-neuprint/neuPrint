package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.convert.Json;
import apoc.create.Create;
import apoc.periodic.Periodic;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprintprocedures.functions.NeuPrintUserFunctions;
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
import java.util.List;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

public class ConnectionSetEditTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(Create.class)
                .withProcedure(Periodic.class)
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
        neo4jImporter.addConnectionSetsOld(dataset, bodyList, mapper.getSynapseLocationToBodyIdMap());
        neo4jImporter.addSynapseSets(dataset, bodyList);
        neo4jImporter.addSkeletonNodes(dataset, skeletonList);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 1);

        Session session = driver.session();

//        session.writeTransaction(tx -> tx.run("CALL apoc.periodic.iterate('MATCH (cs:`test-ConnectionSet`) RETURN cs','WITH cs CALL proofreader.updateConnectionSetRels(cs,$dataset) RETURN cs',{batchSize:10}) YIELD batches, total RETURN batches, total", parameters("dataset", "test")));

        session.writeTransaction(tx -> tx.run("MATCH (cs:`test-ConnectionSet`) WITH cs CALL proofreader.updateConnectionSetRels(cs, $dataset) RETURN cs", parameters("dataset", "test")));

    }

    @Test
    public void shouldHaveCorrectConnectsToWeights() {

        Session session = driver.session();

        // weight is equal to number of psds (can have a 1 pre to many post or 1 pre to 1 post connection, but no many pre to 1 post)
        int weight_8426959To2589725 = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:2589725})<-[r:ConnectsTo]-(s{bodyId:8426959}) RETURN r.weight").single().get(0).asInt();

        Assert.assertEquals(1, weight_8426959To2589725);

        int weight_8426959To26311 = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:26311})<-[r:ConnectsTo]-(s{bodyId:8426959}) RETURN r.weight").single().get(0).asInt();

        Assert.assertEquals(1, weight_8426959To26311);

        int weight_8426959To831744 = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:831744})<-[r:ConnectsTo]-(s{bodyId:8426959}) RETURN r.weight").single().get(0).asInt();

        Assert.assertEquals(1, weight_8426959To831744);

        int weight_26311To8426959 = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:26311})-[r:ConnectsTo]->(s{bodyId:8426959}) RETURN r.weight").single().get(0).asInt();

        Assert.assertEquals(2, weight_26311To8426959);

        int weight_8426959To8426959 = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})-[r:ConnectsTo]->(n) RETURN r.weight").single().get(0).asInt();

        Assert.assertEquals(1, weight_8426959To8426959);

    }

    @Test
    public void connectionSetShouldContainAllSynapsesForConnection() {

        Session session = driver.session();

        List<Record> synapseCS_8426959_2589725 = session.run("MATCH (t:ConnectionSet:test:`test-ConnectionSet`{datasetBodyIds:\"test:8426959:2589725\"})-[:Contains]->(s) RETURN s").list();
        Assert.assertEquals(2, synapseCS_8426959_2589725.size());
        Node node1 = (Node) synapseCS_8426959_2589725.get(0).asMap().get("s");
        Node node2 = (Node) synapseCS_8426959_2589725.get(1).asMap().get("s");

        Point synapseLocation1 = (Point) node1.asMap().get("location");
        Point synapseLocation2 = (Point) node2.asMap().get("location");

        Point location1 = Values.point(9157, 4287, 2277, 1542).asPoint();
        Point location2 = Values.point(9157, 4298, 2294, 1542).asPoint();

        Assert.assertTrue(synapseLocation1.equals(location1) || synapseLocation2.equals(location1));
        Assert.assertTrue(synapseLocation1.equals(location2) || synapseLocation2.equals(location2));

        int connectionSetPreCount = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:8426959})<-[:From]-(c:ConnectionSet) RETURN count(c)").single().get(0).asInt();

        Assert.assertEquals(4, connectionSetPreCount);

        int connectionSetPostCount = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:8426959})<-[:To]-(c:ConnectionSet) RETURN count(c)").single().get(0).asInt();

        Assert.assertEquals(2, connectionSetPostCount);

        // weight should be equal to the number of psds per connection (assuming no many pre to one post connections)
        List<Record> connections = session.run("MATCH (n:`test-Neuron`)-[c:ConnectsTo]->(m), (cs:ConnectionSet)-[:Contains]->(s:PostSyn) WHERE cs.datasetBodyIds=\"test:\" + n.bodyId + \":\" + m.bodyId RETURN n.bodyId, m.bodyId, c.weight, cs.datasetBodyIds, count(s)").list();
        for (Record record : connections) {
            Assert.assertEquals(record.asMap().get("c.weight"), record.asMap().get("count(s)"));
        }

//        // pre weight should be equal to the number of pre per connection
//        List<Record> connectionsPre = session.run("MATCH (n:`test-Neuron`)-[c:ConnectsTo]->(m), (cs:ConnectionSet)-[:Contains]->(s:PreSyn) WHERE cs.datasetBodyIds=\"test:\" + n.bodyId + \":\" + m.bodyId RETURN n.bodyId, m.bodyId, c.pre, cs.datasetBodyIds, count(s)").list();
//        for (Record record : connectionsPre) {
//            Assert.assertEquals(record.asMap().get("c.pre"), record.asMap().get("count(s)"));
//        }

    }

}
