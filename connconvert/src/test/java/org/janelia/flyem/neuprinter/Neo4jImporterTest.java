package org.janelia.flyem.neuprinter;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
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
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Point;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Tests the {@link Neo4jImporter} class.
 */
public class Neo4jImporterTest {

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
        File swcFile1 = new File("src/test/resources/101.swc");
        File swcFile2 = new File("src/test/resources/102.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);

        String bodiesJsonPath = "src/test/resources/smallBodyListWithExtraRois.json";

        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);
        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());

        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

        neo4jImporter.prepDatabase("test");
        neo4jImporter.addSegments("test", neuronList);
        neo4jImporter.addConnectsTo("test", bodyList);
        neo4jImporter.addSynapsesWithRois("test", bodyList);
        neo4jImporter.addSynapsesTo("test", preToPost);
        neo4jImporter.addSegmentRois("test", bodyList);
        neo4jImporter.addSynapseSets("test", bodyList);
        neo4jImporter.addSkeletonNodes("test", skeletonList);
        neo4jImporter.createMetaNodeWithDataModelNode("test", 1.0F);
        neo4jImporter.addAutoNamesAndNeuronLabels("test", 1);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void skeletonsShouldBeConnectedToAppropriateBody() {

        Session session = driver.session();

        Long skeleton101ContainedByBodyId = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();
        Long skeleton102ContainedByBodyId = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:102\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();

        Assert.assertEquals(new Long(101), skeleton101ContainedByBodyId);
        Assert.assertEquals(new Long(102), skeleton102ContainedByBodyId);

    }

    @Test
    public void skeletonNodeShouldContainAllSkelNodesForSkeleton() {

        Session session = driver.session();

        Integer skeleton101Degree = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();
        Integer skeleton102Degree = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:102\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();

        Assert.assertEquals(new Integer(50), skeleton101Degree);
        Assert.assertEquals(new Integer(29), skeleton102Degree);
    }

    @Test
    public void skeletonShouldHaveAppropriateNumberOfRootSkelNodes() {

        Session session = driver.session();

        Integer skelNode101NumberOfRoots = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode:`test-SkelNode`) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ").single().get(0).asInt();
        Integer skelNode102RootDegree = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:102\"})-[:Contains]->(s:SkelNode:`test-SkelNode`{rowNumber:1}) WITH s, size((s)-[:LinksTo]->()) as degree RETURN degree ").single().get(0).asInt();

        Assert.assertEquals(new Integer(4), skelNode101NumberOfRoots);
        Assert.assertEquals(new Integer(1), skelNode102RootDegree);
    }

    @Test
    public void skelNodesShouldContainAllPropertiesFromSWC() {

        Session session = driver.session();

        Map<String, Object> skelNodeProperties = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode:`test-SkelNode`{rowNumber:13}) RETURN s.location, s.radius, s.skelNodeId, s.type").list().get(0).asMap();
        Assert.assertEquals(Values.point(9157, 5096, 9281, 1624).asPoint(), skelNodeProperties.get("s.location"));
        Assert.assertEquals(28D, skelNodeProperties.get("s.radius"));
        Assert.assertEquals(0L, skelNodeProperties.get("s.type"));
        Assert.assertEquals("test:101:5096:9281:1624", skelNodeProperties.get("s.skelNodeId"));

    }

    @Test
    public void allBodiesShouldBeLabeledAsSegment() {

        Session session = driver.session();

        int numberOfSegments = session.run("MATCH (n:Segment:test:`test-Segment`) RETURN count(n)").single().get(0).asInt();

        Assert.assertEquals(12, numberOfSegments);

    }

    @Test
    public void shouldNotBeAbleToAddDuplicateSegmentsDueToUniquenessConstraint() {

        Session session = driver.session();

        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

        // test uniqueness constraint by trying to add again
        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";
        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);
        neo4jImporter.addSegments("test", neuronList);

        int numberOfSegments2 = session.run("MATCH (n:Segment:test:`test-Segment`) RETURN count(n)").single().get(0).asInt();

        Assert.assertEquals(12, numberOfSegments2);

    }

    @Test
    public void segmentPropertiesShouldMatchInputJson() {

        Session session = driver.session();

        Node bodyId100569 = session.run("MATCH (n:Segment:`test-Segment`:test{bodyId:100569}) RETURN n").single().get(0).asNode();

        Assert.assertEquals("final", bodyId100569.asMap().get("status"));
        Assert.assertEquals(1031L, bodyId100569.asMap().get("size"));
        Assert.assertEquals("KC-5", bodyId100569.asMap().get("name"));
        Assert.assertEquals("KC", bodyId100569.asMap().get("type"));

        Assert.assertEquals(Values.point(9157, 1.0, 2.0, 3.0).asPoint(), bodyId100569.asMap().get("somaLocation"));
        Assert.assertEquals(5.0, bodyId100569.asMap().get("somaRadius"));

        Assert.assertTrue(bodyId100569.asMap().containsKey("roi1"));
        Assert.assertEquals(true, bodyId100569.asMap().get("roi1"));
        Assert.assertTrue(bodyId100569.asMap().containsKey("roi1"));
        Assert.assertTrue(bodyId100569.asMap().containsKey("roi2"));

        int labelCount = 0;
        Iterable<String> bodyLabels = bodyId100569.labels();

        for (String ignored : bodyLabels) labelCount++;
        Assert.assertEquals(3, labelCount);
    }

    @Test
    public void allSegmentsShouldHaveAStatus() {

        Session session = driver.session();

        int noStatusCount = session.run("MATCH (n:Segment) WHERE n.status=null RETURN count(n)").single().get(0).asInt();
        Assert.assertEquals(0, noStatusCount);

    }

    @Test
    public void allSegmentsWithConnectionsShouldHaveRoiInfoProperty() {

        Session session = driver.session();

        int roiInfoCount = session.run("MATCH (n:Segment:test:`test-Segment`) WHERE exists(n.roiInfo) RETURN count(n)").single().get(0).asInt();

        Assert.assertEquals(4, roiInfoCount);
    }

    @Test
    public void segmentsShouldHaveCorrectPreAndPostCountsAndRoiInfo() {

        Gson gson = new Gson();

        Session session = driver.session();

        Node bodyId8426959 = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:2589725})<-[r:ConnectsTo]-(s) RETURN s").single().get(0).asNode();

        Assert.assertEquals(8426959L, bodyId8426959.asMap().get("bodyId"));

        Assert.assertEquals(1L, bodyId8426959.asMap().get("post"));
        Assert.assertEquals(2L, bodyId8426959.asMap().get("pre"));
        Map<String, SynapseCounter> synapseCountPerRoi = gson.fromJson((String) bodyId8426959.asMap().get("roiInfo"), new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
        Assert.assertEquals(3, synapseCountPerRoi.keySet().size());
        Assert.assertEquals(2, synapseCountPerRoi.get("roiA").getPre());
        Assert.assertEquals(0, synapseCountPerRoi.get("seven_column_roi").getPost());
        Assert.assertEquals(1, synapseCountPerRoi.get("roiB").getPost());

    }

    @Test
    public void segmentsShouldHaveRoisAsBooleanProperties() {

        Session session = driver.session();

        Node segmentNode = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959}) RETURN n").single().get(0).asNode();

        Assert.assertTrue(segmentNode.asMap().containsKey("seven_column_roi"));
        Assert.assertTrue(segmentNode.asMap().containsKey("roiA"));
        Assert.assertTrue(segmentNode.asMap().containsKey("roiB"));

    }

    @Test
    public void shouldHaveCorrectConnectsToWeights() {

        Session session = driver.session();

        int weight = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:2589725})<-[r:ConnectsTo]-(s) RETURN r.weight").single().get(0).asInt();

        Assert.assertEquals(2, weight);

    }

    @Test
    public void synapsesShouldHavePropertiesMatchingInputJson() {

        Session session = driver.session();

        Point preLocationPoint = Values.point(9157, 4287, 2277, 1502).asPoint();
        Node preSynNode = session.run("MATCH (s:Synapse:PreSyn:`test-Synapse`:`test-PreSyn`:test{location:$location}) RETURN s",
                parameters("location", preLocationPoint)).single().get(0).asNode();

        Assert.assertEquals(1.0, preSynNode.asMap().get("confidence"));
        Assert.assertEquals("pre", preSynNode.asMap().get("type"));
        Assert.assertTrue(preSynNode.asMap().containsKey("seven_column_roi"));
        Assert.assertTrue(preSynNode.asMap().containsKey("roiA"));
        Assert.assertEquals(true, preSynNode.asMap().get("roiA"));

        Point postLocationPoint = Values.point(9157, 4301, 2276, 1535).asPoint();
        Node postSynNode = session.run("MATCH (s:Synapse:`test-Synapse`:`test-PostSyn`:test:PostSyn{location:$location}) RETURN s",
                parameters("location", postLocationPoint)).single().get(0).asNode();

        Assert.assertEquals(1.0, postSynNode.asMap().get("confidence"));
        Assert.assertEquals("post", postSynNode.asMap().get("type"));
        Assert.assertTrue(postSynNode.asMap().containsKey("seven_column_roi"));
        Assert.assertTrue(postSynNode.asMap().containsKey("roiA"));
        Assert.assertEquals(true, postSynNode.asMap().get("seven_column_roi"));

    }

    @Test
    public void synapsesShouldSynapseToCorrectSynapses() {

        Session session = driver.session();

        Point preLocationPoint2 = Values.point(9157, 4287, 2277, 1502).asPoint();
        int synapsesToCount = session.run("MATCH (s:Synapse:PreSyn:`test-Synapse`:`test-PreSyn`:test{location:$location})-[:SynapsesTo]->(l) RETURN count(l)",
                parameters("location", preLocationPoint2)).single().get(0).asInt();

        Assert.assertEquals(3, synapsesToCount);

    }

    @Test
    public void synapseSetShouldContainAllSynapsesForNeuron() {

        Session session = driver.session();

        int synapseSetContainsCount = session.run("MATCH (t:SynapseSet:test:`test-SynapseSet`{datasetBodyId:\"test:8426959\"})-[:Contains]->(s) RETURN count(s)").single().get(0).asInt();

        Assert.assertEquals(3, synapseSetContainsCount);

        int synapseSetContainedCount = session.run("MATCH (t:SynapseSet:test:`test-SynapseSet`{datasetBodyId:\"test:8426959\"})<-[:Contains]-(n) RETURN count(n)").single().get(0).asInt();

        Assert.assertEquals(1L, synapseSetContainedCount);

    }

    @Test
    public void metaNodeShouldHaveCorrectSynapseCounts() {

        Session session = driver.session();

        Node metaNode = session.run("MATCH (n:Meta:test) RETURN n").single().get(0).asNode();
        Assert.assertEquals(2L, metaNode.asMap().get("totalPreCount"));
        Assert.assertEquals(5L, metaNode.asMap().get("totalPostCount"));

        String metaSynapseCountPerRoi = (String) metaNode.asMap().get("roiInfo");
        Gson gson = new Gson();
        Map<String, SynapseCounter> metaSynapseCountPerRoiMap = gson.fromJson(metaSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        Assert.assertEquals(4L, metaSynapseCountPerRoiMap.get("roiA").getPost());
        Assert.assertEquals(2L, metaSynapseCountPerRoiMap.get("roiA").getPre());
        Assert.assertEquals(0L, metaSynapseCountPerRoiMap.get("roiB").getPre());
        Assert.assertEquals(3L, metaSynapseCountPerRoiMap.get("roiB").getPost());

        // test to handle ' characters predictably
        Assert.assertEquals(0L, metaSynapseCountPerRoiMap.get("roi'C").getPre());
        Assert.assertEquals(1L, metaSynapseCountPerRoiMap.get("roi'C").getPost());
        // test that all rois are listed in meta
        Assert.assertEquals(6, metaSynapseCountPerRoiMap.keySet().size());

    }

    @Test
    public void dataModelShouldHaveCorrectVersionAndBeConnectedToMetaNode() {

        Session session = driver.session();

        Float dataModelVersion = session.run("MATCH (n:Meta)-[:Is]->(d:DataModel) RETURN d.dataModelVersion").single().get(0).asFloat();

        Assert.assertEquals(new Float(1.0F), dataModelVersion);

    }

    @Test
    public void neuronsShouldHaveAutoNamesAndNamesShouldReplaceAutoNamesWhenNull() {

        Session session = driver.session();

        String segmentName = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:8426959}) RETURN n.name").single().get(0).asString();
        String segmentAutoName = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:8426959}) RETURN n.autoName").single().get(0).asString();

        Assert.assertEquals("ROIA-ROIA_0*", segmentName);
        Assert.assertEquals(segmentName.replace("*", ""), segmentAutoName);

        String segmentName2 = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:26311}) RETURN n.name").single().get(0).asString();
        String segmentAutoName2 = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:26311}) RETURN n.autoName").single().get(0).asString();

        Assert.assertEquals("Dm12-4", segmentName2);
        Assert.assertEquals("ROIA-ROIA_1", segmentAutoName2);

    }

    @Test
    public void allNeuronsShouldHaveAutoNamesAndAllSegmentsWithAutoNamesShouldBeLabeledNeuron() {
        Session session = driver.session();

        int neuronWithoutAutoNameCount = session.run("MATCH (n:Neuron:`test-Neuron`:test) WHERE NOT exists(n.autoName) RETURN count(n)").single().get(0).asInt();
        Assert.assertEquals(0, neuronWithoutAutoNameCount);
        int autoNamesWithoutNeuronCount = session.run("MATCH (n:Segment:`test-Segment`:test) WHERE exists(n.autoName) AND NOT n:Neuron RETURN count(n)").single().get(0).asInt();
        Assert.assertEquals(0, autoNamesWithoutNeuronCount);

    }

    @Test
    public void allNodesShouldHaveDatasetLabelAndTimeStamp() {

        Session session = driver.session();

        int nodeWithoutDatasetLabelCount = session.run("MATCH (n) WHERE NOT n:test RETURN count(n)").single().get(0).asInt();
        // DataModel node does not have dataset label
        Assert.assertEquals(1, nodeWithoutDatasetLabelCount);
        int nodeWithoutTimeStamp = session.run("MATCH (n) WHERE NOT exists(n.timeStamp) RETURN count(n)").single().get(0).asInt();
        // Meta node does not have timeStamp
        Assert.assertEquals(1, nodeWithoutTimeStamp);

    }

    @Test
    public void testThatAutoNamesAreAddedToAboveThresholdNeurons() {

        Session session = driver.session();

        int belowThresholdAutoNameCount = session.run("MATCH (n:Segment) WHERE n.pre+n.post>1 AND NOT exists(n.autoName) RETURN count(n)").single().get(0).asInt();

        Assert.assertEquals(0, belowThresholdAutoNameCount);

    }

    @Test
    public void testThatNeuronLabelsAreAddedToAboveThresholdNeurons() {

        Session session = driver.session();

        int belowThresholdNeuronCount = session.run("MATCH (n:Segment) WHERE n.pre+n.post>1 AND NOT n:Neuron RETURN count(n)").single().get(0).asInt();

        Assert.assertEquals(0, belowThresholdNeuronCount);

    }
}
