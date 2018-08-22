package org.janelia.flyem.neuprinter;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.junit.Assert;
import org.junit.Rule;
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

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporterTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void testSkeletonLoad() {

        File swcFile1 = new File("src/test/resources/101.swc");
        File swcFile2 = new File("src/test/resources/102.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");
            neo4jImporter.addSkeletonNodes("test", skeletonList);

            Long skeleton101ContainedByBodyId = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();
            Long skeleton102ContainedByBodyId = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:102\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();

            Assert.assertEquals(new Long(101), skeleton101ContainedByBodyId);
            Assert.assertEquals(new Long(102), skeleton102ContainedByBodyId);

            Integer skeleton101Degree = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();
            Integer skeleton102Degree = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:102\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();

            Assert.assertEquals(new Integer(50), skeleton101Degree);
            Assert.assertEquals(new Integer(29), skeleton102Degree);

            Integer skelNode101NumberOfRoots = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode:`test-SkelNode`) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ").single().get(0).asInt();
            Integer skelNode102RootDegree = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:102\"})-[:Contains]->(s:SkelNode:`test-SkelNode`{rowNumber:1}) WITH s, size((s)-[:LinksTo]->()) as degree RETURN degree ").single().get(0).asInt();

            Assert.assertEquals(new Integer(4), skelNode101NumberOfRoots);
            Assert.assertEquals(new Integer(1), skelNode102RootDegree);

            Map<String, Object> skelNodeProperties = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode:`test-SkelNode`{rowNumber:13}) RETURN s.location, s.radius, s.skelNodeId, s.type").list().get(0).asMap();
            Assert.assertEquals(Values.point(9157, 5096, 9281, 1624).asPoint(), skelNodeProperties.get("s.location"));
            Assert.assertEquals(28D, skelNodeProperties.get("s.radius"));
            Assert.assertEquals(0L, skelNodeProperties.get("s.type"));
            Assert.assertEquals("test:101:5096:9281:1624", skelNodeProperties.get("s.skelNodeId"));

            int noStatusCount = session.run("MATCH (n:Neuron) WHERE n.status=null RETURN count(n)").single().get(0).asInt();
            Assert.assertEquals(0, noStatusCount);

            int noDatasetLabel = session.run("MATCH (n) WHERE NOT n:test RETURN count(n)").single().get(0).asInt();
            Assert.assertEquals(0, noDatasetLabel);

        }
    }

    @Test
    public void testAddNeurons() {

        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");

            neo4jImporter.addNeurons("test", neuronList);

            int numberOfNeurons = session.run("MATCH (n:Neuron:test:`test-Neuron`) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(8, numberOfNeurons);

            // test uniqueness constraint by trying to add again
            neo4jImporter.addNeurons("test", neuronList);

            int numberOfNeurons2 = session.run("MATCH (n:Neuron:test:`test-Neuron`) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(8, numberOfNeurons2);

            Node bodyId100569 = session.run("MATCH (n:Neuron:`test-Neuron`:test{bodyId:100569}) RETURN n").single().get(0).asNode();

            Assert.assertEquals("final", bodyId100569.asMap().get("status"));
            Assert.assertEquals(1031L, bodyId100569.asMap().get("size"));
            Assert.assertEquals("KC-5", bodyId100569.asMap().get("name"));
            Assert.assertEquals("KC", bodyId100569.asMap().get("type"));

            Assert.assertEquals(Values.point(9157, 1.0, 2.0, 3.0).asPoint(), bodyId100569.asMap().get("somaLocation"));
            Assert.assertEquals(5.0, bodyId100569.asMap().get("somaRadius"));

            Assert.assertTrue(bodyId100569.hasLabel("test-roi1"));
            Assert.assertTrue(bodyId100569.hasLabel("test-roi2"));
            Assert.assertTrue(bodyId100569.hasLabel("roi1"));
            Assert.assertTrue(bodyId100569.hasLabel("roi2"));

            int labelCount = 0;
            Iterable<String> bodyLabels = bodyId100569.labels();

            for (String roi : bodyLabels) {
                labelCount++;
            }
            Assert.assertEquals(7, labelCount);

            int noStatusCount = session.run("MATCH (n:Neuron) WHERE n.status=null RETURN count(n)").single().get(0).asInt();
            Assert.assertEquals(0, noStatusCount);

            int noDatasetLabel = session.run("MATCH (n) WHERE NOT n:test RETURN count(n)").single().get(0).asInt();
            Assert.assertEquals(0, noDatasetLabel);

        }

    }

    @Test
    public void testAddConnectsTo() {

        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";
        String bodiesJsonPath = "src/test/resources/smallBodyListWithExtraRois.json";
        Gson gson = new Gson();

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);

        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);

        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();

        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");

            neo4jImporter.addNeurons("test", neuronList);

            neo4jImporter.addConnectsTo("test", bodyList);

            Node bodyId8426959 = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:2589725})<-[r:ConnectsTo]-(s) RETURN s").single().get(0).asNode();

            Assert.assertEquals(8426959L, bodyId8426959.asMap().get("bodyId"));

            Assert.assertEquals(1L, bodyId8426959.asMap().get("post"));
            Assert.assertEquals(2L, bodyId8426959.asMap().get("pre"));
            Map<String, SynapseCounter> synapseCountPerRoi = gson.fromJson((String) bodyId8426959.asMap().get("synapseCountPerRoi"), new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());
            Assert.assertEquals(3, synapseCountPerRoi.keySet().size());
            Assert.assertEquals(2, synapseCountPerRoi.get("roiA").getPre());
            Assert.assertEquals(0, synapseCountPerRoi.get("seven_column_roi").getPost());
            Assert.assertEquals(1, synapseCountPerRoi.get("roiB").getPost());

            int weight = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:2589725})<-[r:ConnectsTo]-(s) RETURN r.weight").single().get(0).asInt();

            Assert.assertEquals(2, weight);

            int neuronCount = session.run("MATCH (n:Neuron:test:`test-Neuron`) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(10, neuronCount);

            // all neurons with synapses have a synapseCountPerRoi property
            int synapseCountPerRoiCount = session.run("MATCH (n:Neuron:test:`test-Neuron`) WHERE exists(n.synapseCountPerRoi) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(4, synapseCountPerRoiCount);

            int noStatusCount = session.run("MATCH (n:Neuron) WHERE n.status=null RETURN count(n)").single().get(0).asInt();
            Assert.assertEquals(0, noStatusCount);

            int noDatasetLabel = session.run("MATCH (n) WHERE NOT n:test RETURN count(n)").single().get(0).asInt();
            Assert.assertEquals(0, noDatasetLabel);

        }
    }

    @Test
    public void testSynapsesSynapseSetsMetaAutoName() {

        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";
        String bodiesJsonPath = "src/test/resources/smallBodyListWithExtraRois.json";

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);

        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);

        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();

        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");

            neo4jImporter.addNeurons("test", neuronList);

            neo4jImporter.addConnectsTo("test", bodyList);

            neo4jImporter.addSynapsesWithRois("test", bodyList);

            neo4jImporter.addSynapsesTo("test", preToPost);

            neo4jImporter.addNeuronRois("test", bodyList);

            neo4jImporter.addSynapseSets("test", bodyList);

            neo4jImporter.createMetaNodeWithDataModelNode("test", 1.0F);

            neo4jImporter.addAutoNames("test", 0);

            Point preLocationPoint = Values.point(9157, 4287, 2277, 1502).asPoint();
            Node preSynNode = session.run("MATCH (s:Synapse:PreSyn:`test-Synapse`:`test-PreSyn`:test{location:$location}) RETURN s",
                    parameters("location", preLocationPoint)).single().get(0).asNode();

            Assert.assertEquals(1.0, preSynNode.asMap().get("confidence"));
            Assert.assertEquals("pre", preSynNode.asMap().get("type"));
            Assert.assertTrue(preSynNode.hasLabel("seven_column_roi-pt"));
            Assert.assertTrue(preSynNode.hasLabel("roiA-pt"));
            Assert.assertTrue(preSynNode.hasLabel("test-seven_column_roi-pt"));
            Assert.assertTrue(preSynNode.hasLabel("test-roiA-pt"));

            Point postLocationPoint = Values.point(9157, 4301, 2276, 1535).asPoint();
            Node postSynNode = session.run("MATCH (s:Synapse:`test-Synapse`:`test-PostSyn`:test:PostSyn{location:$location}) RETURN s",
                    parameters("location", postLocationPoint)).single().get(0).asNode();

            Assert.assertEquals(1.0, postSynNode.asMap().get("confidence"));
            Assert.assertEquals("post", postSynNode.asMap().get("type"));
            Assert.assertTrue(postSynNode.hasLabel("seven_column_roi-pt"));
            Assert.assertTrue(postSynNode.hasLabel("roiA-pt"));
            Assert.assertTrue(postSynNode.hasLabel("test-seven_column_roi-pt"));
            Assert.assertTrue(postSynNode.hasLabel("test-roiA-pt"));

            Point preLocationPoint2 = Values.point(9157, 4287, 2277, 1502).asPoint();
            int synapsesToCount = session.run("MATCH (s:Synapse:PreSyn:`test-Synapse`:`test-PreSyn`:test{location:$location})-[:SynapsesTo]->(l) RETURN count(l)",
                    parameters("location", preLocationPoint2)).single().get(0).asInt();

            Assert.assertEquals(3, synapsesToCount);

            Node neuronNode = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:8426959}) RETURN n").single().get(0).asNode();

            Assert.assertTrue(neuronNode.hasLabel("seven_column_roi"));
            Assert.assertTrue(neuronNode.hasLabel("roiA"));
            Assert.assertTrue(neuronNode.hasLabel("roiB"));
            Assert.assertTrue(neuronNode.hasLabel("test-seven_column_roi"));
            Assert.assertTrue(neuronNode.hasLabel("test-roiA"));
            Assert.assertTrue(neuronNode.hasLabel("test-roiB"));

            int synapseSetContainsCount = session.run("MATCH (t:SynapseSet:test:`test-SynapseSet`{datasetBodyId:\"test:8426959\"})-[:Contains]->(s) RETURN count(s)").single().get(0).asInt();

            Assert.assertEquals(3, synapseSetContainsCount);

            int synapseSetContainedCount = session.run("MATCH (t:SynapseSet:test:`test-SynapseSet`{datasetBodyId:\"test:8426959\"})<-[:Contains]-(n) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(1L, synapseSetContainedCount);

            int noDatasetLabelCount = session.run("MATCH (n) WHERE NOT n:test RETURN count(n)").single().get(0).asInt();
            int noTimeStampCount = session.run("MATCH (n) WHERE NOT exists(n.timeStamp) RETURN count(n)").single().get(0).asInt();
            int noStatusCount = session.run("MATCH (n:Neuron) WHERE NOT exists(n.status) RETURN count(n)").single().get(0).asInt();

            //Only node without dataset label should be :DataModel
            Assert.assertEquals(1, noDatasetLabelCount);
            Assert.assertEquals(1, noTimeStampCount);
            Assert.assertEquals(0, noStatusCount);

            Node metaNode = session.run("MATCH (n:Meta:test) RETURN n").single().get(0).asNode();
            Assert.assertEquals(2L, metaNode.asMap().get("totalPreCount"));
            Assert.assertEquals(4L, metaNode.asMap().get("totalPostCount"));

            String metaSynapseCountPerRoi = (String) metaNode.asMap().get("synapseCountPerRoi");
            Gson gson = new Gson();
            Map<String, SynapseCounter> metaSynapseCountPerRoiMap = gson.fromJson(metaSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());

            Assert.assertEquals(3L, metaSynapseCountPerRoiMap.get("roiA").getPost());
            Assert.assertEquals(2L, metaSynapseCountPerRoiMap.get("roiA").getPre());
            Assert.assertEquals(0L, metaSynapseCountPerRoiMap.get("roiB").getPre());
            Assert.assertEquals(3L, metaSynapseCountPerRoiMap.get("roiB").getPost());
            // test to handle ' characters predictably
            Assert.assertEquals(0L, metaSynapseCountPerRoiMap.get("roi'C").getPre());
            Assert.assertEquals(1L, metaSynapseCountPerRoiMap.get("roi'C").getPost());
            // test that all rois are listed in meta
            Assert.assertEquals(6, metaSynapseCountPerRoiMap.keySet().size());

            //test for :DataModel node
            Float dataModelVersion = session.run("MATCH (n:Meta)-[:Is]->(d:DataModel) RETURN d.dataModelVersion").single().get(0).asFloat();

            Assert.assertEquals(new Float(1.0F), dataModelVersion);

            String neuronName = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:8426959}) RETURN n.name").single().get(0).asString();
            String neuronAutoName = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:8426959}) RETURN n.autoName").single().get(0).asString();

            Assert.assertEquals("ROIA-ROIA-0*", neuronName);
            Assert.assertEquals(neuronName.replace("*",""), neuronAutoName);

            String neuronName2 = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:26311}) RETURN n.name").single().get(0).asString();
            String neuronAutoName2 = session.run("MATCH (n:Neuron:test:`test-Neuron`{bodyId:26311}) RETURN n.autoName").single().get(0).asString();

            Assert.assertEquals("Dm12-4", neuronName2);
            Assert.assertEquals("ROIA-ROIA-1", neuronAutoName2);

        }

    }
}
