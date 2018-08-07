package org.janelia.flyem.neuprinter;

import apoc.create.Create;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jImporterTest {


    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Create.class);

    @Test
    public void testSkeletonLoad() {

        File swcFile1 = new File("src/test/resources/101.swc");
        File swcFile2 = new File("src/test/resources/102.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1,swcFile2};

        List<Skeleton> skeletonList = ConnConvert.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");
            neo4jImporter.addSkeletonNodes("test", skeletonList);

            Long skeleton101ContainedByBodyId = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();
            Long skeleton102ContainedByBodyId = session.run("MATCH (n:Skeleton{skeletonId:\"test:102\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();

            Assert.assertEquals(new Long(101), skeleton101ContainedByBodyId);
            Assert.assertEquals(new Long(102), skeleton102ContainedByBodyId);

            Integer skeleton101Degree = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();
            Integer skeleton102Degree = session.run("MATCH (n:Skeleton{skeletonId:\"test:102\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();

            Assert.assertEquals(new Integer(50), skeleton101Degree);
            Assert.assertEquals(new Integer(29), skeleton102Degree);

            Integer skelNode101NumberOfRoots = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ").single().get(0).asInt();
            Integer skelNode102RootDegree = session.run("MATCH (n:Skeleton{skeletonId:\"test:102\"})-[:Contains]->(s:SkelNode{rowNumber:1}) WITH s, size((s)-[:LinksTo]->()) as degree RETURN degree ").single().get(0).asInt();

            Assert.assertEquals(new Integer(4), skelNode101NumberOfRoots);
            Assert.assertEquals(new Integer(1), skelNode102RootDegree);


            Map<String, Object> skelNodeProperties = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode{rowNumber:13}) RETURN s.location, s.radius, s.skelNodeId, s.x, s.y, s.z").list().get(0).asMap();
            Assert.assertEquals("5096:9281:1624", skelNodeProperties.get("s.location"));
            Assert.assertEquals(28D, skelNodeProperties.get("s.radius"));
            Assert.assertEquals("test:101:5096:9281:1624", skelNodeProperties.get("s.skelNodeId"));
            Assert.assertEquals(5096L, skelNodeProperties.get("s.x"));
            Assert.assertEquals(9281L, skelNodeProperties.get("s.y"));
            Assert.assertEquals(1624L, skelNodeProperties.get("s.z"));

            int noStatusCount = session.run("MATCH (n:Neuron) WHERE n.status=null RETURN count(n)").single().get(0).asInt();
            Assert.assertEquals(0, noStatusCount);


        }
    }

    @Test
    public void testAddNeurons() {

        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";

        List<Neuron> neuronList = ConnConvert.readNeuronsJson(neuronsJsonPath);


        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");

            neo4jImporter.addNeurons("test", neuronList);

            int numberOfNeurons = session.run("MATCH (n:Neuron:test) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(8, numberOfNeurons);

            // test uniqueness constraint by trying to add again
            neo4jImporter.addNeurons("test", neuronList);

            int numberOfNeurons2 = session.run("MATCH (n:Neuron:test) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(8, numberOfNeurons2);

            Node bodyId100569 = session.run("MATCH (n:Neuron{bodyId:100569}) RETURN n").single().get(0).asNode();


            Assert.assertEquals("final", bodyId100569.asMap().get("status"));
            Assert.assertEquals(1031L, bodyId100569.asMap().get("size"));
            Assert.assertEquals("KC-5", bodyId100569.asMap().get("name"));
            Assert.assertEquals("KC", bodyId100569.asMap().get("type"));

            Assert.assertEquals(Values.point(9157,1.0,2.0,3.0).asPoint(), bodyId100569.asMap().get("somaLocation"));
            Assert.assertEquals(5.0, bodyId100569.asMap().get("somaRadius"));

            Assert.assertTrue(bodyId100569.hasLabel("roi1"));
            Assert.assertTrue(bodyId100569.hasLabel("roi2"));

            int labelCount = 0;
            Iterable<String> bodyLabels = bodyId100569.labels();


            for (String roi : bodyLabels) {
                labelCount++;
            }
            Assert.assertEquals(4, labelCount);


        }


    }


    @Test
    public void testAddConnectsTo() {

        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";
        String bodiesJsonPath = "src/test/resources/smallBodyListWithExtraRois.json";


        List<Neuron> neuronList = ConnConvert.readNeuronsJson(neuronsJsonPath);

        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);

        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();

        bodyList.sort(new SortBodyByNumberOfSynapses());


        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");

            neo4jImporter.addNeurons("test", neuronList);

            neo4jImporter.addConnectsTo("test", bodyList, 0);

            Node bodyId8426959 = session.run("MATCH (n:Neuron{bodyId:2589725})<-[r:ConnectsTo]-(s) RETURN s").single().get(0).asNode();

            Assert.assertEquals(8426959L, bodyId8426959.asMap().get("bodyId"));

            Assert.assertEquals(1L, bodyId8426959.asMap().get("post"));
            Assert.assertEquals(2L, bodyId8426959.asMap().get("pre"));
            Assert.assertNotNull(bodyId8426959.asMap().get("sId"));
            Assert.assertTrue(bodyId8426959.hasLabel("Big"));

            int weight = session.run("MATCH (n:Neuron{bodyId:2589725})<-[r:ConnectsTo]-(s) RETURN r.weight").single().get(0).asInt();

            Assert.assertEquals(2, weight);

            int neuronCount = session.run("MATCH (n:Neuron) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(10, neuronCount);


        }
    }

    @Test
    public void testSynapsesNeuronPartsSynapseSetsMetaAutoName() {

        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";
        String bodiesJsonPath = "src/test/resources/smallBodyListWithExtraRois.json";


        List<Neuron> neuronList = ConnConvert.readNeuronsJson(neuronsJsonPath);

        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);

        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();

        bodyList.sort(new SortBodyByNumberOfSynapses());


        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.prepDatabase("test");

            neo4jImporter.addNeurons("test", neuronList);

            neo4jImporter.addConnectsTo("test", bodyList, 0);

            neo4jImporter.addSynapsesWithRois("test", bodyList);

            neo4jImporter.addSynapsesTo("test", preToPost);

            neo4jImporter.addNeuronRois("test", bodyList);

            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();

            }

            neo4jImporter.addNeuronParts("test", bodyList);

            neo4jImporter.addSynapseSets("test", bodyList);

            neo4jImporter.createMetaNode("test");

            try {
                neo4jImporter.addAutoNames("test");
            } catch (Exception e) {
                System.out.println("auto names failed");
            }

            Node preSynNode = session.run("MATCH (s:Synapse:PreSyn{datasetLocation:\"test:4287:2277:1502\"}) RETURN s").single().get(0).asNode();

            Assert.assertEquals(1.0, preSynNode.asMap().get("confidence"));
            Assert.assertEquals("pre", preSynNode.asMap().get("type"));
            Assert.assertEquals(4287L, preSynNode.asMap().get("x"));
            Assert.assertEquals(2277L, preSynNode.asMap().get("y"));
            Assert.assertEquals(1502L, preSynNode.asMap().get("z"));
            Assert.assertTrue(preSynNode.hasLabel("seven_column_roi"));
            Assert.assertTrue(preSynNode.hasLabel("roiA"));


            Node postSynNode = session.run("MATCH (s:Synapse:PostSyn{datasetLocation:\"test:4301:2276:1535\"}) RETURN s").single().get(0).asNode();

            Assert.assertEquals(1.0, postSynNode.asMap().get("confidence"));
            Assert.assertEquals("post", postSynNode.asMap().get("type"));
            Assert.assertEquals(4301L, postSynNode.asMap().get("x"));
            Assert.assertEquals(2276L, postSynNode.asMap().get("y"));
            Assert.assertEquals(1535L, postSynNode.asMap().get("z"));
            Assert.assertTrue(postSynNode.hasLabel("seven_column_roi"));
            Assert.assertTrue(postSynNode.hasLabel("roiA"));


            int synapsesToCount = session.run("MATCH (s:Synapse:PreSyn{datasetLocation:\"test:4287:2277:1502\"})-[:SynapsesTo]->(l) RETURN count(l)").single().get(0).asInt();

            Assert.assertEquals(3, synapsesToCount);

            Node neuronNode = session.run("MATCH (n{bodyId:8426959}) RETURN n").single().get(0).asNode();

            Assert.assertTrue(neuronNode.hasLabel("seven_column_roi"));
            Assert.assertTrue(neuronNode.hasLabel("roiA"));
            Assert.assertTrue(neuronNode.hasLabel("roiB"));

            Node neuronPart = session.run("MATCH (p:NeuronPart {neuronPartId:\"test:8426959:seven_column_roi\"}) RETURN p").single().get(0).asNode();

            Assert.assertEquals(2L, neuronPart.asMap().get("pre"));
            Assert.assertEquals(0L, neuronPart.asMap().get("post"));
            Assert.assertEquals(2L, neuronPart.asMap().get("size"));

            Node neuronPartNeuron = session.run("MATCH (p:NeuronPart {neuronPartId:\"test:8426959:seven_column_roi\"})-[:PartOf]->(n) RETURN n").single().get(0).asNode();

            Assert.assertEquals(8426959L, neuronPartNeuron.asMap().get("bodyId"));

            int synapseSetContainsCount = session.run("MATCH (t:SynapseSet{datasetBodyId:\"test:8426959\"})-[:Contains]->(s) RETURN count(s)").single().get(0).asInt();

            Assert.assertEquals(3, synapseSetContainsCount);

            int synapseSetContainedCount = session.run("MATCH (t:SynapseSet{datasetBodyId:\"test:8426959\"})<-[:Contains]-(n) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(1L, synapseSetContainedCount);


            int noDatasetLabelCount = session.run("MATCH (n) WHERE NOT n:test RETURN count(n)").single().get(0).asInt();
            int noTimeStampCount = session.run("MATCH (n) WHERE NOT exists(n.timeStamp) RETURN count(n)").single().get(0).asInt();
            int noStatusCount = session.run("MATCH (n:Neuron) WHERE NOT exists(n.status) RETURN count(n)").single().get(0).asInt();

            Assert.assertEquals(0, noDatasetLabelCount);
            Assert.assertEquals(1, noTimeStampCount);
            Assert.assertEquals(0, noStatusCount);

            Node metaNode = session.run("MATCH (n:Meta:test) RETURN n").single().get(0).asNode();
            Assert.assertEquals(2L, metaNode.asMap().get("totalPreCount"));
            Assert.assertEquals(4L, metaNode.asMap().get("totalPostCount"));

            Assert.assertEquals(3L, metaNode.asMap().get("roiAPostCount"));
            Assert.assertEquals(2L, metaNode.asMap().get("roiAPreCount"));
            Assert.assertEquals(0L, metaNode.asMap().get("roiBPreCount"));
            Assert.assertEquals(3L, metaNode.asMap().get("roiBPostCount"));
            // test to handle ' characters predictably
            Assert.assertEquals(0L, metaNode.asMap().get("roi_CPreCount"));
            Assert.assertEquals(1L, metaNode.asMap().get("roi_CPostCount"));
            // test that all rois are listed in meta
            List<String> rois = (List<String>) metaNode.asMap().get("rois");
            Assert.assertEquals(3, rois.size());
            Assert.assertEquals("roi'C", rois.get(0));


            String neuronName = session.run("MATCH (n:Neuron:test{bodyId:8426959}) RETURN n.name").single().get(0).asString();
            String neuronAutoName = session.run("MATCH (n:Neuron:test{bodyId:8426959}) RETURN n.autoName").single().get(0).asString();

            Assert.assertEquals("ROIA-ROIA-0", neuronName);
            Assert.assertEquals(neuronName, neuronAutoName);

            String neuronName2 = session.run("MATCH (n:Neuron:test{bodyId:26311}) RETURN n.name").single().get(0).asString();
            String neuronAutoName2 = session.run("MATCH (n:Neuron:test{bodyId:26311}) RETURN n.autoName").single().get(0).asString();

            Assert.assertEquals("Dm12-4", neuronName2);
            Assert.assertEquals("ROIA-ROIA-1", neuronAutoName2);


        }


    }
}
