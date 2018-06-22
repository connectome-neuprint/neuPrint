package org.janelia.flyem.neuprintprocedures;

import apoc.refactor.GraphRefactoring;
import apoc.create.Create;
import org.janelia.flyem.neuprinter.ConnConvert;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.neo4j.driver.v1.Values.parameters;

public class MergeNeuronsTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ProofreaderProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(Create.class);


    @Test
    public void shouldGetConnectsToForBothNodesWithSummedWeights() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n:Neuron:test{bodyId:$id1}), (m:Neuron:test{bodyId:$id2}), (o{bodyId:$id3}), (p{bodyId:$id4}) \n" +
                            "CREATE (n)-[:ConnectsTo{weight:7}]->(o) \n" +
                            "CREATE (m)-[:ConnectsTo{weight:23}]->(o) \n" +
                            "CREATE (o)-[:ConnectsTo{weight:13}]->(n) \n" +
                            "CREATE (o)-[:ConnectsTo{weight:5}]->(m) \n" +
                            "CREATE (o)-[:ConnectsTo{weight:17}]->(p) \n" +
                            "CREATE (n)-[:ConnectsTo{weight:2}]->(n) \n" +
                            "CREATE (m)-[:ConnectsTo{weight:3}]->(m) \n" +
                            "CREATE (m)-[:ConnectsTo{weight:37}]->(n) \n " +
                            "CREATE (n)-[:ConnectsTo{weight:5}]->(m)",
                    parameters("id1", 1, "id2", 2, "id3", 3, "id4", 4));


            session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset) YIELD node RETURN node", parameters("bodyId1", 1, "bodyId2", 2, "dataset", "test")).single().get(0).asNode();

            Long newTo1Weight = session.run("MATCH (n)-[r:ConnectsTo]->(m{bodyId:1}) WHERE id(n)=20 RETURN r.weight").single().get(0).asLong();

            Assert.assertEquals(new Long(47), newTo1Weight);

            Long oneToNewWeight = session.run("MATCH (m{bodyId:1})-[r:ConnectsTo]->(n) WHERE id(n)=20 RETURN r.weight").single().get(0).asLong();

            Assert.assertEquals(new Long(47), oneToNewWeight);

            Long newTo3Weight = session.run("MATCH (n)-[r:ConnectsTo]->(m{bodyId:3}) WHERE id(n)=20 RETURN r.weight").single().get(0).asLong();

            Assert.assertEquals(new Long(30), newTo3Weight);

            Long threeToNewWeight = session.run("MATCH (m{bodyId:3})-[r:ConnectsTo]->(n) WHERE id(n)=20 RETURN r.weight").single().get(0).asLong();

            Assert.assertEquals(new Long(18), threeToNewWeight);

            Long threeTo4Weight = session.run("MATCH (n{bodyId:3})-[r:ConnectsTo]->(p{bodyId:4}) RETURN r.weight").single().get(0).asLong();

            Assert.assertEquals(new Long(17), threeTo4Weight);

            int mergedBody1RelCount = session.run("MATCH (n{mergedBodyId:1})-[r]->() RETURN count(r)").single().get(0).asInt();

            Assert.assertEquals(1, mergedBody1RelCount);

            int mergedBody2RelCount = session.run("MATCH (n{mergedBodyId:2})-[r]->() RETURN count(r)").single().get(0).asInt();

            Assert.assertEquals(1, mergedBody2RelCount);

        }
    }


    @Test
    public void shouldContainMergedSynapseSet() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n:Neuron:test{bodyId:$id1}), (m:Neuron:test{bodyId:$id2}), (o:SynapseSet{datasetBodyId:$ssid1}), (p:SynapseSet{datasetBodyId:$ssid2}), (q:Synapse{location:\"1:2:3\"}), (r:Synapse{location:\"4:5:6\"}), (s:Synapse{location:\"7:8:9\"}) \n" +
                            "CREATE (n)-[:Contains]->(o) \n" +
                            "CREATE (m)-[:Contains]->(p) \n" +
                            "CREATE (o)-[:Contains]->(q) \n" +
                            "CREATE (o)-[:Contains]->(r) \n" +
                            "CREATE (p)-[:Contains]->(s) ",
                    parameters("id1", 1, "id2", 2, "ssid1", "test:1", "ssid2", "test:2"));

            session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset)", parameters("bodyId1", 1, "bodyId2", 2, "dataset", "test"));

            Node newSSNode = session.run("MATCH (o:SynapseSet:test{datasetBodyId:$ssid1}) RETURN o", parameters("ssid1", "test:1")).single().get(0).asNode();

            //should inherit the id from the first listed synapse set
            Assert.assertEquals("test:1", newSSNode.get("datasetBodyId").asString());

            Long newSSNodeNeuronId = session.run("MATCH (o:SynapseSet:test{datasetBodyId:$ssid1})<-[r:Contains]-(n) RETURN n.bodyId", parameters("ssid1", "test:1")).single().get(0).asLong();

            //should only be contained by the new node
            Assert.assertEquals(new Long(1), newSSNodeNeuronId);

            int numberOfRelationships = session.run("MATCH (o:SynapseSet:test{datasetBodyId:$ssid1})-[r:Contains]->(n) RETURN count(n)", parameters("ssid1", "test:1")).single().get(0).asInt();

            //number of relationships to synapses should be equal to sum from node1 and node2
            Assert.assertEquals(3, numberOfRelationships);

        }
    }

    @Test
    public void shouldDeleteEntireSkeletonUponMerge() {

        File swcFile1 = new File("src/test/resources/101.swc");
        File swcFile2 = new File("src/test/resources/102.swc");
        List<File> listOfSwcFiles = new ArrayList<>();
        listOfSwcFiles.add(swcFile1);
        listOfSwcFiles.add(swcFile2);

        List<Skeleton> skeletonList = new ArrayList<>();

        for (File swcFile : listOfSwcFiles) {
            String filepath = swcFile.getAbsolutePath();
            Long associatedBodyId = ConnConvert.setSkeletonAssociatedBodyId(filepath);
            Skeleton skeleton = new Skeleton();

            try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
                skeleton.fromSwc(reader, associatedBodyId);
                skeletonList.add(skeleton);
                System.out.println("Loaded skeleton associated with bodyId " + associatedBodyId + " and size " + skeleton.getSkelNodeList().size());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            neo4jImporter.addSkeletonNodes("test", skeletonList);

            Assert.assertEquals("test:101", session.run("MATCH (n{bodyId:101})-[:Contains]->(s:Skeleton) RETURN s.skeletonId").single().get(0).asString());

            session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset)", parameters("bodyId1", 101, "bodyId2", 102, "dataset", "test"));

            Assert.assertFalse(session.run("MATCH (n{bodyId:101})-[:Contains]->(s:Skeleton) RETURN s.skeletonId").hasNext());

            Assert.assertFalse(session.run("MATCH (n{bodyId:102})-[:Contains]->(s:Skeleton) RETURN s.skeletonId").hasNext());

            Assert.assertFalse(session.run("MATCH (n:Skeleton) RETURN n").hasNext());

            Assert.assertFalse(session.run("MATCH (n:SkelNode) RETURN n").hasNext());


        }

    }

    @Test
    public void shouldCombineNeuronParts() {

        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);
            neo4jImporter.addConnectsTo(dataset, bodyList, 10);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();
            }
            neo4jImporter.addNeuronParts(dataset, bodyList);

            session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset)", parameters("bodyId1", 8426959, "bodyId2", 26311, "dataset", dataset));

            int neuronPartCount = session.run("MATCH (n{bodyId:8426959})<-[:PartOf]-(np) RETURN count(np)").single().get(0).asInt();

            Assert.assertEquals(3, neuronPartCount);

            Long roiAPreCount = session.run("MATCH (n{bodyId:8426959})<-[:PartOf]-(np:roiA) RETURN np.pre").single().get(0).asLong();

            Assert.assertEquals(new Long(2), roiAPreCount);

            Long roiAPostCount = session.run("MATCH (n{bodyId:8426959})<-[:PartOf]-(np:roiA) RETURN np.post").single().get(0).asLong();

            Assert.assertEquals(new Long(1), roiAPostCount);

            Long roiASizeCount = session.run("MATCH (n{bodyId:8426959})<-[:PartOf]-(np:roiA) RETURN np.size").single().get(0).asLong();

            Assert.assertEquals(new Long(3), roiASizeCount);

            Long scRoiSizeCount = session.run("MATCH (n{bodyId:8426959})<-[:PartOf]-(np:seven_column_roi) RETURN np.size").single().get(0).asLong();

            Assert.assertEquals(new Long(4), scRoiSizeCount);

            int neuronPartCountForOldBody = session.run("MATCH (n{mergedBodyId:8426959})<-[:PartOf]-(np) RETURN count(np)").single().get(0).asInt();

            Assert.assertEquals(0, neuronPartCountForOldBody);


        }

    }

    @Test
    public void shouldApplyAppropriateLabelsToNeurons() {

        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);
            neo4jImporter.addConnectsTo(dataset, bodyList, 10);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();
            }
            neo4jImporter.addNeuronParts(dataset, bodyList);

            Set<String> oldNeuronLabelList = session.run("MATCH (n:Neuron:test{bodyId:$bodyId}) RETURN labels(n)", parameters("bodyId", 8426959)).single().get(0).asList().stream().map(object -> Objects.toString(object, null)).collect(Collectors.toSet());

            oldNeuronLabelList.addAll(session.run("MATCH (n:Neuron:test{bodyId:$bodyId}) RETURN labels(n)", parameters("bodyId", 26311)).single().get(0).asList().stream().map(object -> Objects.toString(object, null)).collect(Collectors.toSet()));

            session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset)", parameters("bodyId1", 8426959, "bodyId2", 26311, "dataset", dataset));

            Set<String> newNeuronLabelList = session.run("MATCH (n:Neuron:test{bodyId:$bodyId}) RETURN labels(n)", parameters("bodyId", 8426959)).single().get(0).asList().stream().map(object -> Objects.toString(object, null)).collect(Collectors.toSet());

            Assert.assertEquals(oldNeuronLabelList, newNeuronLabelList);

        }
    }

    @Test
    public void shouldInheritNeuronPropertiesAppropriately() {

        List<Neuron> neuronList = ConnConvert.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addNeurons(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList, 10);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();
            }
            neo4jImporter.addNeuronParts(dataset, bodyList);

            Node newNode = session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset) YIELD node RETURN node", parameters("bodyId1", 8426959, "bodyId2", 26311, "dataset", dataset)).single().get(0).asNode();

            Map<String,Object> newNodeProperties = newNode.asMap();

            Assert.assertEquals(2L, newNodeProperties.get("pre"));
            Assert.assertEquals(2L, newNodeProperties.get("post"));
            Assert.assertEquals(3158061L+14766999L,newNodeProperties.get("size"));
            Assert.assertEquals("Dm12-4",newNodeProperties.get( "name"));
            Assert.assertEquals(8426959L,newNodeProperties.get("bodyId"));
            Assert.assertEquals("Dm",newNodeProperties.get("type"));
            Assert.assertEquals("final",newNodeProperties.get("status"));
            Assert.assertNull(newNodeProperties.get("somaLocation"));



        }
    }

    @Test
    public void shouldConvertOldNodePropertiesToMergedAndRemoveLabelsAndRelationshipsExceptMergedTo () {


        List<Neuron> neuronList = ConnConvert.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addNeurons(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList, 10);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();
            }
            neo4jImporter.addNeuronParts(dataset, bodyList);

            Node newNode = session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset) YIELD node RETURN node", parameters("bodyId1", 8426959, "bodyId2", 26311, "dataset", dataset)).single().get(0).asNode();

            Node node1 = session.run("MATCH (n{mergedBodyId:$bodyId}) RETURN n", parameters("bodyId",8426959)).single().get(0).asNode();
            Node node2 = session.run("MATCH (n{mergedBodyId:$bodyId}) RETURN n", parameters("bodyId",26311)).single().get(0).asNode();

            Map<String, Object> node1Properties = node1.asMap();
            Map<String, Object> node2Properties = node2.asMap();

            for (String propertyName : node1Properties.keySet()) {
                if (!propertyName.equals("timeStamp")) {
                    Assert.assertTrue(propertyName.startsWith("merged"));
                }
            }

            for (String propertyName : node2Properties.keySet()) {
                if (!propertyName.equals("timeStamp")) {
                    Assert.assertTrue(propertyName.startsWith("merged"));
                }
            }

            Assert.assertFalse(node1.labels().iterator().hasNext());
            Assert.assertFalse(node2.labels().iterator().hasNext());

            List<Record> node1Relationships = session.run("MATCH (n{mergedBodyId:$bodyId})-[r]->() RETURN r", parameters("bodyId",8426959)).list();
            List<Record> node2Relationships = session.run("MATCH (n{mergedBodyId:$bodyId})-[r]->() RETURN r", parameters("bodyId",26311)).list();

            Assert.assertEquals(1,node1Relationships.size());
            Assert.assertEquals(1,node2Relationships.size());

            Relationship r1 = (Relationship) node1Relationships.get(0).asMap().get("r");
            Assert.assertTrue(r1.hasType("MergedTo") && r1.endNodeId()==newNode.id());
            Relationship r2 = (Relationship) node2Relationships.get(0).asMap().get("r");
            Assert.assertTrue(r2.hasType("MergedTo") && r2.endNodeId()==newNode.id());
        }
    }

    @Test
    public void shouldApplyTimeStampToAllNodesAfterMerge() {

        List<Neuron> neuronList = ConnConvert.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addNeurons(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList, 10);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();
            }
            neo4jImporter.addNeuronParts(dataset, bodyList);

            Node newNode = session.run("CALL proofreader.mergeNeurons($bodyId1,$bodyId2,$dataset) YIELD node RETURN node", parameters("bodyId1", 8426959, "bodyId2", 26311, "dataset", dataset)).single().get(0).asNode();

            Node mergedNode1 = session.run("MATCH (n{mergedBodyId:8426959}) RETURN n").single().get(0).asNode();

            Assert.assertTrue(mergedNode1.asMap().containsKey("timeStamp"));


        }
    }
}



