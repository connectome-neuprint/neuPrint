package org.janelia.flyem.neuprintprocedures;

import apoc.convert.Json;
import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import com.google.gson.Gson;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
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
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class MergeNeuronsTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ProofreaderProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withFunction(Json.class)
            .withProcedure(Create.class);

    //TODO: write tests for exception/error handling.
    //TODO: consolidate this into fewer tests

    @Test
    public void shouldGetConnectsToForBothNodesWithSummedWeights() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            String connectsToTestJson = "{\"DVIDuuid\": \"7254f5a8aacf4e6f804dcbddfdac4f7f\", \"MutationID\": 68387," +
                    "\"Action\": \"merge\", \"TargetBodyID\": 1, \"BodiesMerged\": [2], " +
                    "\"TargetBodySize\": 216685762, \"TargetBodySynapses\":[]}";

            session.writeTransaction(tx -> tx.run("CREATE (n:`test-Neuron`:Neuron:test{bodyId:$id1}), (m:`test-Neuron`:Neuron:test{bodyId:$id2})," +
                            " (o{bodyId:$id3}), (p{bodyId:$id4}), (s:SynapseSet{datasetBodyId:\"test:1\"})," +
                            " (ss:SynapseSet{datasetBodyId:\"test:2\"}) \n" +
                            "CREATE (n)-[:ConnectsTo{weight:7}]->(o) \n" +
                            "CREATE (m)-[:ConnectsTo{weight:23}]->(o) \n" +
                            "CREATE (o)-[:ConnectsTo{weight:13}]->(n) \n" +
                            "CREATE (o)-[:ConnectsTo{weight:5}]->(m) \n" +
                            "CREATE (o)-[:ConnectsTo{weight:17}]->(p) \n" +
                            "CREATE (n)-[:ConnectsTo{weight:2}]->(n) \n" +
                            "CREATE (m)-[:ConnectsTo{weight:3}]->(m) \n" +
                            "CREATE (m)-[:ConnectsTo{weight:37}]->(n) \n " +
                            "CREATE (n)-[:ConnectsTo{weight:5}]->(m) \n" +
                            "CREATE (n)-[:Contains]->(s) \n" +
                            "CREATE (m)-[:Contains]->(ss) ",
                    parameters("id1", 1, "id2", 2, "id3", 3, "id4", 4)));

            session.writeTransaction(tx ->
                    tx.run("CALL proofreader.mergeNeuronsFromJson($mergeJson,\"test\") YIELD node RETURN node", parameters("mergeJson", connectsToTestJson)));

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

//    @Test
//    public void shouldContainMergedSynapseSet() {
//
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
//
//            Session session = driver.session();
//
//            String synapseSetTestJson = "{\"Action\": \"merge\", \"TargetBodyID\": 1, \"BodiesMerged\": [2], " +
//                    "\"TargetBodySize\": 216685762, \"TargetBodySynapses\":[" +
//                    "{\"Type\": \"pre\", \"Location\": [ 1, 2, 3 ]}," +
//                    "{\"Type\": \"post\", \"Location\": [ 4, 5, 6 ]}," +
//                    "{\"Type\": \"pre\", \"Location\": [ 7, 8, 9 ]}" +
//                    "]}";
//
//            session.writeTransaction(tx -> tx.run("CREATE (n:Neuron:test{bodyId:$id1}), (m:Neuron:test{bodyId:$id2}), (o:SynapseSet{datasetBodyId:$ssid1}), (p:SynapseSet{datasetBodyId:$ssid2}), (q:Synapse{type:\"pre\",location:\"1:2:3\",x:1,y:2,z:3}), (r:Synapse{type:\"post\",location:\"4:5:6\",x:4,y:5,z:6}), (s:Synapse{type:\"pre\",location:\"7:8:9\",x:7,y:8,z:9}) \n" +
//                            "CREATE (n)-[:Contains]->(o) \n" +
//                            "CREATE (m)-[:Contains]->(p) \n" +
//                            "CREATE (o)-[:Contains]->(q) \n" +
//                            "CREATE (o)-[:Contains]->(r) \n" +
//                            "CREATE (p)-[:Contains]->(s) ",
//                    parameters("id1", 1, "id2", 2, "ssid1", "test:1", "ssid2", "test:2")));
//
//            Node neuron = session.writeTransaction(tx ->
//                    tx.run("CALL proofreader.mergeNeuronsFromJson($mergeJson,\"test\") YIELD node RETURN node", parameters("mergeJson", synapseSetTestJson)).single().get(0).asNode());
//
//            Node newSSNode = session.run("MATCH (o:SynapseSet:test{datasetBodyId:$ssid1}) RETURN o", parameters("ssid1", "test:1")).single().get(0).asNode();
//
//            //should inherit the id from the first listed synapse set
//            Assert.assertEquals("test:1", newSSNode.get("datasetBodyId").asString());
//
//            Long newSSNodeNeuronId = session.run("MATCH (o:SynapseSet:test{datasetBodyId:$ssid1})<-[r:Contains]-(n) RETURN n.bodyId", parameters("ssid1", "test:1")).single().get(0).asLong();
//
//            //should only be contained by the new node
//            Assert.assertEquals(new Long(1), newSSNodeNeuronId);
//
//            int numberOfRelationships = session.run("MATCH (o:SynapseSet:test{datasetBodyId:$ssid1})-[r:Contains]->(n) RETURN count(n)", parameters("ssid1", "test:1")).single().get(0).asInt();
//
//            //number of relationships to synapses should be equal to sum from node1 and node2
//            Assert.assertEquals(3, numberOfRelationships);
//
//        }
//    }

    @Test
    public void shouldAddAppropriatePropertiesLabelsAndRelationshipsToResultingBodyUponRecursiveMerge() {

        String mergeInstructionJson = "{\"DVIDuuid\": \"7254f5a8aacf4e6f804dcbddfdac4f7f\", \"MutationID\": 68387," +
                "\"Action\": \"merge\", \"TargetBodyID\": 8426959, \"BodiesMerged\": [26311, 2589725, 831744], " +
                "\"TargetBodySize\": 216685762, \"TargetBodySynapses\": [" +
                "{\"Type\": \"pre\", \"Location\": [ 4287, 2277, 1542 ]}," +
                "{\"Type\": \"post\", \"Location\": [ 4222, 2402, 1688 ]}," +
                "{\"Type\": \"pre\", \"Location\": [ 4287, 2277, 1502 ]}," +
                "{\"Type\": \"post\", \"Location\": [ 4301, 2276, 1535 ]}," +
                "{\"Type\": \"post\", \"Location\": [ 4000, 3000, 1500 ]}," +
                "{\"Type\": \"pre\", \"Location\": [ 4236, 2394, 1700 ]}," +
                "{\"Type\": \"post\", \"Location\": [ 4298, 2294, 1542 ]}," +
                "{\"Type\": \"post\", \"Location\": [ 4292, 2261, 1542 ]}" +
                "]}";

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        File swcFile1 = new File("src/test/resources/8426959.swc");
        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(new File[]{swcFile1});

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addNeurons(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
            neo4jImporter.addAutoNames(dataset, 0);
            neo4jImporter.addSkeletonNodes(dataset, skeletonList);

            Gson gson = new Gson();
            MergeAction mergeAction = gson.fromJson(mergeInstructionJson, MergeAction.class);

            Node neuron = session.writeTransaction(tx ->
                    tx.run("CALL proofreader.mergeNeuronsFromJson($mergeJson,\"test\") YIELD node RETURN node", parameters("mergeJson", mergeInstructionJson)).single().get(0).asNode());

            //check properties on neuron node
            Map<String, Object> neuronProperties = neuron.asMap();
            Assert.assertEquals(3L, neuronProperties.get("pre"));
            Assert.assertEquals(5L, neuronProperties.get("post"));
            Assert.assertEquals(mergeAction.getTargetBodySize(), neuronProperties.get("size"));
            Assert.assertEquals(mergeAction.getTargetBodyId(), neuronProperties.get("bodyId"));
            Assert.assertEquals("Dm", neuronProperties.get("type"));
            Assert.assertEquals("final", neuronProperties.get("status"));
            //TODO: test synapseCountPerRoi

            //check labels
            Assert.assertTrue(neuron.hasLabel("Neuron"));
            Assert.assertTrue(neuron.hasLabel(dataset));
            Assert.assertTrue(neuron.hasLabel(dataset + "-Neuron"));
            String[] roiArray = new String[]{"seven_column_roi", "roiB", "roiA", "anotherRoi"};
            for (String roi : roiArray) {
                Assert.assertTrue(neuron.hasLabel(roi));
                Assert.assertTrue(neuron.hasLabel(dataset + "-" + roi));
            }

            //check weight of connectsTo relationship
            List<Record> connectsToRelationshipList = session.writeTransaction(tx ->
                    tx.run("MATCH (n:`test-Neuron`{bodyId:8426959})-[r:ConnectsTo]-(m) RETURN r.weight").list());

            Assert.assertEquals(1, connectsToRelationshipList.size());
            Assert.assertEquals(8, connectsToRelationshipList.get(0).get("r.weight").asInt());

            //check synapse set
            List<Record> synapseSetList = session.writeTransaction(tx ->
                    tx.run("MATCH (n:`test-Neuron`{bodyId:8426959})-[r:Contains]-(m:SynapseSet)-[:Contains]->(l:Synapse) RETURN m,count(l)").list());

            Assert.assertEquals(1, synapseSetList.size());
            Assert.assertEquals(8, synapseSetList.get(0).get("count(l)").asInt());

            //no skeleton
            List<Record> skeletonCountList = session.writeTransaction(tx ->
                    tx.run("MATCH (n:`test-Neuron`{bodyId:8426959})-[r:Contains]-(s:Skeleton) RETURN count(s)").list());

            Assert.assertEquals(0, skeletonCountList.get(0).get("count(s)").asInt());
            Assert.assertFalse(session.run("MATCH (n:`test-Skeleton`) RETURN n").hasNext());
            Assert.assertFalse(session.run("MATCH (n:`test-SkelNode`) RETURN n").hasNext());

            //check history
            List<Record> m1HistoryList = session.writeTransaction(tx ->
                    tx.run("MATCH (n:`test-Neuron`{bodyId:8426959})-[r:From]-(h:History)<-[:MergedTo]-(m1) RETURN count(h),m1.mergedBodyId,m1.mergedPost,m1.mergedPre,m1.mergedDvidUuid,m1.mergedMutationId").list());

            Assert.assertEquals(2, m1HistoryList.size());
            Assert.assertEquals(1, m1HistoryList.get(0).get("count(h)").asInt());

            Assert.assertEquals(mergeAction.getDvidUuid(), m1HistoryList.get(0).get("m1.mergedDvidUuid").asString());
            Assert.assertEquals(mergeAction.getMutationId(), (Long) m1HistoryList.get(0).get("m1.mergedMutationId").asLong());

            Long m11BodyId = m1HistoryList.get(0).get("m1.mergedBodyId").asLong();
            Long m11Pre = m1HistoryList.get(0).get("m1.mergedPre").asLong();
            Long m11Post = m1HistoryList.get(0).get("m1.mergedPost").asLong();

            Long m12BodyId = m1HistoryList.get(1).get("m1.mergedBodyId").asLong();
            Long m12Pre = m1HistoryList.get(1).get("m1.mergedPre").asLong();
            Long m12Post = m1HistoryList.get(1).get("m1.mergedPost").asLong();

            Assert.assertEquals(neuronProperties.get("pre"), m11Pre + m12Pre);
            Assert.assertEquals(neuronProperties.get("post"), m11Post + m12Post);
            Long bodyId1 = 8426959L;
            Long bodyId2 = 831744L;
            Assert.assertTrue((m12BodyId.equals(bodyId1) && m11BodyId.equals(bodyId2)) || (m12BodyId.equals(bodyId2) && m11BodyId.equals(bodyId1)));

            List<Record> m2HistoryList = session.writeTransaction(tx ->
                    tx.run("MATCH (n:`test-Neuron`{bodyId:8426959})-[r:From]-(h:History)<-[:MergedTo]-()<-[:MergedTo]-(m2) RETURN m2.mergedBodyId,m2.mergedPost,m2.mergedPre,m2.mergedDvidUuid,m2.mergedMutationId").list());

            Assert.assertEquals(2, m2HistoryList.size());

            Assert.assertEquals(mergeAction.getDvidUuid(), m2HistoryList.get(0).get("m2.mergedDvidUuid").asString());
            Assert.assertEquals(mergeAction.getMutationId(), (Long) m2HistoryList.get(0).get("m2.mergedMutationId").asLong());

            Long m21BodyId = m2HistoryList.get(0).get("m2.mergedBodyId").asLong();
            Long m21Pre = m2HistoryList.get(0).get("m2.mergedPre").asLong();
            Long m21Post = m2HistoryList.get(0).get("m2.mergedPost").asLong();

            Long m22BodyId = m2HistoryList.get(1).get("m2.mergedBodyId").asLong();
            Long m22Pre = m2HistoryList.get(1).get("m2.mergedPre").asLong();
            Long m22Post = m2HistoryList.get(1).get("m2.mergedPost").asLong();

            Assert.assertEquals((long) m12Pre, m21Pre + m22Pre);
            Assert.assertEquals((long) m12Post, m21Post + m22Post);
            Long bodyId3 = 2589725L;
            Assert.assertTrue((m22BodyId.equals(bodyId1) && m21BodyId.equals(bodyId3)) || (m22BodyId.equals(bodyId3) && m21BodyId.equals(bodyId1)));

            List<Record> m3HistoryList = session.writeTransaction(tx ->
                    tx.run("MATCH (n:`test-Neuron`{bodyId:8426959})-[r:From]-(h:History)<-[:MergedTo]-()<-[:MergedTo]-()<-[:MergedTo]-(m3) RETURN m3.mergedBodyId,m3.mergedPost,m3.mergedPre").list());

            Assert.assertEquals(2, m2HistoryList.size());

            Long m31BodyId = m3HistoryList.get(0).get("m3.mergedBodyId").asLong();
            Long m31Pre = m3HistoryList.get(0).get("m3.mergedPre").asLong();
            Long m31Post = m3HistoryList.get(0).get("m3.mergedPost").asLong();

            Long m32BodyId = m3HistoryList.get(1).get("m3.mergedBodyId").asLong();
            Long m32Pre = m3HistoryList.get(1).get("m3.mergedPre").asLong();
            Long m32Post = m3HistoryList.get(1).get("m3.mergedPost").asLong();

            Assert.assertEquals((long) m22Pre, m31Pre + m32Pre);
            Assert.assertEquals((long) m22Post, m31Post + m32Post);
            Long bodyId4 = 26311L;
            Assert.assertTrue((m32BodyId.equals(bodyId1) && m31BodyId.equals(bodyId4)) || (m32BodyId.equals(bodyId4) && m31BodyId.equals(bodyId1)));

            //check that all nodes except Meta have time stamps and dataset labels on everything except ghost bodies
            Integer countOfNodesWithoutTimeStamp = session.readTransaction(tx -> {
                return tx.run("MATCH (n) WHERE (NOT exists(n.timeStamp) AND NOT n:Meta) RETURN count(n)").single().get(0).asInt();
            });
            Assert.assertEquals(new Integer(0), countOfNodesWithoutTimeStamp);

            Integer countOfNodesWithoutDatasetLabel = session.readTransaction(tx -> {
                return tx.run("MATCH (n) WHERE (NOT n:test AND NOT exists(n.mergedBodyId) AND NOT n:DataModel) RETURN count(n)").single().get(0).asInt();
            });

            Assert.assertEquals(new Integer(0), countOfNodesWithoutDatasetLabel);

            //check that ghost nodes have "merged" properties and no labels
            Node mergedNode8426959 = session.run("MATCH (n{mergedBodyId:$bodyId})-[:MergedTo]->(h:History) RETURN n", parameters("bodyId", 8426959)).single().get(0).asNode();
            Node mergedNode26311 = session.run("MATCH (n{mergedBodyId:$bodyId}) RETURN n", parameters("bodyId", 26311)).single().get(0).asNode();

            Map<String, Object> node8426959Properties = mergedNode8426959.asMap();
            Map<String, Object> node26311Properties = mergedNode26311.asMap();
            for (String propertyName : node8426959Properties.keySet()) {
                if (!propertyName.equals("timeStamp")) {
                    Assert.assertTrue(propertyName.startsWith("merged"));
                }
            }
            for (String propertyName : node26311Properties.keySet()) {
                if (!propertyName.equals("timeStamp")) {
                    Assert.assertTrue(propertyName.startsWith("merged"));
                }
            }
            Assert.assertFalse(mergedNode8426959.labels().iterator().hasNext());
            Assert.assertFalse(mergedNode26311.labels().iterator().hasNext());

        }

    }

}



