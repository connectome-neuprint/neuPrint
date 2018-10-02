//package org.janelia.flyem.neuprintprocedures.proofreading;
//
//import apoc.convert.Json;
//import apoc.create.Create;
//import apoc.refactor.GraphRefactoring;
//import com.google.gson.Gson;
//import com.google.gson.reflect.TypeToken;
//import org.janelia.flyem.neuprinter.Neo4jImporter;
//import org.janelia.flyem.neuprinter.NeuPrinterMain;
//import org.janelia.flyem.neuprinter.SynapseMapper;
//import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
//import org.janelia.flyem.neuprinter.model.Neuron;
//import org.janelia.flyem.neuprinter.model.Skeleton;
//import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
//import org.janelia.flyem.neuprinter.model.SynapseCounter;
//import org.janelia.flyem.neuprintprocedures.functions.NeuPrintUserFunctions;
//import org.junit.Assert;
//import org.junit.Rule;
//import org.junit.Test;
//import org.neo4j.driver.v1.Config;
//import org.neo4j.driver.v1.Driver;
//import org.neo4j.driver.v1.GraphDatabase;
//import org.neo4j.driver.v1.Record;
//import org.neo4j.driver.v1.Session;
//import org.neo4j.driver.v1.types.Node;
//import org.neo4j.driver.v1.types.Relationship;
//import org.neo4j.harness.junit.Neo4jRule;
//
//import java.io.File;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import static org.neo4j.driver.v1.Values.parameters;
//
//public class CleaveOrSplitNeuronsTest {
//
//    @Rule
//    public Neo4jRule neo4j = new Neo4jRule()
//            .withProcedure(ProofreaderProcedures.class)
//            .withFunction(NeuPrintUserFunctions.class)
//            .withProcedure(GraphRefactoring.class)
//            .withFunction(Json.class)
//            .withProcedure(Create.class);
//
//    //TODO: write tests for exception/error handling.
//
//    @Test
//    public void shouldCleaveNeurons() {
//        String cleaveInstructionJson = "{\"DVIDuuid\": \"7254f5a8aacf4e6f804dcbddfdac4f7f\", \"MutationID\": 68387, " +
//                "\"Action\": \"cleave\", \"NewBodyId\": 5555, \"OrigBodyId\": 8426959, " +
//                "\"NewBodySize\": 2778831, \"NewBodySynapses\": [" +
//                "{\"Type\": \"pre\", \"Location\": [ 4287, 2277, 1542 ]}," +
//                "{\"Type\": \"post\", \"Location\": [ 4222, 2402, 1688 ]}" +
//                "]}";
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//        bodyList.sort(new SortBodyByNumberOfSynapses());
//
//        File swcFile1 = new File("src/test/resources/8426959.swc");
//        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(new File[]{swcFile1});
//
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
//
//            Session session = driver.session();
//            String dataset = "test";
//
//            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
//            neo4jImporter.prepDatabase(dataset);
//
//            neo4jImporter.addSegments(dataset, neuronList);
//
//            neo4jImporter.addConnectsTo(dataset, bodyList);
//            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
//            neo4jImporter.addSynapsesTo(dataset, preToPost);
//            neo4jImporter.addSegmentRois(dataset, bodyList);
//            neo4jImporter.addSynapseSets(dataset, bodyList);
//            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
//            neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);
//            neo4jImporter.addSkeletonNodes(dataset, skeletonList);
//
//            List<Object> neurons = session.writeTransaction(tx ->
//                    tx.run("CALL proofreader.cleaveNeuronFromJson($cleaveJson,\"test\") YIELD nodes RETURN nodes", parameters("cleaveJson", cleaveInstructionJson)).single().get(0).asList());
//
//            Node neuron1 = (Node) neurons.get(0);
//            Node neuron2 = (Node) neurons.get(1);
//
//            Gson gson = new Gson();
//            CleaveOrSplitAction cleaveOrSplitAction = gson.fromJson(cleaveInstructionJson, CleaveOrSplitAction.class);
//            Assert.assertEquals("cleave", cleaveOrSplitAction.getAction());
//
//            //check properties on new nodes
//            Assert.assertEquals(cleaveOrSplitAction.getNewBodyId(), neuron1.asMap().get("bodyId"));
//            Assert.assertEquals(cleaveOrSplitAction.getNewBodySize(), neuron1.asMap().get("size"));
//            Assert.assertEquals(1L, neuron1.asMap().get("pre"));
//            Assert.assertEquals(1L, neuron1.asMap().get("post"));
//            Assert.assertEquals(cleaveOrSplitAction.getOriginalBodyId(), neuron2.asMap().get("bodyId"));
//            Assert.assertEquals(14766999L, neuron2.asMap().get("size"));
//            Assert.assertEquals(1L, neuron2.asMap().get("pre"));
//            Assert.assertEquals(0L, neuron2.asMap().get("post"));
//            Assert.assertEquals("Dm", neuron2.asMap().get("type"));
//            Assert.assertEquals("final", neuron2.asMap().get("status"));
//
//            //check labels
//            Assert.assertTrue(neuron1.hasLabel("Segment"));
//            Assert.assertTrue(neuron1.hasLabel(dataset));
//            Assert.assertTrue(neuron1.hasLabel(dataset + "-Segment"));
//            String[] roiArray1 = new String[]{"seven_column_roi", "roiB", "roiA"};
//            for (String roi : roiArray1) {
//                Assert.assertTrue(neuron1.asMap().containsKey(roi));
//            }
//            Assert.assertTrue(neuron2.hasLabel("Segment"));
//            Assert.assertTrue(neuron2.hasLabel(dataset));
//            Assert.assertTrue(neuron1.hasLabel(dataset + "-Segment"));
//            String[] roiArray2 = new String[]{"seven_column_roi", "roiA"};
//            for (String roi : roiArray2) {
//                Assert.assertTrue(neuron2.asMap().containsKey(roi));
//            }
//            Assert.assertFalse(neuron2.hasLabel("roiB"));
//
//            //should delete all skeletons
//            Assert.assertFalse(session.run("MATCH (n:`test-Skeleton`:test:Skeleton) RETURN n").hasNext());
//            Assert.assertFalse(session.run("MATCH (n:`test-SkelNode`:test:SkelNode) RETURN n").hasNext());
//
//            //all properties on ghost node should be prefixed with "cleaved", all labels removed, only relationships to history node, mutationid and dviduuid added
//            Node prevOrigNode = session.run("MATCH (n{cleavedBodyId:$bodyId}) RETURN n", parameters("bodyId", 8426959)).single().get(0).asNode();
//
//            Map<String, Object> node1Properties = prevOrigNode.asMap();
//
//            for (String propertyName : node1Properties.keySet()) {
//                if (!propertyName.equals("timeStamp")) {
//                    Assert.assertTrue(propertyName.startsWith("cleaved"));
//                }
//            }
//
//            Assert.assertEquals(cleaveOrSplitAction.getDvidUuid(), node1Properties.get("cleavedDvidUuid"));
//            Assert.assertEquals(cleaveOrSplitAction.getMutationId(), node1Properties.get("cleavedMutationId"));
//
//            Assert.assertFalse(prevOrigNode.labels().iterator().hasNext());
//
//            List<Record> prevOrigNodeRelationships = session.run("MATCH (n{cleavedBodyId:$bodyId})-[r]->() RETURN r", parameters("bodyId", 8426959)).list();
//            List<Record> origNeuronHistoryNode = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:$bodyId})-[:From]->(h:History) RETURN h", parameters("bodyId", 8426959)).list();
//            List<Record> newNeuronHistoryNode = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:$bodyId})-[:From]->(h:History) RETURN h", parameters("bodyId", 5555)).list();
//
//            Assert.assertEquals(2, prevOrigNodeRelationships.size());
//            Assert.assertEquals(1, origNeuronHistoryNode.size());
//            Node historyNodeOrig = (Node) origNeuronHistoryNode.get(0).asMap().get("h");
//            Node historyNodeNew = (Node) newNeuronHistoryNode.get(0).asMap().get("h");
//
//            Relationship r1 = (Relationship) prevOrigNodeRelationships.get(0).asMap().get("r");
//            Relationship r2 = (Relationship) prevOrigNodeRelationships.get(1).asMap().get("r");
//            Assert.assertTrue(r1.hasType("CleavedTo") && r2.hasType("CleavedTo"));
//            Assert.assertTrue((r1.endNodeId() == historyNodeOrig.id() && r2.endNodeId() == historyNodeNew.id()) ||
//                    (r2.endNodeId() == historyNodeOrig.id() && r1.endNodeId() == historyNodeNew.id()));
//
//            //check connectsto relationships
//            Long origTo26311Weight = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})-[r:ConnectsTo]->(m:Segment:test:`test-Segment`{bodyId:26311}) RETURN r.weight").single().get(0).asLong();
//            Assert.assertEquals(new Long(1), origTo26311Weight);
//            Long origTo2589725Weight = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})-[r:ConnectsTo]->(m:Segment:test:`test-Segment`{bodyId:2589725}) RETURN r.weight").single().get(0).asLong();
//            Assert.assertEquals(new Long(1), origTo2589725Weight);
//            Long origTo831744Weight = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})-[r:ConnectsTo]->(m:Segment:test:`test-Segment`{bodyId:831744}) RETURN r.weight").single().get(0).asLong();
//            Assert.assertEquals(new Long(1), origTo831744Weight);
//
//            Long newTo26311Weight = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:5555})-[r:ConnectsTo]->(m:Segment:test:`test-Segment`{bodyId:26311}) RETURN r.weight").single().get(0).asLong();
//            Assert.assertEquals(new Long(2), newTo26311Weight);
//            Long newTo2589725Weight = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:5555})-[r:ConnectsTo]->(m:Segment:test:`test-Segment`{bodyId:2589725}) RETURN r.weight").single().get(0).asLong();
//            Assert.assertEquals(new Long(1), newTo2589725Weight);
//            Long newTo831744Weight = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:5555})-[r:ConnectsTo]->(m:Segment:test:`test-Segment`{bodyId:831744}) RETURN r.weight").single().get(0).asLong();
//            Assert.assertEquals(new Long(1), newTo831744Weight);
//
//            //check synapseCountsPerRoi
//            String newSynapseCountPerRoi = session.writeTransaction(tx ->
//                    tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:5555}) RETURN n.roiInfo").single().get(0).asString());
//            String origSynapseCountPerRoi = session.writeTransaction(tx ->
//                    tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959}) RETURN n.roiInfo").single().get(0).asString());
//            Map<String, SynapseCounter> newSynapseCountMap = gson.fromJson(newSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
//            }.getType());
//            Map<String, SynapseCounter> origSynapseCountMap = gson.fromJson(origSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
//            }.getType());
//
//            Assert.assertEquals(3, newSynapseCountMap.keySet().size());
//            Assert.assertEquals(1, newSynapseCountMap.get("roiA").getPre());
//            Assert.assertEquals(1, newSynapseCountMap.get("roiB").getPost());
//
//            System.out.println(origSynapseCountPerRoi);
//
//            Assert.assertEquals(2, origSynapseCountMap.keySet().size());
//            Assert.assertEquals(0, origSynapseCountMap.get("seven_column_roi").getPost());
//            Assert.assertEquals(1, origSynapseCountMap.get("roiA").getPre());
//
//            //check synapse sets
//            List<Record> origSynapseSetList = session.writeTransaction(tx ->
//                    tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:8426959})-[r:Contains]-(m:SynapseSet:test:`test-SynapseSet`)-[:Contains]->(l:`test-Synapse`:test:Synapse) RETURN m,count(l)").list());
//            Assert.assertEquals(1, origSynapseSetList.size());
//            Assert.assertEquals(1, origSynapseSetList.get(0).get("count(l)").asInt());
//
//            List<Record> newSynapseSetList = session.writeTransaction(tx ->
//                    tx.run("MATCH (n:Segment:test:`test-Segment`{bodyId:5555})-[r:Contains]-(m:SynapseSet:test:`test-SynapseSet`)-[:Contains]->(l:`test-Synapse`:test:Synapse) RETURN m,count(l)").list());
//            Assert.assertEquals(1, newSynapseSetList.size());
//            Assert.assertEquals(2, newSynapseSetList.get(0).get("count(l)").asInt());
//
//            //check that everything except Meta node has time stamp and all have dataset label except the cleaved ghost body
//            Integer countOfNodesWithoutTimeStamp = session.readTransaction(tx -> tx.run("MATCH (n) WHERE (NOT exists(n.timeStamp) AND NOT n:Meta) RETURN count(n)").single().get(0).asInt());
//            Assert.assertEquals(new Integer(0), countOfNodesWithoutTimeStamp);
//            Integer countOfNodesWithoutDatasetLabel = session.readTransaction(tx -> tx.run("MATCH (n) WHERE NOT n:test AND NOT n:DataModel RETURN count(n)").single().get(0).asInt());
//            Assert.assertEquals(new Integer(1), countOfNodesWithoutDatasetLabel);
//
//        }
//    }
//
//    @Test
//    public void shouldSplitIfActionIsListedAsSplit() {
//        String splitInstructionJson = "{\"DVIDuuid\": \"7254f5a8aacf4e6f804dcbddfdac4f7f\", \"MutationID\": 68387," +
//                "\"Action\": \"split\", \"NewBodyId\": 5555, \"OrigBodyId\": 8426959, " +
//                "\"NewBodySize\": 2778831, \"NewBodySynapses\": [" +
//                "{\"Type\": \"pre\", \"Location\": [ 4287, 2277, 1542 ]}," +
//                "{\"Type\": \"post\", \"Location\": [ 4222, 2402, 1688 ]}" +
//                "]}";
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//        bodyList.sort(new SortBodyByNumberOfSynapses());
//
//        File swcFile1 = new File("src/test/resources/8426959.swc");
//        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(new File[]{swcFile1});
//
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
//
//            Session session = driver.session();
//            String dataset = "test";
//
//            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
//            neo4jImporter.prepDatabase(dataset);
//
//            neo4jImporter.addSegments(dataset, neuronList);
//
//            neo4jImporter.addConnectsTo(dataset, bodyList);
//            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
//            neo4jImporter.addSynapsesTo(dataset, preToPost);
//            neo4jImporter.addSegmentRois(dataset, bodyList);
//            neo4jImporter.addSynapseSets(dataset, bodyList);
//            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
//            neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);
//            neo4jImporter.addSkeletonNodes(dataset, skeletonList);
//
//            session.writeTransaction(tx ->
//                    tx.run("CALL proofreader.cleaveNeuronFromJson($splitJson,\"test\") YIELD nodes RETURN nodes", parameters("splitJson", splitInstructionJson)).single().get(0).asList());
//
//            Gson gson = new Gson();
//            CleaveOrSplitAction cleaveOrSplitAction = gson.fromJson(splitInstructionJson, CleaveOrSplitAction.class);
//            Assert.assertEquals("split", cleaveOrSplitAction.getAction());
//
//            //all properties on ghost node should be prefixed with "split", all labels removed, only relationships to history node
//            Node prevOrigNode = session.run("MATCH (n{splitBodyId:$bodyId}) RETURN n", parameters("bodyId", 8426959)).single().get(0).asNode();
//
//            Map<String, Object> node1Properties = prevOrigNode.asMap();
//
//            for (String propertyName : node1Properties.keySet()) {
//                if (!propertyName.equals("timeStamp")) {
//                    Assert.assertTrue(propertyName.startsWith("split"));
//                }
//            }
//
//            Assert.assertFalse(prevOrigNode.labels().iterator().hasNext());
//
//            List<Record> prevOrigNodeRelationships = session.run("MATCH (n{splitBodyId:$bodyId})-[r]->() RETURN r", parameters("bodyId", 8426959)).list();
//            List<Record> origNeuronHistoryNode = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:$bodyId})-[:From]->(h:History) RETURN h", parameters("bodyId", 8426959)).list();
//            List<Record> newNeuronHistoryNode = session.run("MATCH (n:Segment:test:`test-Segment`{bodyId:$bodyId})-[:From]->(h:History) RETURN h", parameters("bodyId", 5555)).list();
//
//            Assert.assertEquals(2, prevOrigNodeRelationships.size());
//            Assert.assertEquals(1, origNeuronHistoryNode.size());
//            Node historyNodeOrig = (Node) origNeuronHistoryNode.get(0).asMap().get("h");
//            Node historyNodeNew = (Node) newNeuronHistoryNode.get(0).asMap().get("h");
//
//            Relationship r1 = (Relationship) prevOrigNodeRelationships.get(0).asMap().get("r");
//            Relationship r2 = (Relationship) prevOrigNodeRelationships.get(1).asMap().get("r");
//            Assert.assertTrue(r1.hasType("SplitTo") && r2.hasType("SplitTo"));
//            Assert.assertTrue((r1.endNodeId() == historyNodeOrig.id() && r2.endNodeId() == historyNodeNew.id()) ||
//                    (r2.endNodeId() == historyNodeOrig.id() && r1.endNodeId() == historyNodeNew.id()));
//
//        }
//    }
//
//    @Test
//    public void shouldWorkWhenOriginalBodyDoesNotExistInDatabase() {
//        String cleaveInstructionJson = "{\"DVIDuuid\": \"7254f5a8aacf4e6f804dcbddfdac4f7f\", \"MutationID\": 68387, " +
//                "\"Action\": \"cleave\", \"NewBodyId\": 5555, \"OrigBodyId\": 1, " +
//                "\"NewBodySize\": 2778831, \"NewBodySynapses\": [" +
//                "]}";
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//        bodyList.sort(new SortBodyByNumberOfSynapses());
//
//        File swcFile1 = new File("src/test/resources/8426959.swc");
//        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(new File[]{swcFile1});
//
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
//
//            Session session = driver.session();
//            String dataset = "test";
//
//            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
//            neo4jImporter.prepDatabase(dataset);
//
//            neo4jImporter.addSegments(dataset, neuronList);
//
//            neo4jImporter.addConnectsTo(dataset, bodyList);
//            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
//            neo4jImporter.addSynapsesTo(dataset, preToPost);
//            neo4jImporter.addSegmentRois(dataset, bodyList);
//            neo4jImporter.addSynapseSets(dataset, bodyList);
//            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
//            neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);
//            neo4jImporter.addSkeletonNodes(dataset, skeletonList);
//
//            List<Object> neurons = session.writeTransaction(tx ->
//                    tx.run("CALL proofreader.cleaveNeuronFromJson($cleaveJson,\"test\") YIELD nodes RETURN nodes", parameters("cleaveJson", cleaveInstructionJson)).single().get(0).asList());
//
//            Node neuron1 = (Node) neurons.get(0);
//            Node neuron2 = (Node) neurons.get(1);
//
//            Assert.assertEquals(0, (long) neuron1.asMap().get("pre") + (long) neuron1.asMap().get("post") + (long) neuron2.asMap().get("pre") + (long) neuron2.asMap().get("post"));
//            Assert.assertEquals("{}", neuron1.asMap().get("roiInfo"));
//            Assert.assertEquals("{}", neuron2.asMap().get("roiInfo"));
//            if ((long) neuron1.asMap().get("bodyId") == 5555) {
//                Assert.assertEquals(2778831, (long) neuron1.asMap().get("size"));
//            } else {
//                Assert.assertEquals(2778831, (long) neuron2.asMap().get("size"));
//            }
//
//            int relationshipCount5555 = session.readTransaction(tx ->
//                    tx.run("MATCH (n:`test-Segment`{bodyId:5555})-[r]->() RETURN count(r)").single().get(0).asInt());
//            Assert.assertEquals(2, relationshipCount5555);
//
//            int relationshipCount1 = session.readTransaction(tx ->
//                    tx.run("MATCH (n:`test-Segment`{bodyId:1})-[r]->() RETURN count(r)").single().get(0).asInt());
//            Assert.assertEquals(2, relationshipCount1);
//
//        }
//    }
//}
