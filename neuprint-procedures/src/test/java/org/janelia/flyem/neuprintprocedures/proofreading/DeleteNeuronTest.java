//package org.janelia.flyem.neuprintprocedures;
//
//import apoc.convert.Json;
//import apoc.create.Create;
//import apoc.refactor.GraphRefactoring;
//import org.janelia.flyem.neuprinter.Neo4jImporter;
//import org.janelia.flyem.neuprinter.NeuPrinterMain;
//import org.janelia.flyem.neuprinter.SynapseMapper;
//import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
//import org.janelia.flyem.neuprinter.model.Neuron;
//import org.janelia.flyem.neuprinter.model.Skeleton;
//import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
//import org.janelia.flyem.neuprintprocedures.proofreading.ProofreaderProcedures;
//import org.junit.Assert;
//import org.junit.Rule;
//import org.junit.Test;
//import org.neo4j.driver.v1.Config;
//import org.neo4j.driver.v1.Driver;
//import org.neo4j.driver.v1.GraphDatabase;
//import org.neo4j.driver.v1.Record;
//import org.neo4j.driver.v1.Session;
//import org.neo4j.driver.v1.Values;
//import org.neo4j.driver.v1.types.Node;
//import org.neo4j.driver.v1.types.Point;
//import org.neo4j.harness.junit.Neo4jRule;
//
//import java.io.File;
//import java.time.LocalDateTime;
//import java.time.temporal.ChronoUnit;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Set;
//
//import static org.neo4j.driver.v1.Values.parameters;
//
//public class DeleteNeuronTest {
//
//    @Rule
//    public Neo4jRule neo4j = new Neo4jRule()
//            .withProcedure(ProofreaderProcedures.class)
//            .withProcedure(GraphRefactoring.class)
//            .withProcedure(Create.class)
//            .withFunction(Json.class);
//
//    @Test
//    public void shouldDeleteNeuronAndAllAssociatedNodesExceptSynapseNodes() {
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//        bodyList.sort(new SortBodyByNumberOfSynapses());
//
//        File swcFile = new File("src/test/resources/8426959.swc");
//
//        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(new File[]{swcFile});
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
//
//            neo4jImporter.addSkeletonNodes(dataset, skeletonList);
//            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
//            neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);
//
//            session.writeTransaction(tx -> tx.run("MATCH (n:Segment) SET n.timeStamp=$timeStamp", parameters("timeStamp", LocalDateTime.of(2000, 1, 1,1,1))));
//            session.writeTransaction(tx -> tx.run("MATCH (n:Meta) SET n.lastDatabaseEdit=$timeStamp", parameters("timeStamp", LocalDateTime.of(2000, 1, 1,1,1))));
//            String synapseCountPerRoi = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n.roiInfo").single().get(0).asString());
//            long preCount = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n.totalPreCount").single().get(0).asLong());
//            long postCount = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n.totalPostCount").single().get(0).asLong());
//
//            session.writeTransaction(tx -> tx.run("CALL proofreader.deleteNeuron($bodyId,$dataset)", parameters("bodyId", 8426959, "dataset", "test")));
//
//            int deletedNeuronNodeCount = session.readTransaction(tx -> tx.run("MATCH (n:Segment{bodyId:8426959}) RETURN count(n)").single().get(0).asInt());
//
//            Assert.assertEquals(0, deletedNeuronNodeCount);
//
//            int deletedSSNodeCount = session.readTransaction(tx -> tx.run("MATCH (n:SynapseSet{datasetBodyId:\"test:8426959\"}) RETURN count(n)").single().get(0).asInt());
//
//            Assert.assertEquals(0, deletedSSNodeCount);
//
//            Point locationPoint1 = Values.point(9157, 4287, 2277, 1542).asPoint();
//            int deletedSynapse1NodeCount = session.readTransaction(tx -> tx.run("MATCH (n:Synapse{location:$location}) RETURN count(n)", parameters("location", locationPoint1)).single().get(0).asInt());
//
//            Assert.assertEquals(1, deletedSynapse1NodeCount);
//
//            Point locationPoint2 = Values.point(9157, 4222, 2402, 1688).asPoint();
//            int deletedSynapse2NodeCount = session.readTransaction(tx -> tx.run("MATCH (n:Synapse{location:$location}) RETURN count(n)", parameters("location", locationPoint2)).single().get(0).asInt());
//
//            Assert.assertEquals(1, deletedSynapse2NodeCount);
//
//            Point locationPoint3 = Values.point(9157, 4287, 2277, 1502).asPoint();
//            int deletedSynapse3NodeCount = session.readTransaction(tx -> tx.run("MATCH (n:Synapse{location:$location}) RETURN count(n)", parameters("location", locationPoint3)).single().get(0).asInt());
//
//            Assert.assertEquals(1, deletedSynapse3NodeCount);
//
//            int deletedSkelNodeCount = session.readTransaction(tx -> tx.run("MATCH (n:SkelNode) WHERE n.skelNodeId STARTS WITH \"test:8426959\" RETURN count(n)").single().get(0).asInt());
//
//            Assert.assertEquals(0, deletedSkelNodeCount);
//
//            int deletedSkeletonCount = session.readTransaction(tx -> tx.run("MATCH (n:Skeleton{skeletonId:\"test:8426959\"}) RETURN count(n)").single().get(0).asInt());
//
//            Assert.assertEquals(0, deletedSkeletonCount);
//
//            // check time stamps on previously connected nodes
//
//            List<Record> neuronTimeStamps = session.readTransaction(tx -> tx.run("MATCH (n:Segment) WHERE n.bodyId=26311 OR n.bodyId=2589725 OR n.bodyId=831744 RETURN n.timeStamp").list());
//
//            for (Record record : neuronTimeStamps) {
//                LocalDateTime dateTime = (LocalDateTime) record.asMap().get("n.timeStamp");
//                Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), dateTime.truncatedTo(ChronoUnit.HOURS));
//            }
//
//            // check meta node time stamps and synapseCountsPerRoi/preCount/postCount
//
//            Node metaNode = session.readTransaction(tx -> tx.run("MATCH (n:Meta) RETURN n").single().get(0).asNode());
//
//            LocalDateTime metaNodeUpdateTime = (LocalDateTime) metaNode.asMap().get("lastDatabaseEdit");
//            Assert.assertEquals(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS), metaNodeUpdateTime.truncatedTo(ChronoUnit.HOURS));
//            Assert.assertEquals(synapseCountPerRoi, metaNode.asMap().get("roiInfo"));
//            Assert.assertEquals(preCount, metaNode.asMap().get("totalPreCount"));
//            Assert.assertEquals(postCount, metaNode.asMap().get("totalPostCount"));
//
//        }
//    }
//}
