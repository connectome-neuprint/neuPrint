//package org.janelia.flyem.neuprintprocedures;
//
//import apoc.create.Create;
//import apoc.refactor.GraphRefactoring;
//import org.janelia.flyem.neuprinter.NeuPrinterMain;
//import org.janelia.flyem.neuprinter.Neo4jImporter;
//import org.janelia.flyem.neuprinter.SynapseMapper;
//import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
//import org.janelia.flyem.neuprinter.model.Neuron;
//import org.janelia.flyem.neuprinter.model.Skeleton;
//import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
//import org.junit.Assert;
//import org.junit.Rule;
//import org.junit.Test;
//import org.neo4j.driver.v1.Config;
//import org.neo4j.driver.v1.Driver;
//import org.neo4j.driver.v1.GraphDatabase;
//import org.neo4j.driver.v1.Session;
//import org.neo4j.harness.junit.Neo4jRule;
//
//import java.io.File;
//import java.util.HashMap;
//import java.util.List;
//
//import static org.neo4j.driver.v1.Values.parameters;
//
//public class DeleteNeuronTest {
//
//    @Rule
//    public Neo4jRule neo4j = new Neo4jRule()
//            .withProcedure(ProofreaderProcedures.class)
//            .withProcedure(GraphRefactoring.class)
//            .withProcedure(Create.class);
//
//    //TODO: if keeping this update to deal with history nodes
//    @Test
//    public void shouldDeleteNeuronAndAllAssociatedNodes() {
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
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
//            neo4jImporter.addNeurons(dataset, neuronList);
//
//            neo4jImporter.addConnectsTo(dataset, bodyList);
//            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
//            neo4jImporter.addSynapsesTo(dataset, preToPost);
//            neo4jImporter.addNeuronRois(dataset, bodyList);
//            neo4jImporter.addSynapseSets(dataset, bodyList);
//
//            neo4jImporter.addSkeletonNodes(dataset, skeletonList);
//
//            session.run("CALL proofreader.deleteNeuron($bodyId,$dataset)", parameters("bodyId", 8426959, "dataset", "test"));
//
//            int deletedNeuronNodeCount = session.run("MATCH (n:Neuron{bodyId:8426959}) RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(0, deletedNeuronNodeCount);
//
//            int deletedSSNodeCount = session.run("MATCH (n:SynapseSet{datasetBodyId:\"test:8426959\"}) RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(0, deletedSSNodeCount);
//
//            int deletedSynapse1NodeCount = session.run("MATCH (n:Synapse{datasetLocation:\"test:4287:2277:1542\"}) RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(0, deletedSynapse1NodeCount);
//
//            int deletedSynapse2NodeCount = session.run("MATCH (n:Synapse{datasetLocation:\"test:4222:2402:1688\"}) RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(0, deletedSynapse2NodeCount);
//
//            int deletedSynapse3NodeCount = session.run("MATCH (n:Synapse{datasetLocation:\"test:4287:2277:1502\"}) RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(0, deletedSynapse3NodeCount);
//
//            int deletedSkelNodeCount = session.run("MATCH (n:SkelNode) WHERE n.skelNodeId STARTS WITH \"test:8426959\" RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(0, deletedSkelNodeCount);
//
//            int deletedSkeletonCount = session.run("MATCH (n:Skeleton{skeletonId:\"test:8426959\"}) RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(0, deletedSkeletonCount);
//
//
//        }
//    }
//}
