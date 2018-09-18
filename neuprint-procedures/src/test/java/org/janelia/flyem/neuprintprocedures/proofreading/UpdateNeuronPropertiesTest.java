//package org.janelia.flyem.neuprintprocedures;
//
//import org.janelia.flyem.neuprinter.Neo4jImporter;
//import org.janelia.flyem.neuprinter.NeuPrinterMain;
//import org.janelia.flyem.neuprinter.SynapseMapper;
//import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
//import org.janelia.flyem.neuprinter.model.Neuron;
//import org.janelia.flyem.neuprinter.model.Skeleton;
//import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
//import org.junit.Rule;
//import org.junit.Test;
//import org.neo4j.driver.v1.Config;
//import org.neo4j.driver.v1.Driver;
//import org.neo4j.driver.v1.GraphDatabase;
//import org.neo4j.driver.v1.Session;
//import org.neo4j.driver.v1.types.Node;
//import org.neo4j.harness.junit.Neo4jRule;
//
//import java.io.File;
//import java.util.HashMap;
//import java.util.List;
//
//import static org.neo4j.driver.v1.Values.parameters;
//
//public class UpdateNeuronPropertiesTest {
//
//    @Rule
//    public Neo4jRule neo4j = new Neo4jRule()
//            .withProcedure(ProofreaderProcedures.class);
////            .withFunction(NeuPrintUserFunctions.class)
////            .withProcedure(GraphRefactoring.class)
////            .withFunction(Json.class)
////            .withProcedure(Create.class);
//
//    //TODO: write tests for exception/error handling.
//
//    @Test
//    public void shouldUpdateNeuronProperties() {
//
//        String updateInstructionJson = "{\"DVIDuuid\": \"7254f5a8aacf4e6f804dcbddfdac4f7f\", \"MutationID\": 68387, " +
//                "\"Action\": \"update\", \"BodyId\": 8426959, " +
//                "\"NewBodySize\": 2778831, " +
//                "}";
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
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
//            Node neuron = session.writeTransaction(tx ->
//                    tx.run("CALL proofreader.updateNeuronProperties($updateJson,\"test\") YIELD node RETURN node", parameters("updateJson", updateInstructionJson)).single().get(0).asNode());
//
//        }
//
//    }
//}
