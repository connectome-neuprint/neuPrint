//package org.janelia.flyem.neuprinter;
//
//import apoc.convert.Json;
//import apoc.create.Create;
//import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
//import org.janelia.flyem.neuprinter.model.Neuron;
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
//import java.util.HashMap;
//import java.util.List;
//
//public class Neo4jEditorTest {
//
//    @Rule
//    public Neo4jRule neo4j = new Neo4jRule()
//            .withFunction(Json.class)
//            .withProcedure(Create.class);
//
//    @Test
//    public void shouldUpdateNeuronProperties() {
//
//        String neuronsJsonPath = "src/test/resources/smallNeuronList.json";
//        String bodiesJsonPath = "src/test/resources/smallBodyListWithExtraRois.json";
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);
//
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);
//
//        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
//
//        bodyList.sort(new SortBodyByNumberOfSynapses());
//
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
//
//            Session session = driver.session();
//
//            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
//
//            neo4jImporter.prepDatabase("test");
//
//            neo4jImporter.addSegments("test", neuronList);
//
//            neo4jImporter.addConnectsTo("test", bodyList);
//
//            neo4jImporter.addSynapsesWithRois("test", bodyList);
//
//            neo4jImporter.addSynapsesTo("test", preToPost);
//
//            neo4jImporter.addSegmentRois("test", bodyList);
//
//            Neo4jEditor neo4jEditor = new Neo4jEditor(driver);
//
//            String neuronsJsonPath2 = "src/test/resources/smallNeuronList2.json";
//            List<Neuron> neuronList2 = NeuPrinterMain.readNeuronsJson(neuronsJsonPath2);
//
//            neo4jEditor.updateNeuronProperties("test", neuronList2);
//
//            Integer numberNeuronsChanged = session.run("MATCH (n:`test-Neuron`) WHERE n.status='changed' AND n.size=1 RETURN count(n)").single().get(0).asInt();
//
//            Assert.assertEquals(new Integer(6),numberNeuronsChanged);
//
//        }
//    }
//
//}
