//package org.janelia.flyem.neuprint;
//
//import apoc.convert.Json;
//import apoc.create.Create;
//import com.google.common.base.Stopwatch;
//import org.janelia.flyem.neuprint.model.BodyWithSynapses;
//import org.janelia.flyem.neuprint.model.Neuron;
//import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;
//import org.junit.BeforeClass;
//import org.junit.ClassRule;
//import org.junit.Test;
//import org.neo4j.driver.v1.Config;
//import org.neo4j.driver.v1.Driver;
//import org.neo4j.driver.v1.GraphDatabase;
//import org.neo4j.harness.junit.Neo4jRule;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Set;
//
//public class SpeedTest {
//    @ClassRule
//    public static Neo4jRule neo4j;
//    private static Driver driver;
//    private static Stopwatch newTime;
//    private static Stopwatch oldTime;
//
//    static {
//        neo4j = new Neo4jRule()
//                .withFunction(Json.class)
//                .withProcedure(LoadingProcedures.class)
//                .withProcedure(Create.class);
//    }
//
//    @BeforeClass
//    public static void before() {
//
////        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2, swcFile3};
//
////        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);
//
////
////        String bodiesJsonPath = "/Users/neubarthn/Documents/GitHub/neuPrint/mb6_neo4j_inputs/mb6_Synapses.json";
////
////        SynapseMapper mapper = new SynapseMapper();
////        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);
////        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//
////        neo4jImporter.addConnectsTo(dataset, bodyList);
////        neo4jImporter.addSynapsesWithRois(dataset, bodyList);
////        neo4jImporter.addSynapsesTo(dataset, preToPost);
////        neo4jImporter.addSegmentRois(dataset, bodyList);
////        neo4jImporter.addConnectionSets(dataset, bodyList, mapper.getSynapseLocationToBodyIdMap(), .2F, .8F);
////        neo4jImporter.addSynapseSets(dataset, bodyList);
//////        neo4jImporter.addSkeletonNodes(dataset, skeletonList);
////        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F, .20F, .80F);
////        neo4jImporter.addAutoNames(dataset, 5);
////        neo4jImporter.addDvidUuid(dataset, "1234");
////        neo4jImporter.addDvidServer(dataset, "test1:23");
////        neo4jImporter.addClusterNames(dataset, .1F);
////        neo4jImporter.setSuperLevelRois(dataset, bodyList);
//
//    }
//
//    @Test
//    public void runNewLoad() {
//
//        String neuronsJsonPath = "/Users/neubarthn/Documents/GitHub/neuPrint/mb6_neo4j_inputs/mb6_Neurons_with_nt.json";
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);
//
//        String bodiesJsonPath = "/Users/neubarthn/Documents/GitHub/neuPrint/mb6_neo4j_inputs/mb6_Synapses.json";
//
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//
//        String dataset = "mb6";
//
//        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());
//
//        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
//
//        neo4jImporter.prepDatabase(dataset);
//
//        Stopwatch stopwatch = Stopwatch.createStarted();
//        neo4jImporter.addSegments(dataset, neuronList);
//        neo4jImporter.addConnectsTo(dataset, bodyList);
//        newTime = stopwatch.stop();
//
//        System.out.println("New: " + newTime);
//
//    }
//
//    @Test
//    public void runOldLoad() {
//
//        String neuronsJsonPath = "/Users/neubarthn/Documents/GitHub/neuPrint/mb6_neo4j_inputs/mb6_Neurons_with_nt.json";
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson(neuronsJsonPath);
//
//        String dataset = "mb6";
//
//        String bodiesJsonPath = "/Users/neubarthn/Documents/GitHub/neuPrint/mb6_neo4j_inputs/mb6_Synapses.json";
//
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies(bodiesJsonPath);
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//
//        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());
//
//        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
//
//        neo4jImporter.prepDatabase(dataset);
//
//        Stopwatch stopwatch = Stopwatch.createStarted();
//        neo4jImporter.addSegmentsOld(dataset, neuronList);
//        neo4jImporter.addConnectsToOld(dataset, bodyList);
//        oldTime = stopwatch.stop();
//
//        System.out.println("Old: " + oldTime);
//
//    }
//
//}
