//package org.janelia.flyem.neuprintprocedures.analysis;
//
//import apoc.convert.Json;
//import apoc.create.Create;
//import apoc.refactor.GraphRefactoring;
//import com.google.gson.Gson;
//import org.janelia.flyem.neuprinter.Neo4jImporter;
//import org.janelia.flyem.neuprinter.NeuPrinterMain;
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
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public class GetLineGraphTest {
//
//    @Rule
//    public Neo4jRule neo4j = new Neo4jRule()
//            .withProcedure(AnalysisProcedures.class)
//            .withProcedure(GraphRefactoring.class)
//            .withFunction(Json.class)
//            .withProcedure(Create.class);
//
//    @Test
//    public void shouldProduceLineGraphForRoi() {
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//        bodyList.sort(new SortBodyByNumberOfSynapses());
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
//            Map<String, Object> jsonData = session.writeTransaction(tx -> tx.run("CALL analysis.getLineGraphForRoi(\"roiA\",\"test\",0,1) YIELD value AS dataJson RETURN dataJson").single().get(0).asMap());
//
//            String nodes = (String) jsonData.get("Vertices");
//            String edges = (String) jsonData.get("Edges");
//
//            Gson gson = new Gson();
//
//            SynapticConnectionVertex[] nodeArray = gson.fromJson(nodes, SynapticConnectionVertex[].class);
//            List<SynapticConnectionVertex> nodeList = Arrays.asList(nodeArray);
//
//            Assert.assertEquals(4, nodeList.size());
//            Assert.assertEquals("8426959_to_26311", nodeList.get(2).getConnectionDescription());
//            Assert.assertEquals(new Integer(2), nodeList.get(2).getPre());
//            Assert.assertEquals(new Integer(2), nodeList.get(2).getPost());
//            Long[] centroidLocation2 = nodeList.get(2).getCentroidLocation();
//            Assert.assertEquals(new Location(4219L, 2458L, 1520L), new Location(centroidLocation2[0], centroidLocation2[1], centroidLocation2[2]));
//
//            Assert.assertEquals("8426959_to_2589725", nodeList.get(3).getConnectionDescription());
//            Assert.assertEquals(new Integer(2), nodeList.get(3).getPre());
//            Assert.assertEquals(new Integer(1), nodeList.get(3).getPost());
//            Long[] centroidLocation3 = nodeList.get(3).getCentroidLocation();
//            Assert.assertEquals(new Location(4291L, 2283L, 1529L), new Location(centroidLocation3[0], centroidLocation3[1], centroidLocation3[2]));
//
//            SynapticConnectionEdge[] edgeArray = gson.fromJson(edges, SynapticConnectionEdge[].class);
//            List<SynapticConnectionEdge> edgeList = Arrays.asList(edgeArray);
//
//            Assert.assertEquals(6, edgeList.size());
//            Assert.assertEquals("8426959_to_2589725", edgeList.get(1).getSourceName());
//            Assert.assertEquals("8426959_to_26311", edgeList.get(1).getTargetName());
//            Assert.assertEquals(new Long(189), edgeList.get(1).getDistance());
//
//        }
//
//    }
//
//    @Test
//    public void shouldProduceLineGraphForNeuron() {
//
//        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
//        SynapseMapper mapper = new SynapseMapper();
//        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
//        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
//        bodyList.sort(new SortBodyByNumberOfSynapses());
//
//        File swcFile1 = new File("src/test/resources/8426959.swc");
//
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
//            neo4jImporter.addSkeletonNodes("test", skeletonList);
//
//            Map<String, Object> jsonData = session.writeTransaction(tx -> tx.run("CALL analysis.getLineGraphForNeuron(8426959,\"test\",0) YIELD value AS dataJson RETURN dataJson").single().get(0).asMap());
//
//            String nodes = (String) jsonData.get("Vertices");
//            String edges = (String) jsonData.get("Edges");
//
//            Gson gson = new Gson();
//
//            SynapticConnectionVertex[] nodeArray = gson.fromJson(nodes, SynapticConnectionVertex[].class);
//            List<SynapticConnectionVertex> nodeList = Arrays.asList(nodeArray);
//
//            Assert.assertEquals(4, nodeList.size());
//            Assert.assertEquals("8426959_to_26311", nodeList.get(2).getConnectionDescription());
//            Assert.assertEquals(new Integer(2), nodeList.get(2).getPre());
//            Assert.assertEquals(new Integer(2), nodeList.get(2).getPost());
//            Long[] centroidLocation2 = nodeList.get(2).getCentroidLocation();
//            Assert.assertEquals(new Location(4219L, 2458L, 1520L), new Location(centroidLocation2[0], centroidLocation2[1], centroidLocation2[2]));
//
//            Assert.assertEquals("8426959_to_2589725", nodeList.get(3).getConnectionDescription());
//            Assert.assertEquals(new Integer(2), nodeList.get(3).getPre());
//            Assert.assertEquals(new Integer(1), nodeList.get(3).getPost());
//            Long[] centroidLocation3 = nodeList.get(3).getCentroidLocation();
//            Assert.assertEquals(new Location(4291L, 2283L, 1529L), new Location(centroidLocation3[0], centroidLocation3[1], centroidLocation3[2]));
//
//            SynapticConnectionEdge[] edgeArray = gson.fromJson(edges, SynapticConnectionEdge[].class);
//            List<SynapticConnectionEdge> edgeList = Arrays.asList(edgeArray);
//
//            Assert.assertEquals(6, edgeList.size());
//            Assert.assertEquals("8426959_to_2589725", edgeList.get(1).getSourceName());
//            Assert.assertEquals("8426959_to_26311", edgeList.get(1).getTargetName());
//            Assert.assertEquals(new Long(189), edgeList.get(1).getDistance());
//
//            Map<String, Object> jsonDataWithCableLength = session.writeTransaction(tx -> tx.run("CALL analysis.getLineGraphForNeuron(8426959,\"test\",0,true) YIELD value AS dataJson RETURN dataJson").single().get(0).asMap());
//
//            String nodesFromCableLength = (String) jsonDataWithCableLength.get("Vertices");
//            String edgesFromCableLength = (String) jsonDataWithCableLength.get("Edges");
//
//            SynapticConnectionVertex[] nodeArrayFromCableLength = gson.fromJson(nodesFromCableLength, SynapticConnectionVertex[].class);
//            List<SynapticConnectionVertex> nodeListFromCableLength = Arrays.asList(nodeArrayFromCableLength);
//            SynapticConnectionEdge[] edgeArrayFromCableLength = gson.fromJson(edgesFromCableLength, SynapticConnectionEdge[].class);
//            List<SynapticConnectionEdge> edgeListFromCableLength = Arrays.asList(edgeArrayFromCableLength);
//
//            Assert.assertEquals(new Long(212), edgeListFromCableLength.get(1).getDistance());
//
//            Map<String, Object> jsonCentroidAndSkeleton = session.writeTransaction(tx -> tx.run("CALL analysis.getConnectionCentroidsAndSkeleton(8426959,\"test\",0) YIELD value AS dataJson RETURN dataJson").single().get(0).asMap());
//
//            String centroids = (String) jsonCentroidAndSkeleton.get("Centroids");
//            String skeleton = (String) jsonCentroidAndSkeleton.get("Skeleton");
//
//            Assert.assertEquals(nodesFromCableLength, centroids);
//            // TODO: write test for this procedure
//
//        }
//
//    }
//
//}