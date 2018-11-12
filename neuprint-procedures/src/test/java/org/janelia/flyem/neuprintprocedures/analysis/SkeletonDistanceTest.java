//package org.janelia.flyem.neuprintprocedures.analysis;
//
//import apoc.create.Create;
//import apoc.refactor.GraphRefactoring;
//import org.janelia.flyem.neuprinter.Neo4jImporter;
//import org.janelia.flyem.neuprinter.NeuPrinterMain;
//import org.janelia.flyem.neuprinter.model.Skeleton;
//import org.junit.Assert;
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
//import java.util.List;
//
//public class SkeletonDistanceTest {
//
//    @Rule
//    public Neo4jRule neo4j = new Neo4jRule()
//            .withProcedure(AnalysisProcedures.class)
//            .withProcedure(GraphRefactoring.class)
//            .withProcedure(Create.class);
//
//    @Test
//    public void shouldCalculateCorrectDistanceAndFindClosestPoint() {
//        File swcFile1 = new File("src/test/resources/101.swc");
//        File swcFile2 = new File("src/test/resources/102.swc");
//
//        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(new File[]{swcFile1, swcFile2});
//
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
//
//            Session session = driver.session();
//
//            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
//
//            neo4jImporter.addSkeletonNodes("test", skeletonList);
//
//            Long distance = session.readTransaction(tx -> tx.run("MATCH (n:SkelNode{skelNodeId:\"test:101:5464:9385:1248:1\"}), (m:SkelNode{skelNodeId:\"test:101:5328:9385:1368:5\"}) WITH n,m CALL analysis.calculateSkeletonDistance(\"test\",n,m) YIELD value RETURN value").single().get(0).asLong());
//
//            Assert.assertEquals(new Long(207), distance);
//
//            Node skelNode = session.readTransaction(tx -> tx.run("CALL analysis.getNearestSkelNodeOnBodyToPoint(101,\"test\",4864,8817,1936) YIELD node RETURN node").single().get(0).asNode());
//
//            Assert.assertEquals("test:101:4864:8817:1936:26", skelNode.asMap().get("skelNodeId"));
//
//            Long distanceFromNearestCalculation = session.readTransaction(tx -> tx.run("CALL analysis.getNearestSkelNodeOnBodyToPoint(101,\"test\",5464,9385,1248) YIELD node AS node1 WITH node1" +
//                    " CALL analysis.getNearestSkelNodeOnBodyToPoint(101,\"test\",5328,9385,1368) YIELD node AS node2 WITH node1,node2" +
//                    " CALL analysis.calculateSkeletonDistance(\"test\",node1,node2) YIELD value RETURN value").single().get(0).asLong());
//
//            Assert.assertEquals(new Long(207), distanceFromNearestCalculation);
//        }
//
//    }
//}
