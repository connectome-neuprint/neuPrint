package org.janelia.flyem.neuprintprocedures;

import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class AddSkeletonTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ProofreaderProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(Create.class);

//TODO: fix this to conform to new data model
//    @Test
//    public void shouldAddSkeletonToNeuron() {
//
//
//        String datasetLabel = "test";
//
//        String fileCall = "file:///Users/neubarthn/Documents/GitHub/neuPrint/neuprint-procedures/src/test/resources/101.swc";
//
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
//
//            Session session = driver.session();
//
//            session.run("CREATE (n:Neuron:test{bodyId:101})");
//
//            Node skeleton = session.run("CALL proofreader.addSkeleton($fileUrl,$datasetLabel) YIELD node RETURN node",parameters("fileUrl",fileCall,"datasetLabel","test")).single().get(0).asNode();
//
//            Assert.assertEquals("test:101",skeleton.asMap().get("skeletonId"));
//            Assert.assertTrue(skeleton.hasLabel("test"));
//            Assert.assertTrue(skeleton.hasLabel("Skeleton"));
//
//            Long skeleton101ContainedByBodyId = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();
//
//            Assert.assertEquals(new Long(101), skeleton101ContainedByBodyId);
//
//            Integer skeleton101Degree = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();
//
//            Assert.assertEquals(new Integer(50), skeleton101Degree);
//
//            Integer skelNode101NumberOfRoots = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ").single().get(0).asInt();
//
//            Assert.assertEquals(new Integer(4), skelNode101NumberOfRoots);
//
//            Map<String, Object> skelNodeProperties = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode{rowNumber:13}) RETURN s.location, s.radius, s.skelNodeId, s.x, s.y, s.z").list().get(0).asMap();
//            Assert.assertEquals("5096:9281:1624", skelNodeProperties.get("s.location"));
//            Assert.assertEquals(28D, skelNodeProperties.get("s.radius"));
//            Assert.assertEquals("test:101:5096:9281:1624", skelNodeProperties.get("s.skelNodeId"));
//            Assert.assertEquals(5096L, skelNodeProperties.get("s.x"));
//            Assert.assertEquals(9281L, skelNodeProperties.get("s.y"));
//            Assert.assertEquals(1624L, skelNodeProperties.get("s.z"));
//
//
//        }
//
//
//
//
//        }
}
