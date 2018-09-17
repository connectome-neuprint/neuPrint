package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import org.janelia.flyem.neuprintprocedures.functions.NeuPrintUserFunctions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class AddSkeletonTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ProofreaderProcedures.class)
            .withFunction(NeuPrintUserFunctions.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(Create.class);

    @Test
    public void shouldAddSkeletonToNeuron() {

        String datasetLabel = "test";

        String fileCall = "file:./../neuprint-procedures/src/test/resources/101.swc";

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            session.run("CREATE (n:`test-Segment`{bodyId:101}) SET n:Segment, n:test");

            Node skeleton = session.run("CALL proofreader.addSkeleton($fileUrl,$datasetLabel) YIELD node RETURN node", parameters("fileUrl", fileCall, "datasetLabel", datasetLabel)).single().get(0).asNode();

            Assert.assertEquals("test:101", skeleton.asMap().get("skeletonId"));
            Assert.assertTrue(skeleton.hasLabel(datasetLabel));
            Assert.assertTrue(skeleton.hasLabel("Skeleton"));
            Assert.assertTrue(skeleton.hasLabel("test-Skeleton"));

            Long skeleton101ContainedByBodyId = session.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"})<-[:Contains]-(s) RETURN s.bodyId").single().get(0).asLong();

            Assert.assertEquals(new Long(101), skeleton101ContainedByBodyId);

            Integer skeleton101Degree = session.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();

            Assert.assertEquals(new Integer(50), skeleton101Degree);

            Integer skelNode101NumberOfRoots = session.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:`test-SkelNode`:test:SkelNode) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ").single().get(0).asInt();

            Assert.assertEquals(new Integer(4), skelNode101NumberOfRoots);

            Map<String, Object> skelNodeProperties = session.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:`test-SkelNode`:test:SkelNode{rowNumber:13}) RETURN s.location, s.radius, s.skelNodeId").list().get(0).asMap();
            Assert.assertEquals(Values.point(9157, 5096, 9281, 1624).asPoint(), skelNodeProperties.get("s.location"));
            Assert.assertEquals(28D, skelNodeProperties.get("s.radius"));
            Assert.assertEquals("test:101:5096:9281:1624", skelNodeProperties.get("s.skelNodeId"));

        }

    }
}
