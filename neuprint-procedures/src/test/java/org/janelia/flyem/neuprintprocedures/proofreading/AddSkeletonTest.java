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
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

        String fileCall_831744 = "file:./../neuprint-procedures/src/test/resources/831744.swc";
        String fileCall_101 = "file:./../neuprint-procedures/src/test/resources/101.swc";

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            // multiroot skeleton
            session.writeTransaction(tx -> tx.run("CREATE (n:`test-Segment`{bodyId:101}) SET n:Segment, n:test"));

            session.writeTransaction(tx -> tx.run("CALL proofreader.addSkeleton($fileUrl,$datasetLabel)", parameters("fileUrl", fileCall_101, "datasetLabel", datasetLabel)));

            Node skeleton = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`{skeletonId:\"test:101\"}) RETURN n")).single().get(0).asNode();

            Assert.assertEquals("test:101", skeleton.asMap().get("skeletonId"));
            Assert.assertTrue(skeleton.hasLabel(datasetLabel));
            Assert.assertTrue(skeleton.hasLabel("Skeleton"));
            Assert.assertTrue(skeleton.hasLabel("test-Skeleton"));

            Long skeleton101ContainedByBodyId = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"})<-[:Contains]-(s) RETURN s.bodyId")).single().get(0).asLong();

            Assert.assertEquals(new Long(101), skeleton101ContainedByBodyId);

            Integer skeleton101Degree = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ")).single().get(0).asInt();

            Assert.assertEquals(new Integer(50), skeleton101Degree);

            Integer skelNode101NumberOfRoots = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:`test-SkelNode`:test:SkelNode) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ")).single().get(0).asInt();

            Assert.assertEquals(new Integer(4), skelNode101NumberOfRoots);

            Map<String, Object> skelNodeProperties = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:`test-SkelNode`:test:SkelNode{rowNumber:13}) RETURN s.location, s.radius, s.skelNodeId")).list().get(0).asMap();
            Assert.assertEquals(Values.point(9157, 5096, 9281, 1624).asPoint(), skelNodeProperties.get("s.location"));
            Assert.assertEquals(28D, skelNodeProperties.get("s.radius"));
            Assert.assertEquals("test:101:5096:9281:1624:13", skelNodeProperties.get("s.skelNodeId"));

            // skeleton with locations that round to the same point
            session.writeTransaction(tx -> tx.run("CREATE (n:`test-Segment`{bodyId:831744}) SET n:Segment, n:test"));

            session.writeTransaction(tx -> tx.run("CALL proofreader.addSkeleton($fileUrl,$datasetLabel)", parameters("fileUrl", fileCall_831744, "datasetLabel", datasetLabel)));

            Long skeleton831744ContainedByBodyId = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:831744\"})<-[:Contains]-(s) RETURN s.bodyId")).single().get(0).asLong();

            Assert.assertEquals(new Long(831744), skeleton831744ContainedByBodyId);

            Integer skeleton831744Degree = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:831744\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ")).single().get(0).asInt();

            Assert.assertEquals(new Integer(1679), skeleton831744Degree);

            Integer skelNode831744NumberOfRoots = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:831744\"})-[:Contains]->(s:`test-SkelNode`:test:SkelNode) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ")).single().get(0).asInt();

            Assert.assertEquals(new Integer(1), skelNode831744NumberOfRoots);

            Map<String, Object> skelNode831744Properties = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`:test:Skeleton{skeletonId:\"test:831744\"})-[:Contains]->(s:`test-SkelNode`:test:SkelNode{rowNumber:315}) RETURN s.location, s.radius, s.skelNodeId")).list().get(0).asMap();
            Assert.assertEquals(Values.point(9157, 12238, 16085, 26505).asPoint(), skelNode831744Properties.get("s.location"));
            Assert.assertEquals(49.141D, (double) skelNode831744Properties.get("s.radius"), 0.001D);
            Assert.assertEquals("test:831744:12238:16085:26505:315", skelNode831744Properties.get("s.skelNodeId"));

            List<Record> skelNode831744Row315LinksTo = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:831744\"})-[:Contains]->(s:SkelNode:`test-SkelNode`{rowNumber:315})-[:LinksTo]->(l) RETURN l").list();

            Set<Long> linkedToRowNumbers = skelNode831744Row315LinksTo.stream()
                    .map(r -> (Node) r.asMap().get("l"))
                    .map(node -> (long) node.asMap().get("rowNumber"))
                    .collect(Collectors.toSet());
            Set<Long> expectedRowNumbers = new HashSet<>();
            expectedRowNumbers.add(316L);
            expectedRowNumbers.add(380L);

            Assert.assertEquals(expectedRowNumbers, linkedToRowNumbers);

            List<Record> skelNode831744Row315LinksFrom = session.run("MATCH (n:Skeleton:`test-Skeleton`{skeletonId:\"test:831744\"})-[:Contains]->(s:SkelNode:`test-SkelNode`{rowNumber:315})<-[:LinksTo]-(l) RETURN l").list();

            Set<Long> linkedFromRowNumbers = skelNode831744Row315LinksFrom.stream()
                    .map(r -> (Node) r.asMap().get("l"))
                    .map(n -> (long) n.asMap().get("rowNumber"))
                    .collect(Collectors.toSet());

            Assert.assertTrue(linkedFromRowNumbers.size() == 1 && linkedFromRowNumbers.contains(314L));

            // check that there are no self-loops
            int loopCount = session.readTransaction(tx -> tx.run("MATCH (n:SkelNode)-[:LinksTo]-(n) RETURN count(n)")).single().get(0).asInt();
            Assert.assertEquals(0, loopCount);

        }

    }
}
