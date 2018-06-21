package org.janelia.flyem.neuprinter;

import apoc.create.Create;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;
import com.google.common.base.Stopwatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporterTest {


    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Create.class);
    @Test
    public void testingSkeletonLoadSpeed() {

        File swcFile1 = new File("src/test/resources/101.swc");
        File swcFile2 = new File("src/test/resources/102.swc");
        List<File> listOfSwcFiles = new ArrayList<>();
        listOfSwcFiles.add(swcFile1);
        listOfSwcFiles.add(swcFile2);

        List<Skeleton> skeletonList = new ArrayList<>();

        for (File swcFile : listOfSwcFiles) {
            String filepath = swcFile.getAbsolutePath();
            Long associatedBodyId = ConnConvert.setSkeletonAssociatedBodyId(filepath);
            Skeleton skeleton = new Skeleton();

            try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
                skeleton.fromSwc(reader, associatedBodyId);
                skeletonList.add(skeleton);
                System.out.println("Loaded skeleton associated with bodyId " + associatedBodyId + " and size " + skeleton.getSkelNodeList().size());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            Stopwatch timer = Stopwatch.createStarted();
            neo4jImporter.addSkeletonNodes("test", skeletonList);
            System.out.println(timer.stop());

            Integer skeleton101Degree = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();
            Integer skeleton102Degree = session.run("MATCH (n:Skeleton{skeletonId:\"test:102\"}) WITH n, size((n)-[:Contains]->()) as degree RETURN degree ").single().get(0).asInt();

            Assert.assertEquals(new Integer(50), skeleton101Degree);
            Assert.assertEquals(new Integer(29), skeleton102Degree);

            Integer skelNode101NumberOfRoots = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode) WHERE NOT (s)<-[:LinksTo]-() RETURN count(s) ").single().get(0).asInt();
            Integer skelNode102RootDegree = session.run("MATCH (n:Skeleton{skeletonId:\"test:102\"})-[:Contains]->(s:SkelNode{rowNumber:1}) WITH s, size((s)-[:LinksTo]->()) as degree RETURN degree ").single().get(0).asInt();

            Assert.assertEquals(new Integer(4), skelNode101NumberOfRoots);
            Assert.assertEquals(new Integer(1), skelNode102RootDegree);


            Map<String,Object> skelNodeProperties = session.run("MATCH (n:Skeleton{skeletonId:\"test:101\"})-[:Contains]->(s:SkelNode{rowNumber:13}) RETURN s.location, s.radius, s.skelNodeId, s.x, s.y, s.z").list().get(0).asMap();
            Assert.assertEquals("5096:9281:1624", skelNodeProperties.get("s.location"));
            Assert.assertEquals(28D,skelNodeProperties.get("s.radius"));
            Assert.assertEquals("test:101:5096:9281:1624",skelNodeProperties.get("s.skelNodeId"));
            Assert.assertEquals(5096L,skelNodeProperties.get("s.x"));
            Assert.assertEquals(9281L,skelNodeProperties.get("s.y"));
            Assert.assertEquals(1624L,skelNodeProperties.get("s.z"));



        }
    }



}
