package org.janelia.flyem.neuprinter;

import apoc.create.Create;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;
import com.google.common.base.Stopwatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporterTest {


    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Create.class);
    @Test
    public void testingSkeletonLoadSpeed() {

        File swcFile1 = new File("src/test/resources/101.swc");
        //File swcFile2 = new File("src/test/resources/102.swc");
        List<File> listOfSwcFiles = new ArrayList<>();
        listOfSwcFiles.add(swcFile1);
        //listOfSwcFiles.add(swcFile2);

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


        }
    }



}
