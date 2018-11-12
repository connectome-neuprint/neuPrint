package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprintprocedures.functions.NeuPrintUserFunctions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DeleteSkeletonTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ProofreaderProcedures.class)
            .withFunction(NeuPrintUserFunctions.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(Create.class);

    @Test
    public void shouldDeleteAllSkeletons() {

        File swcFile1 = new File("src/test/resources/8426959.swc");
        File swcFile2 = new File("src/test/resources/831744.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addSegments(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);

            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addSkeletonNodes(dataset, skeletonList);
            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
            neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 1);

            Session session = driver.session();

            int skeletonCountBefore = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`) RETURN count(n)")).single().get(0).asInt();

            Assert.assertEquals(2, skeletonCountBefore);

            //delete skeletons
            session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSkeleton(8426959,\"test\")"));
            session.writeTransaction(tx -> tx.run("CALL proofreader.deleteSkeleton(831744,\"test\")"));

            int skeletonCountAfter = session.readTransaction(tx -> tx.run("MATCH (n:`test-Skeleton`) RETURN count(n)")).single().get(0).asInt();
            int skelNodeCountAfter = session.readTransaction(tx -> tx.run("MATCH (n:`test-SkelNode`) RETURN count(n)")).single().get(0).asInt();

            Assert.assertEquals(0, skeletonCountAfter);
            Assert.assertEquals(0, skelNodeCountAfter);

        }

    }
}
