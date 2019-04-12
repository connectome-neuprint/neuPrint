package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrintMain;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;
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
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class DeleteSkeletonTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ProofreaderProcedures.class)
            .withFunction(NeuPrintUserFunctions.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(LoadingProcedures.class)
            .withProcedure(Create.class);

    @Test
    public void shouldDeleteAllSkeletons() {

        File swcFile1 = new File("src/test/resources/8426959.swc");
        File swcFile2 = new File("src/test/resources/831744.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrintMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

            String neuronsJsonPath = "src/test/resources/neuronList.json";
            List<Neuron> neuronList = NeuPrintMain.readNeuronsJson(neuronsJsonPath);

            String synapseJsonPath = "src/test/resources/synapseList.json";
            List<Synapse> synapseList = NeuPrintMain.readSynapsesJson(synapseJsonPath);

            String connectionsJsonPath = "src/test/resources/connectionsList.json";
            List<SynapticConnection> connectionsList = NeuPrintMain.readConnectionsJson(connectionsJsonPath);

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            String dataset = "test";

            NeuPrintMain.runStandardLoadWithoutMetaInfo(neo4jImporter, dataset, synapseList, connectionsList, neuronList, skeletonList, 1.0F, .2D, .8D, 5,true, true, timeStamp);

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
