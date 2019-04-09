package org.janelia.flyem.neuprintprocedures.analysis;

import apoc.convert.Json;
import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrintMain;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter;
import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;
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
import java.util.Map;

public class InputAndOutputCountsForRoisTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(AnalysisProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(LoadingProcedures.class)
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void shouldGetCountsForRois() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

            File swcFile1 = new File("src/test/resources/8426959.swc");
            File swcFile2 = new File("src/test/resources/831744.swc");
            File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

            List<Skeleton> skeletonList = NeuPrintMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

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

            String jsonData = session.readTransaction(tx -> tx.run("CALL analysis.getInputAndOutputCountsForRois(8426959,\"test\") YIELD value AS dataJson RETURN dataJson").single().get(0).asString());

            Gson gson = new Gson();

            Map<String, SynapseCounter> synapseCounterMap = gson.fromJson(jsonData, new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());

            //is 4 due to overlapping rois which should not occur in real data
            Assert.assertEquals(2L, synapseCounterMap.get("total").getPre());
            Assert.assertEquals(1L, synapseCounterMap.get("roiB").getPost());

            String featureVectorJson = session.readTransaction(tx -> tx.run("CALL analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi(\"roiA\",0,\"test\",0) YIELD value AS dataJson RETURN dataJson").single().get(0).asString());

            ClusteringFeatureVector[] clusteringFeatureVectors = gson.fromJson(featureVectorJson, ClusteringFeatureVector[].class);

            long[] expectedInputVector = { 0, 3, 1};
            Assert.assertEquals(expectedInputVector[0], clusteringFeatureVectors[1].getInputFeatureVector()[0]);
            Assert.assertEquals(expectedInputVector[1], clusteringFeatureVectors[1].getInputFeatureVector()[1]);
            Assert.assertEquals(expectedInputVector[2], clusteringFeatureVectors[1].getInputFeatureVector()[2]);

        }
    }
}
