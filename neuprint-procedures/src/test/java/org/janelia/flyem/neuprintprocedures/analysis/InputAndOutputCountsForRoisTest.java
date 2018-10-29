package org.janelia.flyem.neuprintprocedures.analysis;

import apoc.convert.Json;
import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InputAndOutputCountsForRoisTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(AnalysisProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void shouldGetCountsForRois() {

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addSegments(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addSegmentRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
            neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);

            String jsonData = session.readTransaction(tx -> tx.run("CALL analysis.getInputAndOutputCountsForRois(8426959,\"test\") YIELD value AS dataJson RETURN dataJson").single().get(0).asString());

            Gson gson = new Gson();

            Map<String, SynapseCounter> synapseCounterMap = gson.fromJson(jsonData, new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());

            //is 4 due to overlapping rois which should not occur in real data
            Assert.assertEquals(2L, synapseCounterMap.get("total").getPre());
            Assert.assertEquals(1L, synapseCounterMap.get("roiB").getPost());

            String featureVectorJson = session.readTransaction(tx -> tx.run("CALL analysis.getInputAndOutputFeatureVectorsForNeuronsInRoi(\"roiA\",0,\"test\",0) YIELD value AS dataJson RETURN dataJson").single().get(0).asString());

            ClusteringFeatureVector[] clusteringFeatureVectors = gson.fromJson(featureVectorJson, ClusteringFeatureVector[].class);

            long[] expectedInputVector = {0, 3, 1};
            Assert.assertEquals(expectedInputVector[0], clusteringFeatureVectors[2].getInputFeatureVector()[0]);
            Assert.assertEquals(expectedInputVector[1], clusteringFeatureVectors[2].getInputFeatureVector()[1]);
            Assert.assertEquals(expectedInputVector[2], clusteringFeatureVectors[2].getInputFeatureVector()[2]);
        }
    }
}
