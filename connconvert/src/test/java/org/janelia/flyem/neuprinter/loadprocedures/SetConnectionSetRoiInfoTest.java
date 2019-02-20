package org.janelia.flyem.neuprinter.loadprocedures;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounterWithHighPrecisionCounts;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;
import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

public class SetConnectionSetRoiInfoTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(Create.class)
                .withProcedure(LoadingProcedures.class);
    }

    @BeforeClass
    public static void before() {
        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();

        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());

        String dataset = "test";

        Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
        neo4jImporter.prepDatabase(dataset);

        neo4jImporter.addSegments(dataset, neuronList);

        neo4jImporter.addConnectsTo(dataset, bodyList);
        neo4jImporter.addSynapsesWithRois(dataset, bodyList);

        neo4jImporter.addSynapsesTo(dataset, preToPost);
        neo4jImporter.addSegmentRois(dataset, bodyList);
        neo4jImporter.addConnectionSets(dataset, bodyList, mapper.getSynapseLocationToBodyIdMap(), .2F, .8F);
        neo4jImporter.addSynapseSets(dataset, bodyList);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F, .20F, .80F);
        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 1);

    }

    @Test
    public void shouldAddRoiInfoToConnectionSet() {

        Session session = driver.session();

        session.writeTransaction(tx -> tx.run("CALL loader.setConnectionSetRoiInfo($preBodyId, $postBodyId, $dataset, $preHPThreshold, $postHPThreshold)", parameters("preBodyId", 8426959, "postBodyId", 26311, "dataset", "test", "preHPThreshold", .2, "postHPThreshold", .8)));

        String roiInfoString = session.readTransaction(tx -> tx.run("MATCH (n:`test-ConnectionSet`{datasetBodyIds:$datasetBodyIds}) RETURN n.roiInfo", parameters("datasetBodyIds", "test:8426959:26311"))).single().get("n.roiInfo").asString();

        Assert.assertNotNull(roiInfoString);

        Gson gson = new Gson();
        Map<String, SynapseCounterWithHighPrecisionCounts> roiInfo = gson.fromJson(roiInfoString, new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
        }.getType());

        Assert.assertEquals(1, roiInfo.size());

        Assert.assertEquals(roiInfo.get("roiA").getPre(), 1);
        Assert.assertEquals(roiInfo.get("roiA").getPreHP(), 0);
        Assert.assertEquals(roiInfo.get("roiA").getPost(), 1);
        Assert.assertEquals(roiInfo.get("roiA").getPostHP(), 1);

    }
}
