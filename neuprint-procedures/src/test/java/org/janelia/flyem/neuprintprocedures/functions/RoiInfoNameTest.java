package org.janelia.flyem.neuprintprocedures.functions;

import apoc.convert.Json;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

public class RoiInfoNameTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withFunction(NeuPrintUserFunctions.class);
    }

    @BeforeClass
    public static void before() {

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
        neo4jImporter.addSynapseSets(dataset, bodyList);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);
        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);
        neo4jImporter.addClusterNames("test", .1F);
    }

    @Test
    public void shouldProduceCorrectRoiBasedName() {
        Session session = driver.session();

        List<String> superLevelRois = new ArrayList<>();
        superLevelRois.add("roiA");

        String name5percent = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`{bodyId:8426959}) WITH neuprint.roiInfoAsName(n.roiInfo,n.pre,n.post,.05,$superLevelRois) AS name RETURN name ", parameters("superLevelRois", superLevelRois))).single().get(0).asString();

        Assert.assertEquals("roiA-roiA", name5percent);

        String name100percent = session.readTransaction(tx -> tx.run("MATCH (n:`test-Neuron`{bodyId:8426959}) WITH neuprint.roiInfoAsName(n.roiInfo,n.pre,n.post,1.0,$superLevelRois) AS name RETURN name ", parameters("superLevelRois", superLevelRois))).single().get(0).asString();

        Assert.assertEquals("none-none", name100percent);

    }

    @Test
    public void shouldGetConnectionClusterNames() {
        Session session = driver.session();

        String resultJson = session.readTransaction(tx -> tx.run("WITH neuprint.getClusterNamesOfConnections(8426959, \"test\") AS result RETURN result")).single().get(0).asString();

        Gson gson = new Gson();

        Map<String, SynapseCounter> resultObject = gson.fromJson(resultJson, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        int sumPre = 0;
        int sumPost = 0;

        for (String category : resultObject.keySet()) {
            sumPre += resultObject.get(category).getPre();
            sumPost += resultObject.get(category).getPost();
        }

        Assert.assertEquals(2, sumPre);
        Assert.assertEquals(3, sumPost);

        Assert.assertEquals(1, resultObject.get("roiA-roiA:roiB-none").getPre());
        Assert.assertEquals(2, resultObject.get("roiA-roiA").getPost());

    }

}
