package org.janelia.flyem.neuprintprocedures;

import apoc.convert.Json;
import apoc.create.Create;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrinterMain;
import org.janelia.flyem.neuprint.SynapseMapper;
import org.janelia.flyem.neuprint.model.BodyWithSynapses;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;
import org.janelia.flyem.neuprintprocedures.functions.NeuPrintUserFunctions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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

public class GraphTraversalToolsTest {

    @ClassRule
    public static Neo4jRule neo4j;
    private static Driver driver;

    static {
        neo4j = new Neo4jRule()
                .withFunction(Json.class)
                .withProcedure(Create.class)
                .withProcedure(LoadingProcedures.class)
                .withFunction(NeuPrintUserFunctions.class);
    }

    @BeforeClass
    public static void before() {

        File swcFile1 = new File("src/test/resources/8426959.swc");
        File swcFile2 = new File("src/test/resources/831744.swc");
        File[] arrayOfSwcFiles = new File[]{swcFile1, swcFile2};

        List<Skeleton> skeletonList = NeuPrinterMain.createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

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
        neo4jImporter.addConnectionSets(dataset, bodyList, mapper.getSynapseLocationToBodyIdMap(), .2F, .8F, true);
        neo4jImporter.addSynapseSets(dataset, bodyList);
        neo4jImporter.addSkeletonNodes(dataset, skeletonList);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F, .20F, .80F, true);
        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 1);

    }

    @AfterClass
    public static void after() {
        driver.close();
    }

    @Test
    public void shouldReturnSynapseRois() {

        Session session = driver.session();

        List<Object> synapseRois = session.readTransaction(tx -> tx.run("WITH neuprint.getSynapseRois(4292, 2261, 1542,'test') AS roiList RETURN roiList")).single().get(0).asList();

        Assert.assertEquals(3, synapseRois.size());
        Assert.assertTrue(synapseRois.contains("roiA"));
        Assert.assertTrue(synapseRois.contains("roiB"));
        Assert.assertTrue(synapseRois.contains("roi'C"));

    }

    @Test
    public void shouldReturnSegmentRois() {

        Session session = driver.session();

        List<Object> segmentRois = session.readTransaction(tx -> tx.run("WITH neuprint.getSegmentRois(831744,'test') AS roiList RETURN roiList")).single().get(0).asList();

        Assert.assertEquals(3, segmentRois.size());
        Assert.assertTrue(segmentRois.contains("roiA"));
        Assert.assertTrue(segmentRois.contains("roiB"));
        Assert.assertTrue(segmentRois.contains("roi'C"));

        List<Object> segment2Rois = session.readTransaction(tx -> tx.run("WITH neuprint.getSegmentRois(100569,'test') AS roiList RETURN roiList")).single().get(0).asList();

        Assert.assertEquals(0, segment2Rois.size());

    }

}




