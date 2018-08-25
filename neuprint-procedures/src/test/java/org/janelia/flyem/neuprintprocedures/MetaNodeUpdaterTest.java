package org.janelia.flyem.neuprintprocedures;

import apoc.convert.Json;
import apoc.create.Create;
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
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaNodeUpdaterTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void shouldUpdateMetaNodeAfterCommits() {

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addNeurons(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F);

            Session session = driver.session();

            Node metaNodeOriginal = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test) RETURN n").single().get(0).asNode());
        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            //should only trigger an update of lastDatabaseEdit
            session.writeTransaction(tx -> {
                tx.run("CREATE (n:Neuron:test:roiA:newRoi:`test-roiA`:`test-newRoi`:`test-Neuron`{bodyId:50}) SET n.synapseCountPerRoi=\"{'roiA':{'pre':5,'post':2,'total':7},'newRoi':{'pre':5,'post':2,'total':7}}\", n.pre=10, n.post=4 RETURN n");
                return 1;
            });

        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
            Session session = driver.session();

            Node metaNode = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode());

            Assert.assertEquals(LocalDate.now(), metaNode.asMap().get("lastDatabaseEdit"));

            Assert.assertEquals(5L, metaNode.asMap().get("totalPostCount"));
            Assert.assertEquals(3L, metaNode.asMap().get("totalPreCount"));

            String metaSynapseCountPerRoi = (String) metaNode.asMap().get("synapseCountPerRoi");
            Gson gson = new Gson();
            Map<String, SynapseCounter> metaSynapseCountPerRoiMap = gson.fromJson(metaSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());

            Assert.assertEquals(2L, metaSynapseCountPerRoiMap.get("roiA").getPre());
            Assert.assertEquals(3L, metaSynapseCountPerRoiMap.get("roiA").getPost());

            Assert.assertEquals(4, metaSynapseCountPerRoiMap.keySet().size());
            Assert.assertTrue(metaSynapseCountPerRoiMap.containsKey("roiA")
                    && metaSynapseCountPerRoiMap.containsKey("roiB")
                    && metaSynapseCountPerRoiMap.containsKey("anotherRoi")
                    && metaSynapseCountPerRoiMap.containsKey("seven_column_roi"));

        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();

            //should trigger complete meta node update
            //note that update is based on synapse count per roi on neurons, not info from synapse nodes directly
            session.writeTransaction(tx -> {
                tx.run("CREATE (n:Synapse:test:`test-Synapse`:`roiA-pt`:`newRoi-pt`:`test-roiA-pt`:`test-newRoi-pt`) RETURN n");
                return 1;
            });

        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
            Session session = driver.session();

            Node metaNode = session.readTransaction(tx -> tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode());

            Assert.assertEquals(LocalDate.now(), metaNode.asMap().get("lastDatabaseEdit"));

            Assert.assertEquals(9L, metaNode.asMap().get("totalPostCount"));
            Assert.assertEquals(13L, metaNode.asMap().get("totalPreCount"));

            String metaSynapseCountPerRoi = (String) metaNode.asMap().get("synapseCountPerRoi");
            Gson gson = new Gson();
            Map<String, SynapseCounter> metaSynapseCountPerRoiMap = gson.fromJson(metaSynapseCountPerRoi, new TypeToken<Map<String, SynapseCounter>>() {
            }.getType());

            Assert.assertEquals(7L, metaSynapseCountPerRoiMap.get("roiA").getPre());
            Assert.assertEquals(5L, metaSynapseCountPerRoiMap.get("roiA").getPost());

            Assert.assertEquals(5, metaSynapseCountPerRoiMap.keySet().size());
            Assert.assertTrue(metaSynapseCountPerRoiMap.containsKey("roiA")
                    && metaSynapseCountPerRoiMap.containsKey("roiB")
                    && metaSynapseCountPerRoiMap.containsKey("anotherRoi")
                    && metaSynapseCountPerRoiMap.containsKey("newRoi")
                    && metaSynapseCountPerRoiMap.containsKey("seven_column_roi"));

        }

    }
}



