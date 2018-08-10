package org.janelia.flyem.neuprintprocedures;

import apoc.create.Create;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashMap;
import java.util.List;

public class MetaNodeUpdaterTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Create.class);

    @Test
    public void shouldUpdateMetaNodeAfterCommits() {

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRois.json");
        HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();
        bodyList.sort(new SortBodyByNumberOfSynapses());

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            String dataset = "test";

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);
            neo4jImporter.prepDatabase(dataset);

            neo4jImporter.addNeurons(dataset, neuronList);

            neo4jImporter.addConnectsTo(dataset, bodyList);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            neo4jImporter.createMetaNode(dataset);
        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();


            //TODO: write this to use roi synapse count property
            session.writeTransaction(tx -> {
                tx.run("CREATE (n:Neuron:test{bodyId:50})<-[:PartOf]-(p:NeuronPart:newRoi:test) SET n.pre=5, n.post=2 RETURN n,p");
                return 1;
            });

            Node neuronPart = session.writeTransaction(tx -> {
                Node node = tx.run(
                        "CREATE (np:NeuronPart:test:roiA{neuronPartId:\"test:50:roiA\"}) SET np.pre=5, np.post=2 RETURN np").single().get(0).asNode();
                return node;
            });

        }

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
            Session session = driver.session();

            Node metaNode = session.readTransaction(tx -> {
                Node node = tx.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode();
                return node;
            });

            Assert.assertEquals(7L, metaNode.asMap().get("totalPostCount"));
            Assert.assertEquals(8L, metaNode.asMap().get("totalPreCount"));

            Assert.assertEquals(7L, metaNode.asMap().get("roiAPreCount"));
            Assert.assertEquals(5L, metaNode.asMap().get("roiAPostCount"));

            List<String> roiList = (List<String>) metaNode.asMap().get("rois");
            Assert.assertEquals(4, roiList.size());
            Assert.assertEquals("anotherRoi", roiList.get(0));

        }


    }
}



