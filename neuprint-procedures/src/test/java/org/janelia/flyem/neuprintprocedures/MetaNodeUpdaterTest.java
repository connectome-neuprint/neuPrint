package org.janelia.flyem.neuprintprocedures;

import apoc.create.Create;
import org.janelia.flyem.neuprinter.ConnConvert;
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

        List<Neuron> neuronList = ConnConvert.readNeuronsJson("src/test/resources/smallNeuronList.json");
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

            neo4jImporter.addConnectsTo(dataset, bodyList, 10);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();
            }
            neo4jImporter.addNeuronParts(dataset, bodyList);
            neo4jImporter.createMetaNode(dataset);

            session.run("CREATE (n:Neuron:test{bodyId:50}) SET n.pre=5, n.post=2 RETURN n");

            Node metaNode = session.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode();

            Assert.assertEquals(6L,metaNode.asMap().get("totalPostCount"));


            System.out.println("metaNode: " + metaNode.asMap());
//
//            Assert.assertEquals(5L, metaNodeV1.asMap().get("totalPreCount"));
//            Assert.assertEquals(2L, metaNodeV1.asMap().get("totalPostCount"));
//
//            session.run("CREATE (n:Neuron:test{bodyId:2}) SET n.pre=5, n.post=0 RETURN n");
//
//            // TODO: figure out how to clear test harness cache
//            Node metaNodeV2 = session.run("MATCH (n:Meta:test{dataset:\"test\"}) RETURN n").single().get(0).asNode();
//
//            Assert.assertEquals(10L, metaNodeV2.get("totalPreCount").asLong());
//            Assert.assertEquals(2L, metaNodeV2.get("totalPostCount").asLong());
//


        }
    }


}
