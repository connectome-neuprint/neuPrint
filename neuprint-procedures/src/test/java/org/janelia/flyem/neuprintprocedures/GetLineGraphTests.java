package org.janelia.flyem.neuprintprocedures;

import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import com.google.gson.Gson;
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
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetLineGraphTests {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(AnalysisProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(Create.class);


    @Test
    public void shouldStoreSynapticConnectionNodesCorrectly() {

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

            neo4jImporter.addConnectsTo(dataset, bodyList, 0);
            neo4jImporter.addSynapsesWithRois(dataset, bodyList);
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            neo4jImporter.addNeuronRois(dataset, bodyList);
            neo4jImporter.addSynapseSets(dataset, bodyList);
            for (BodyWithSynapses bws : bodyList) {
                bws.setNeuronParts();
            }
            neo4jImporter.addNeuronParts(dataset, bodyList);


            Map<String,Object> jsonData = session.writeTransaction(tx -> {
                Map<String,Object> jsonMap = tx.run("CALL analysis.getLineGraph(\"seven_column_roi\",\"test\",0,1) YIELD value AS dataJson RETURN dataJson").single().get(0).asMap();
                return jsonMap;
            });

            String nodes = (String) jsonData.get("Vertices");
            String edges = (String) jsonData.get("Edges");


            Gson gson = new Gson();

            SynapticConnectionVertex[] nodeArray = gson.fromJson(nodes,SynapticConnectionVertex[].class);
            List<SynapticConnectionVertex> nodeList = Arrays.asList(nodeArray);

            Assert.assertEquals(4, nodeList.size());
            Assert.assertEquals("8426959_to_26311",nodeList.get(2).getConnectionDescription());
            Assert.assertEquals(new Integer(2), nodeList.get(2).getPre());
            Assert.assertEquals(new Integer(2), nodeList.get(2).getPost());
            Assert.assertEquals(new Location(4219L,2458L,1520L).getLocation(), nodeList.get(2).getCentroidLocation());

            Assert.assertEquals("8426959_to_2589725",nodeList.get(3).getConnectionDescription());
            Assert.assertEquals(new Integer(2), nodeList.get(3).getPre());
            Assert.assertEquals(new Integer(1), nodeList.get(3).getPost());
            Assert.assertEquals(new Location(4291L,2283L,1529L).getLocation(), nodeList.get(3).getCentroidLocation());

            SynapticConnectionEdge[] edgeArray = gson.fromJson(edges,SynapticConnectionEdge[].class);
            List<SynapticConnectionEdge> edgeList = Arrays.asList(edgeArray);

            Assert.assertEquals(6, edgeList.size());
            Assert.assertEquals("8426959_to_2589725", edgeList.get(1).getSourceName());
            Assert.assertEquals("8426959_to_26311", edgeList.get(1).getTargetName());
            Assert.assertEquals(new Long(189),edgeList.get(1).getDistance());

        }

    }
}