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

public class InputAndOutputCountsForRoisTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(AnalysisProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(Create.class);

    @Test
    public void shouldGetCountsForRois() {

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


            String jsonData = session.writeTransaction(tx -> {
                String json = tx.run("CALL analysis.getInputAndOutputCountsForRois(8426959,\"test\") YIELD value AS dataJson RETURN dataJson").single().get(0).asString();
                return json;
            });

            Gson gson = new Gson();

            RoiSynapseCount[] roiSynapseCountsArray = gson.fromJson(jsonData,RoiSynapseCount[].class);
            List<RoiSynapseCount> roiSynapseCountList = Arrays.asList(roiSynapseCountsArray);

            //is 4 due to overlapping rois which should not occur in real data
            Assert.assertEquals(new Long(4),roiSynapseCountList.get(2).getOutputCount());

            Assert.assertEquals(new Long(2),roiSynapseCountList.get(1).getOutputCount());

        }
    }
}
