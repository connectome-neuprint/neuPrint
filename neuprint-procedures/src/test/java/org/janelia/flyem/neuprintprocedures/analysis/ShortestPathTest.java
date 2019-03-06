package org.janelia.flyem.neuprintprocedures.analysis;

import apoc.convert.Json;
import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import org.janelia.flyem.neuprinter.Neo4jImporter;
import org.janelia.flyem.neuprinter.NeuPrinterMain;
import org.janelia.flyem.neuprinter.SynapseMapper;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;
import org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

public class ShortestPathTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(AnalysisProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withProcedure(LoadingProcedures.class)
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void shouldGetShortestPathThatMeetsMinWeightThreshold() {

        List<Neuron> neuronList = NeuPrinterMain.readNeuronsJson("src/test/resources/smallNeuronList.json");
        SynapseMapper mapper = new SynapseMapper();
        List<BodyWithSynapses> bodyList = mapper.loadAndMapBodies("src/test/resources/smallBodyListWithExtraRoisForShortestPath.json");
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
            neo4jImporter.createMetaNodeWithDataModelNode(dataset, 1.0F, .20F, .80F);
            neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 0);

            Path segments = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959}), (m{bodyId:26311}) CALL analysis.getShortestPathWithMinWeight" +
                    "(n,m," +
                    "$relationshipTypeAndDir," +
                    "$weightName," +
                    "$thresholdName," +
                    "$defaultWeight," +
                    "$minValue) " +
                    "YIELD path, weight RETURN path, weight", parameters(
                    "relationshipTypeAndDir", "ConnectsTo>",
                    "weightName", "default",
                    "defaultWeight", 1,
                    "thresholdName", "weight",
                    "minValue", 1
            )).single().get(0).asPath());

            for (Node node : segments.nodes()) {
                long bodyId = (Long) node.asMap().get("bodyId");
                Assert.assertTrue(bodyId == 8426959L || bodyId == 26311L);
            }

            List<Record> records = session.readTransaction(tx -> tx.run("MATCH (n{bodyId:8426959}), (m{bodyId:26311}) CALL analysis.getShortestPathWithMinWeight" +
                    "(n,m," +
                    "$relationshipTypeAndDir," +
                    "$weightName," +
                    "$thresholdName," +
                    "$defaultWeight," +
                    "$minValue) " +
                    "YIELD path, weight RETURN path, weight", parameters(
                    "relationshipTypeAndDir", "ConnectsTo>",
                    "weightName", "default",
                    "defaultWeight", 1,
                    "thresholdName", "weight",
                    "minValue", 2
            )).list());


            Assert.assertEquals(2, records.size());

            for (Record record : records) {
                Path segmentsWithMinWeight = (Path) record.asMap().get("path");

                for (Node node : segmentsWithMinWeight.nodes()) {
                    long bodyId = (Long) node.asMap().get("bodyId");
                    Assert.assertTrue(bodyId == 8426959L || bodyId == 26311L || bodyId == 1L || bodyId == 2L);
                }

                for (Relationship relationship : segmentsWithMinWeight.relationships()) {
                    Assert.assertEquals(2L, relationship.asMap().get("weight"));
                }

                Assert.assertEquals(2,segmentsWithMinWeight.length());

            }



        }

    }

}
