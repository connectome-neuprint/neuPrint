package org.janelia.flyem.neuprintprocedures.analysis;

import apoc.convert.Json;
import apoc.create.Create;
import apoc.refactor.GraphRefactoring;
import org.janelia.flyem.neuprint.Neo4jImporter;
import org.janelia.flyem.neuprint.NeuPrintMain;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
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

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

public class ShortestPathTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(AnalysisProcedures.class)
            .withProcedure(LoadingProcedures.class)
            .withProcedure(GraphRefactoring.class)
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void shouldGetShortestPathThatMeetsMinWeightThreshold() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {

            Session session = driver.session();
            final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

            String neuronsJsonPath = "src/test/resources/shortestPathNeuronList.json";
            List<Neuron> neuronList = NeuPrintMain.readNeuronsJson(neuronsJsonPath);

            String synapseJsonPath = "src/test/resources/shortestPathSynapseList.json";
            List<Synapse> synapseList = NeuPrintMain.readSynapsesJson(synapseJsonPath);

            String connectionsJsonPath = "src/test/resources/shortestPathConnectionsList.json";
            List<SynapticConnection> connectionsList = NeuPrintMain.readConnectionsJson(connectionsJsonPath);

            Neo4jImporter neo4jImporter = new Neo4jImporter(driver);

            String dataset = "test";

            NeuPrintMain.runStandardLoadWithoutMetaInfo(neo4jImporter, dataset, synapseList, connectionsList, neuronList, new ArrayList<>(), 1.0F, .2D, .8D, 5,true, true, timeStamp);

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

                Assert.assertEquals(2, segmentsWithMinWeight.length());

            }

        }

    }

}
