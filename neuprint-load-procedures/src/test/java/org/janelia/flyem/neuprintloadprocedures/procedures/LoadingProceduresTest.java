package org.janelia.flyem.neuprintloadprocedures.procedures;

import apoc.convert.Json;
import apoc.create.Create;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprintloadprocedures.model.RoiInfoWithHighPrecisionCounts;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounterWithHighPrecisionCounts;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Map;

import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.addSynapseToRoiInfoWithHP;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.Values.point;

public class LoadingProceduresTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(LoadingProcedures.class)
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void shouldAddRoiInfoToConnectionSetAndWeightHPToConnectsTo() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig())) {
            Session session = driver.session();

            session.writeTransaction(tx -> tx.run("CREATE (n:`test-Segment`:Segment:test{bodyId:$preBodyId})-[c:ConnectsTo{weight:1}]->(m:`test-Segment`:Segment:test{bodyId:$postBodyId})",
                    parameters("preBodyId", 8426959, "postBodyId", 26311)));

            session.writeTransaction(tx -> tx.run("MERGE (n:`test-Segment`{bodyId:$preBodyId}) \n" +
                            "MERGE (m:`test-Segment`{bodyId:$postBodyId}) \n" +
                            "MERGE (c:ConnectionSet:`test-ConnectionSet`:test{datasetBodyIds:$datasetBodyIds}) \n" +
                            "MERGE (n)<-[:From]-(c)-[:To]->(m)",
                    parameters("preBodyId", 8426959, "postBodyId", 26311, "datasetBodyIds", "test:" + 8426959 + ":" + 26311)));

            session.writeTransaction(tx -> tx.run(
                    "MERGE (s1:`test-Synapse`{location:$location1}) ON CREATE SET s1.type=\"pre\", s1.confidence=0.6, s1.roiA=True\n" +
                            "MERGE (s2:`test-Synapse`{location:$location2}) ON CREATE SET s2.type=\"pre\", s2.confidence=1.0, s2.roiA=True \n" +
                            "MERGE (s3:`test-Synapse`{location:$location3}) ON CREATE SET s3.type=\"post\", s3.confidence=0.7, s3.roiA=True \n" +
                            "MERGE (s4:`test-Synapse`{location:$location4}) ON CREATE SET s4.type=\"post\", s4.confidence=1.0, s4.roiA=True \n" +
                            "MERGE (c:ConnectionSet{datasetBodyIds:$datasetBodyIds}) \n" +
                            "MERGE (c)-[:Contains]->(s1) \n" +
                            "MERGE (c)-[:Contains]->(s2) \n" +
                            "MERGE (c)-[:Contains]->(s3) \n" +
                            "MERGE (c)-[:Contains]->(s4) \n",
                    parameters(
                            "location1", point(9157, 2, 3, 1),
                            "location2", point(9157, 4, 1, 1),
                            "location3", point(9157, 7, 8, 1),
                            "location4", point(9157, 2, 3, 2),
                            "datasetBodyIds", "test:" + 8426959 + ":" + 26311)));

            session.writeTransaction(tx -> tx.run("CALL loader.setConnectionSetRoiInfoAndWeightHP($preBodyId, $postBodyId, $dataset, $preHPThreshold, $postHPThreshold)", parameters("preBodyId", 8426959, "postBodyId", 26311, "dataset", "test", "preHPThreshold", .65, "postHPThreshold", .8)));

            String roiInfoString = session.readTransaction(tx -> tx.run("MATCH (n:`test-ConnectionSet`{datasetBodyIds:$datasetBodyIds}) RETURN n.roiInfo", parameters("datasetBodyIds", "test:8426959:26311"))).single().get("n.roiInfo").asString();

            Assert.assertNotNull(roiInfoString);

            Gson gson = new Gson();
            Map<String, SynapseCounterWithHighPrecisionCounts> roiInfo = gson.fromJson(roiInfoString, new TypeToken<Map<String, SynapseCounterWithHighPrecisionCounts>>() {
            }.getType());

            Assert.assertEquals(1, roiInfo.size());

            Assert.assertEquals(2, roiInfo.get("roiA").getPre());
            Assert.assertEquals(1, roiInfo.get("roiA").getPreHP());
            Assert.assertEquals(2, roiInfo.get("roiA").getPost());
            Assert.assertEquals(1, roiInfo.get("roiA").getPostHP());

            int weightHP = session.readTransaction(tx -> tx.run("MATCH (:`test-Segment`{bodyId:8426959})-[c:ConnectsTo]->({bodyId:26311}) RETURN c.weightHP")).single().get("c.weightHP").asInt();

            Assert.assertEquals(1, weightHP);
        }
    }

    @Test
    public void shouldAddSynapseToRoiInfoWithHP() {
        RoiInfoWithHighPrecisionCounts roiInfo = new RoiInfoWithHighPrecisionCounts();
        roiInfo.incrementPreForRoi("roiA");
        roiInfo.incrementPreHPForRoi("roiA");
        roiInfo.incrementPostForRoi("roiA");

        String initialRoiInfoString = roiInfo.getAsJsonString();

        String newRoiInfoString = addSynapseToRoiInfoWithHP(initialRoiInfoString, "roiB", "post", 0.5, .9, .4);

        Assert.assertEquals("{\"roiA\":{\"preHP\":1,\"postHP\":0,\"pre\":1,\"post\":1},\"roiB\":{\"preHP\":0,\"postHP\":1,\"pre\":0,\"post\":1}}",newRoiInfoString);

        String newRoiInfoString2 = addSynapseToRoiInfoWithHP(newRoiInfoString, "roiB", "pre", 0.98, .9, .4);

        Assert.assertEquals("{\"roiA\":{\"preHP\":1,\"postHP\":0,\"pre\":1,\"post\":1},\"roiB\":{\"preHP\":1,\"postHP\":1,\"pre\":1,\"post\":1}}",newRoiInfoString2);

    }
}
