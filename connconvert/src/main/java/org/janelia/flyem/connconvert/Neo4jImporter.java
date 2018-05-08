package org.janelia.flyem.connconvert;

import org.janelia.flyem.connconvert.db.DbConfig;

import org.janelia.flyem.connconvert.db.DbTransactionBatch;
import org.janelia.flyem.connconvert.db.StdOutTransactionBatch;
import org.janelia.flyem.connconvert.db.TransactionBatch;
import org.janelia.flyem.connconvert.model.Neuron;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporter implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;

    public Neo4jImporter(final DbConfig dbConfig) {

        if (dbConfig == null) {

            this.driver = null;
            this.statementsPerTransaction = 1;

        } else {

            this.driver = GraphDatabase.driver(dbConfig.getUri(),
                    AuthTokens.basic(dbConfig.getUser(),
                            dbConfig.getPassword()));
            this.statementsPerTransaction = dbConfig.getStatementsPerTransaction();

        }

    }

    @Override
    public void close() {
        driver.close();
        System.out.println("Driver closed.");
    }

    private TransactionBatch getBatch() {
        final TransactionBatch batch;
        if (driver == null) {
            batch = new StdOutTransactionBatch();
        } else {
            batch = new DbTransactionBatch(driver.session(), statementsPerTransaction);
        }
        return batch;
    }

    public void prepDatabase() {

        LOG.info("prepDatabase: entry");

        final String[] prepTextArray = {
                "CREATE CONSTRAINT ON (n:Neuron) ASSERT n.datasetBodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (n:Neuron) ASSERT n.sId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:SynapseSet) ASSERT s.datasetBodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:Synapse) ASSERT s.datasetLocation IS UNIQUE",
                "CREATE CONSTRAINT ON (p:NeuronPart) ASSERT p.neuronPartId IS UNIQUE",
                "CREATE INDEX ON :Neuron(bodyId)",
                "CREATE INDEX ON :Neuron(status)",
                "CREATE INDEX ON :Synapse(x)",
                "CREATE INDEX ON :Synapse(y)",
                "CREATE INDEX ON :Synapse(z)",
                "CREATE INDEX ON :Synapse(location)"
        };

        for (final String prepText : prepTextArray) {
            try (final TransactionBatch batch = getBatch()) {
                batch.addStatement(new Statement(prepText));
                batch.writeTransaction();
            }
        }

        LOG.info("prepDatabase: exit");

    }

    public void addNeurons(final String dataset,
                           final List<Neuron> neuronList) {

        final String neuronText = "MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) " +
                "ON CREATE SET n.bodyId = $bodyId," +
                " n.name = $name," +
                " n.type = $type," +
                " n.status = $status," +
                " n.datasetBodyId = $datasetBodyId," +
                " n.size = $size" +
                " WITH n" +
                " CALL apoc.create.addLabels(id(n),['" + dataset + "']) YIELD node" +
                " RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {
                batch.addStatement(
                        new Statement(neuronText,
                                parameters("bodyId", neuron.getId(),
                                        "name", neuron.getName(),
                                        "type", neuron.getNeuronType(),
                                        "status", neuron.getStatus(),
                                        "datasetBodyId", dataset + ":" + neuron.getId(),
                                        "size", neuron.getSize()))
                );
            }
            batch.writeTransaction();
        }

    }

    public void addConnectsTo(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addConnectsTo: entry");

        final String connectsToText =
                "MERGE (n:Neuron {datasetBodyId:$datasetBodyId1}) ON CREATE SET n.bodyId = $bodyId1, n.datasetBodyId=$datasetBodyId1 \n" +
                        "MERGE (m:Neuron {datasetBodyId:$datasetBodyId2}) ON CREATE SET m.bodyId = $bodyId2, m.datasetBodyId=$datasetBodyId2 \n" +
                        "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m) \n" +
                        "WITH n,m \n" +
                        "CALL apoc.create.addLabels([id(n),id(m)],['" + dataset + "']) YIELD node AS node1 \n" +
                        "CALL apoc.create.addLabels(id(n),$rois) YIELD node AS node2 \n" +
                        "RETURN node1,node2";

        final String terminalCountText = "MATCH (n:Neuron {datasetBodyId:$datasetBodyId} ) SET n.pre = $pre, n.post = $post";

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses bws : bodyList) {
                for (final Long postsynapticBodyId : bws.getConnectsTo().keySet()) {
                    batch.addStatement(
                            new Statement(connectsToText,
                                    parameters("bodyId1", bws.getBodyId(),
                                            "bodyId2", postsynapticBodyId,
                                            "datasetBodyId1", dataset + ":" + bws.getBodyId(),
                                            "datasetBodyId2", dataset + ":" + postsynapticBodyId,
                                            "weight", bws.getConnectsTo().get(postsynapticBodyId),
                                            "rois", bws.getBodyRois()))
                    );
                }
                batch.addStatement(
                        new Statement(terminalCountText,
                                parameters("pre", bws.getNumberOfPreSynapses(),
                                        "post", bws.getNumberOfPostSynapses(),
                                        "datasetBodyId", dataset + ":" + bws.getBodyId()))
                );

            }
            batch.writeTransaction();
        }

        LOG.info("addConnectsTo: exit");

    }

    public void addSynapses(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSynapses: entry");

        final String preSynapseText =
                "MERGE (s:Synapse:PreSyn {datasetLocation:$datasetLocation}) " +
                        " ON CREATE SET s.location=$location, " +
                        " s.datasetLocation = $datasetLocation," +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        " s.x=$x, " +
                        " s.y=$y, " +
                        " s.z=$z \n" +
                        " WITH s \n" +
                        " CALL apoc.create.addLabels(id(s),$datasetAndRois) YIELD node \n" +
                        " RETURN node";

        final String postSynapseText =
                "MERGE (s:Synapse:PostSyn {datasetLocation:$datasetLocation}) " +
                        " ON CREATE SET s.location=$location, " +
                        " s.datasetLocation = $datasetLocation," +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        " s.x=$x, " +
                        " s.y=$y, " +
                        " s.z=$z \n" +
                        " WITH s \n" +
                        " CALL apoc.create.addLabels(id(s),$datasetAndRois) YIELD node \n" +
                        " RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses bws : bodyList) {
                // issue with this body id in mb6
                if (bws.getBodyId() != 304654117 || !(dataset.equals("mb6v2") || dataset.equals("mb6"))) {
                    for (final Synapse synapse : bws.getSynapseSet()) {
                        List<String> datasetAndRois = synapse.getRois();
                        datasetAndRois.add(dataset);
                        if (synapse.getType().equals("pre")) {

                            batch.addStatement(new Statement(
                                    preSynapseText,
                                    parameters("location", synapse.getLocationString(),
                                            "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                            "confidence", synapse.getConfidence(),
                                            "type", synapse.getType(),
                                            "x", synapse.getLocation().get(0),
                                            "y", synapse.getLocation().get(1),
                                            "z", synapse.getLocation().get(2),
                                            "datasetAndRois", datasetAndRois))
                            );
                        } else if (synapse.getType().equals("post")) {
                            batch.addStatement(new Statement(
                                    postSynapseText,
                                    parameters("location", synapse.getLocationString(),
                                            "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                            "confidence", synapse.getConfidence(),
                                            "type", synapse.getType(),
                                            "x", synapse.getLocation().get(0),
                                            "y", synapse.getLocation().get(1),
                                            "z", synapse.getLocation().get(2),
                                            "datasetAndRois", datasetAndRois))
                            );

                        }
                    }
                }
            }
            batch.writeTransaction();
        }
        LOG.info("addSynapses: exit");
    }




    public void addSynapsesTo(final String dataset,  HashMap<String,List<String>> preToPost) {

        LOG.info("addSynapsesTo: entry");

        final String synapseRelationsText = "MERGE (s:Synapse {datasetLocation:$datasetPreLocation}) ON CREATE SET s.location = $prelocation, s.datasetLocation=$datasetPreLocation, s:createdforsynapsesto \n" +
                "MERGE (t:Synapse {datasetLocation:$datasetPostLocation}) ON CREATE SET t.location = $postlocation, t.datasetLocation=$datasetPostLocation, t:createdforsynapsesto \n" +
                "MERGE (s)-[:SynapsesTo]->(t)";

        try (final TransactionBatch batch = getBatch()) {
            for (String preLoc: preToPost.keySet()) {
                for (String postLoc : preToPost.get(preLoc)) {
                    batch.addStatement(new Statement(synapseRelationsText,
                        parameters("prelocation", preLoc,
                                "datasetPreLocation", dataset+ ":" +preLoc,
                                "datasetPostLocation", dataset+ ":" +postLoc,
                                "postlocation", postLoc))
                );
                }
            }
            batch.writeTransaction();
        }


        LOG.info("addSynapsesTo: exit");
    }


    public void addRois(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addRois: entry");

        final String roiSynapseText = "MERGE (s:Synapse {datasetLocation:$datasetLocation}) ON CREATE SET s.location = $location, s.datasetLocation=$datasetLocation \n" +
                "WITH s \n" +
                "CALL apoc.create.addLabels(id(s),$rois) YIELD node \n" +
                "RETURN node";

        final String roiNeuronText = "MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId = $bodyId, n.datasetBodyId=$datasetBodyId \n" +
                "WITH n \n" +
                "CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (Synapse synapse: bws.getSynapseSet()) {
                    List<String> roiList = synapse.getRois();
                    batch.addStatement(new Statement(roiSynapseText,parameters("location", synapse.getLocationString(),
                            "datasetLocation",dataset+":"+synapse.getLocationString(),
                            "rois", roiList)));
                    batch.addStatement(new Statement(roiNeuronText,parameters("bodyId", bws.getBodyId(),
                            "datasetBodyId",dataset+":"+bws.getBodyId(),
                            "rois", roiList)));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addRois: exit");


    }


    public void addNeuronParts(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addNeuronParts: entry");

        final String neuronPartText = "MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId=$bodyId, n.datasetBodyId=$datasetBodyId, n:createdforneuronpart \n"+
                "MERGE (p:NeuronPart {neuronPartId:$neuronPartId}) ON CREATE SET p.neuronPartId = $neuronPartId, p.pre=$pre, p.post=$post, p.size=$size \n"+
                "MERGE (p)-[:PartOf]->(n) \n" +
                "WITH p \n" +
                "CALL apoc.create.addLabels(id(p),[$roi, $dataset]) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (NeuronPart np : bws.getNeuronParts()) {
                    String neuronPartId = dataset+":"+bws.getBodyId()+":"+np.getRoi();
                    batch.addStatement(new Statement(neuronPartText,parameters("bodyId",bws.getBodyId(),
                            "roi",np.getRoi(),
                            "dataset",dataset,
                            "neuronPartId",neuronPartId,
                            "datasetBodyId",dataset+":"+bws.getBodyId(),
                            "pre",np.getPre(),
                            "post",np.getPost(),
                            "size",np.getPre()+np.getPost())));

                }
            }
            batch.writeTransaction();
        }
        LOG.info("addNeuronParts: exit");
    }



    public void addSizeId(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSizeId: entry");

        final String sizeIdText = "MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId=$bodyId, n.datasetBodyId=$datasetBodyId, n:createdforsid \n" +
                "SET n.sId=$sId";

        int sId = 0;

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                batch.addStatement(new Statement(sizeIdText,parameters("bodyId", bws.getBodyId(),
                        "datasetBodyId",dataset+":"+ bws.getBodyId(),
                        "sId", sId)));
                sId++;
            }
            batch.writeTransaction();
        }


        LOG.info("addSize: exit");
    }


    public void addSynapseSets(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSynapseSets: entry");

        final String neuronContainsSSText = "MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId=$bodyId, n.datasetBodyId=$datasetBodyId \n" +
                "MERGE (s:SynapseSet {datasetBodyId:$datasetBodyId}) ON CREATE SET s.datasetBodyId=$datasetBodyId \n" +
                "MERGE (n)-[:Contains]->(s) \n" +
                "WITH s \n" +
                "CALL apoc.create.addLabels(id(s),[$dataset]) YIELD node \n" +
                "RETURN node";

        final String ssContainsSynapseText = "MERGE (s:Synapse {datasetLocation:$datasetLocation}) ON CREATE SET s.location=$location, s.datasetLocation=$datasetLocation \n"+
                "MERGE (t:SynapseSet {datasetBodyId:$datasetBodyId}) ON CREATE SET t.bodyId=$datasetBodyId \n" +
                "MERGE (t)-[:Contains]->(s) \n" +
                "WITH t \n" +
                "CALL apoc.create.addLabels(id(t),[$dataset]) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                batch.addStatement(new Statement(neuronContainsSSText,parameters("bodyId",bws.getBodyId(),
                        "datasetBodyId",dataset+":"+bws.getBodyId(),
                        "dataset",dataset)));

                for (Synapse synapse : bws.getSynapseSet()) {
                    batch.addStatement(new Statement(ssContainsSynapseText, parameters("location", synapse.getLocationString(),
                            "datasetLocation",dataset+":"+synapse.getLocationString(),
                            "bodyId", bws.getBodyId(),
                            "datasetBodyId",dataset+":"+bws.getBodyId(),
                            "dataset",dataset)));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSynapseSets: exit");
    }




    private static final Logger LOG = LoggerFactory.getLogger(Neo4jImporter.class);
}