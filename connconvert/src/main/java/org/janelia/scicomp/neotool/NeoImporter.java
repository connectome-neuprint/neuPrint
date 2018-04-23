package org.janelia.scicomp.neotool;

import java.util.List;

import org.janelia.scicomp.neotool.db.DbConfig;
import org.janelia.scicomp.neotool.db.DbTransactionBatch;
import org.janelia.scicomp.neotool.db.StdOutTransactionBatch;
import org.janelia.scicomp.neotool.db.TransactionBatch;
import org.janelia.scicomp.neotool.model.Body;
import org.janelia.scicomp.neotool.model.Location;
import org.janelia.scicomp.neotool.model.Neuron;
import org.janelia.scicomp.neotool.model.Synapse;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Imports in-memory object model and relationships into a neo4j database.
 */
public class NeoImporter {

    private final Driver driver;
    private final int statementsPerTransaction;

    public NeoImporter(final DbConfig dbConfig) {

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

    public void prepDatabase() {

        LOG.info("prepDatabase: entry");

        final String prepText =
                "CREATE CONSTRAINT ON (n:Neuron) ASSERT n.datasetBodyId IS UNIQUE\n" +
                "CREATE CONSTRAINT ON (s:SynapseSet) ASSERT s.datasetBodyId IS UNIQUE\n" +
                "CREATE INDEX ON :Neuron(bodyId)\n" +
                "CREATE INDEX ON :Synapse(x)\n" +
                "CREATE INDEX ON :Synapse(y)\n" +
                "CREATE INDEX ON :Synapse(z)\n" +
                "CREATE INDEX ON :Neuron(status)\n" +
                "CREATE INDEX ON :Synapse(location)\n" +
                "CREATE CONSTRAINT ON (s:Synapse) ASSERT s.datasetLocation IS UNIQUE\n" +
                "CREATE CONSTRAINT ON (p:NeuronPart) ASSERT p.neuronPartId IS UNIQUE";

        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(prepText));
            batch.writeTransaction();
        }

        LOG.info("prepDatabase: exit");
    }

    public void addNeurons(final String dataSet,
                           final List<Neuron> neuronList) throws Exception {

        final String neuronText =
                "MERGE (n:Neuron {bodyId:$bodyId}) " +
                "ON CREATE SET n.bodyId=$bodyId, " +
                "n.name=$name, " +
                "n.type=$type, " +
                "n.status=$status, " +
                "n.datasetBodyId = $datasetBodyId, " +
                "n.size = $size " +
                "WITH n " +
                "CALL apoc.create.addLabels(id(n),[$dataset]) YIELD node " +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {
                batch.addStatement(
                        new Statement(neuronText,
                                      parameters("bodyId", neuron.getId(),
                                                 "name", neuron.getName(),
                                                 "type", neuron.getNeuronType(),
                                                 "status", neuron.getStatus(),
                                                 "datasetBodyId", dataSet + ":" + neuron.getId(),
                                                 "size", neuron.getSize(),
                                                 "dataset", dataSet))
                );
            }
            batch.writeTransaction();
        }

    }

    public void importBodyList(final List<Body> bodyList) {

        LOG.info("importBodyList: entry, bodyList.size={}", bodyList.size());

        addConnectsTo(bodyList);
        addSynapses(bodyList);
        addSynapsesToRelations(bodyList);

        LOG.info("importBodyList: exit");
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

    private void addConnectsTo(final List<Body> bodyList) {

        LOG.info("addConnectsTo: entry");

        final String connectsToText =
                "MERGE (n:Neuron {bodyId:$bodyId1}) ON CREATE SET n.bodyId=$bodyId1, n:fib25, n:notinneurons\n" +
                "MERGE (m:Neuron {bodyId:$bodyId2}) ON CREATE SET m.bodyId=$bodyId2, m:fib25, m:notinneurons\n" +
                "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m)\n";

        final String terminalCountText = "MATCH (n:Neuron {bodyId:$bodyId1}) SET n.pre=$pre, n.post=$post";

        try (final TransactionBatch batch = getBatch()) {
            for (final Body body : bodyList) {
                for (final Body connectsToBody : body.getConnectsToBodies()) {
                    batch.addStatement(
                            new Statement(connectsToText,
                                          parameters("bodyId1", body.getBodyId(),
                                                     "bodyId2", connectsToBody.getBodyId(),
                                                     "weight", body.getConnectsToWeight(connectsToBody)))
                    );
                }
                batch.addStatement(
                        new Statement(terminalCountText,
                                      parameters("bodyId1", body.getBodyId(),
                                                 "pre", body.getTotalNumberOfPreSynapticTerminals(),
                                                 "post", body.getTotalNumberOfPostSynapticTerminals()))
                );

                // TODO: make sure Nicole's second getPreSynapticConnectedBodies loop is not needed
            }
            batch.writeTransaction();
        }

        LOG.info("addConnectsTo: exit");
    }

    private void addSynapses(final List<Body> bodyList) {

        LOG.info("addSynapses: entry");

        final String synapseText =
                "MERGE (s:Synapse:$neoType {location:$location}) " +
                "ON CREATE SET s.location=$location, " +
                "s.confidence=$confidence, " +
                "s.type=$type, " +
                "s.x=$x, " +
                "s.y=$y, " +
                "s.z=$z";

        try (final TransactionBatch batch = getBatch()) {
            Location location;
            for (final Body body : bodyList) {
                for (final Synapse synapse : body.getSynapseSet()) {
                    location = synapse.getLocation();

                    batch.addStatement(new Statement(
                            synapseText,
                            parameters("neoType", synapse.getNeoType(), // TODO: find better name than 'neoType'
                                       "location", location.getKey(),
                                       "confidence", synapse.getConfidence(),
                                       "type", synapse.getType(),
                                       "x", location.getX(),
                                       "y", location.getY(),
                                       "z", location.getZ())));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSynapses: exit");
    }

    private void addSynapsesToRelations(final List<Body> bodyList) {

        LOG.info("addSynapsesToRelations: entry");

        final String synapseRelationsText =
                "MERGE (s:Synapse {location:$prelocation}) ON CREATE SET s.location=$prelocation, s:createdforsynapsesto\n" +
                "MERGE (t:Synapse {location:$postlocation}) ON CREATE SET t.location=$postlocation, t:createdforsynapsesto\n" +
                "MERGE (s)-[:SynapsesTo]->(t)";

        try (final TransactionBatch batch = getBatch()) {
            for (final Body body : bodyList) {
                for (final Synapse synapse : body.getSynapseSet()) {
                    if (synapse.isPreSynapse()) {
                        final String preLocationKey = synapse.getLocation().getKey();
                        for (final Location connectionLocation : synapse.getConnections()) {
                            batch.addStatement(
                                    new Statement(synapseRelationsText,
                                                  parameters("prelocation", preLocationKey,
                                                             "postlocation", connectionLocation.getKey())));
                        }
                    }
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSynapsesToRelations: exit");
    }

    private static final Logger LOG = LoggerFactory.getLogger(NeoImporter.class);

}
