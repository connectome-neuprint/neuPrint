package org.janelia.flyem.neuprint;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.janelia.flyem.neuprint.db.DbConfig;
import org.janelia.flyem.neuprint.db.DbTransactionBatch;
import org.janelia.flyem.neuprint.db.StdOutTransactionBatch;
import org.janelia.flyem.neuprint.db.TransactionBatch;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.MetaInfo;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.SkelNode;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.janelia.flyem.neuprintloadprocedures.model.RoiInfo;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * A class for importing neuron and synapse information into a neuprint neo4j database.
 */
public class Neo4jImporter implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;

    /**
     * Class constructor.
     *
     * @param dbConfig {@link DbConfig} object containing the database configuration
     */
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

    /**
     * Class constructor for testing.
     *
     * @param driver neo4j bolt driver
     */
    public Neo4jImporter(final Driver driver) {
        this.driver = driver;
        this.statementsPerTransaction = 20;
    }

    /**
     * Closes driver.
     */
    @Override
    public void close() {
        driver.close();
        LOG.info("Driver closed.");
    }

    /**
     * Acquires a database transaction batch.
     *
     * @return {@link TransactionBatch} object for storing and writing transactions
     */
    private TransactionBatch getBatch() {
        final TransactionBatch batch;
        if (driver == null) {
            batch = new StdOutTransactionBatch();
        } else {
            batch = new DbTransactionBatch(driver.session(), statementsPerTransaction);
        }
        return batch;
    }

    /**
     * Adds uniqueness constraints and indices to database.
     *
     * @param dataset dataset name
     */
    public void prepDatabase(final String dataset) {

        LOG.info("prepDatabase: entry");

        final String[] prepTextArray = {
                "CREATE CONSTRAINT ON (n:`" + dataset + "-Neuron`) ASSERT n.bodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (n:`" + dataset + "-Segment`) ASSERT n.bodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-ConnectionSet`) ASSERT s.datasetBodyIds IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-SynapseSet`) ASSERT s.datasetBodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-PreSyn`) ASSERT s.location IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-PostSyn`) ASSERT s.location IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-SkelNode`) ASSERT s.skelNodeId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-Skeleton`) ASSERT s.skeletonId IS UNIQUE",
                "CREATE CONSTRAINT ON (m:Meta) ASSERT m.dataset IS UNIQUE",
                "CREATE CONSTRAINT ON (d:DataModel) ASSERT d.dataModelVersion IS UNIQUE",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(status)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(somaLocation)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(name)",
                "CREATE INDEX ON :`" + dataset + "-SkelNode`(location)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(pre)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(post)",
                "CREATE INDEX ON :Neuron(name)",
                "CREATE INDEX ON :`" + dataset + "-Segment`(pre)",
                "CREATE INDEX ON :`" + dataset + "-Segment`(post)",
                "CREATE INDEX ON :`" + dataset + "-Synapse`(location)",
                "CREATE CONSTRAINT ON (n:`" + dataset + "-Segment`) ASSERT n.mutationUuidAndId IS UNIQUE" //used for live updates
        };

        for (final String prepText : prepTextArray) {
            try (final TransactionBatch batch = getBatch()) {
                batch.addStatement(new Statement(prepText));
                batch.writeTransaction();
            }
        }

        LOG.info("prepDatabase: exit");

    }

    public void prepDatabaseForClusterNames(final String dataset) {

        LOG.info("prepDatabaseForClusterNames: entry");

        final String prepText = "CREATE INDEX ON :`" + dataset + "-Neuron`(clusterName)";

        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(prepText));
            batch.writeTransaction();
        }

        LOG.info("prepDatabaseForClusterNames: exit");
    }

    public void indexBooleanRoiProperties(String dataset) {

        LOG.info("indexBooleanRoiProperties: entry");

        RoiInfo currentRoiInfo;
        try (Session session = driver.session()) {
            currentRoiInfo = session.readTransaction(tx -> getMetaNodeRoiInfo(tx, dataset));
        }
        if (currentRoiInfo == null) {
            currentRoiInfo = new RoiInfo();
        }

        Set<String> roiNameSet = currentRoiInfo.getSetOfRois();

        String[] indexTextArray = new String[roiNameSet.size() * 4];
        int i = 0;
        for (String roi : roiNameSet) {
            indexTextArray[i] = "CREATE INDEX ON :`" + dataset + "-Neuron`(`" + roi + "`)";
            indexTextArray[i + 1] = "CREATE INDEX ON :`" + dataset + "-Segment`(`" + roi + "`)";
            indexTextArray[i + 2] = "CREATE INDEX ON :`" + dataset + "-PreSyn`(`" + roi + "`)";
            indexTextArray[i + 3] = "CREATE INDEX ON :`" + dataset + "-PostSyn`(`" + roi + "`)";
            i += 4;
        }

        for (final String indexText : indexTextArray) {
            try (final TransactionBatch batch = getBatch()) {
                batch.addStatement(new Statement(indexText));
                batch.writeTransaction();
            }
        }
        LOG.info("indexBooleanRoiProperties: exit");

    }

    /**
     * Adds a Meta node and DataModel node to the database (if they do not exist). The Meta node stores summary information for
     * a given dataset. The DataModel node indicates the data model version and links to all Meta nodes in the database with
     * an Is relationship.
     *
     * @param dataset                            dataset name
     * @param dataModelVersion                   version of data model
     * @param preHPThreshold                     high-precision threshold for presynaptic densities
     * @param postHPThreshold                    high-precision threshold for postsynaptic densities
     * @param addConnectionSetRoiInfoAndWeightHP boolean indicating if ConnectionSet nodes should have roiInfo property and weightHP should be added to ConnectsTo relationships
     * @param timeStamp                          time stamp for load
     */
    public void createMetaNodeWithDataModelNode(final String dataset,
                                                final float dataModelVersion,
                                                final double preHPThreshold,
                                                final double postHPThreshold,
                                                final boolean addConnectionSetRoiInfoAndWeightHP,
                                                final LocalDateTime timeStamp) {

        LOG.info("createMetaNodeWithDataModelNode: enter");

        final String metaNodeString = "MERGE (m:Meta{dataset:$dataset}) ON CREATE SET " +
                "m:" + dataset + "," +
                "m.lastDatabaseEdit=$timeStamp," +
                "m.dataset=$dataset, " +
                "m.roiInfo=\"{}\", " + // starts empty
                "m.superLevelRois=[], " + // starts empty
                "m.preHPThreshold=$preHPThreshold, " +
                "m.postHPThreshold=$postHPThreshold, " +
                "m.totalPreCount=0, " +
                "m.totalPostCount=0";

        final String metaNodeStringWithoutHPThresholds = "MERGE (m:Meta{dataset:$dataset}) ON CREATE SET " +
                "m:" + dataset + "," +
                "m.lastDatabaseEdit=$timeStamp," +
                "m.dataset=$dataset, " +
                "m.roiInfo=\"{}\", " + // starts empty
                "m.superLevelRois=[], " + // starts empty
                "m.totalPreCount=0, " +
                "m.totalPostCount=0";

        final String dataModelString = "MERGE (d:DataModel{dataModelVersion:$dataModelVersion}) ON CREATE SET d.dataModelVersion=$dataModelVersion, d.timeStamp=$timeStamp";
        final String isString = "MERGE (m:Meta{dataset:$dataset}) \n" +
                "MERGE (d:DataModel{dataModelVersion:$dataModelVersion}) \n" +
                "MERGE (m)-[:Is]->(d)";

        try (final TransactionBatch batch = getBatch()) {
            if (addConnectionSetRoiInfoAndWeightHP) {
                batch.addStatement(new Statement(metaNodeString, parameters(
                        "dataset", dataset,
                        "preHPThreshold", preHPThreshold,
                        "postHPThreshold", postHPThreshold,
                        "timeStamp", timeStamp
                )));
            } else {
                batch.addStatement(new Statement(metaNodeStringWithoutHPThresholds, parameters(
                        "dataset", dataset,
                        "timeStamp", timeStamp,
                        "dataModelVersion", dataModelVersion
                )));
            }

            batch.addStatement(new Statement(dataModelString, parameters(
                    "dataModelVersion", dataModelVersion,
                    "timeStamp", timeStamp
            )));

            batch.addStatement(new Statement(isString, parameters(
                    "dataset", dataset,
                    "dataModelVersion", dataModelVersion,
                    "timeStamp", timeStamp
            )));

            batch.writeTransaction();

        }

        LOG.info("createMetaNodeWithDataModelNode: exit");

    }

    /**
     * Adds Synapse nodes to database as specified by a <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapses JSON file</a>.
     *
     * @param dataset     dataset
     * @param synapseList list of {@link Synapse} objects
     * @param timeStamp   time stamp for load
     */
    public void addSynapsesWithRois(final String dataset, final List<Synapse> synapseList, final LocalDateTime timeStamp) {

        LOG.info("addSynapses: entry");

        String roiPropertyBaseString = " s.`%s` = TRUE,";

        final String preSynapseText =
                "MERGE (s:`" + dataset + "-Synapse`{location:$location}) " +
                        " ON CREATE SET s.location=$location, " +
                        "s:`" + dataset + "-PreSyn`," +
                        "s:Synapse," +
                        "s:PreSyn," +
                        "s:" + dataset + "," +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        "%s" + //placeholder for roi properties
                        " s.timeStamp=$timeStamp";

        final String postSynapseText =
                "MERGE (s:`" + dataset + "-Synapse`{location:$location}) " +
                        " ON CREATE SET s.location=$location, " +
                        "s:`" + dataset + "-PostSyn`," +
                        "s:Synapse," +
                        "s:PostSyn," +
                        "s:" + dataset + "," +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        "%s" + //placeholder for roi properties
                        " s.timeStamp=$timeStamp";

        // get existing values from meta node
        Set<String> currentSuperLevelRois;
        RoiInfo currentRoiInfo;
        try (Session session = driver.session()) {
            currentSuperLevelRois = session.readTransaction(tx -> getMetaNodeSuperLevelRois(tx, dataset));
            currentRoiInfo = session.readTransaction(tx -> getMetaNodeRoiInfo(tx, dataset));
        }

        RoiInfo updatedRoiInfo = currentRoiInfo;
        Set<String> updatedSuperLevelRois = currentSuperLevelRois;

        try (final TransactionBatch batch = getBatch()) {
            for (final Synapse synapse : synapseList) {
                // accumulates super level rois and roi info data
                StringBuilder roiProperties = updateSuperRoisRoiInfoAndCreateRoiPropertyString(updatedSuperLevelRois, updatedRoiInfo, roiPropertyBaseString, synapse.getRois(), synapse.getType());

                if (synapse.getType().equals("pre")) {

                    String preSynapseTextWithRois = String.format(preSynapseText, roiProperties.toString());
                    batch.addStatement(new Statement(
                            preSynapseTextWithRois,
                            parameters("location", synapse.getLocationAsPoint(),
                                    "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                    "confidence", synapse.getConfidence(),
                                    "type", synapse.getType(),
                                    "timeStamp", timeStamp))
                    );
                } else if (synapse.getType().equals("post")) {

                    String postSynapseTextWithRois = String.format(postSynapseText, roiProperties.toString());
                    batch.addStatement(new Statement(
                            postSynapseTextWithRois,
                            parameters("location", synapse.getLocationAsPoint(),
                                    "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                    "confidence", synapse.getConfidence(),
                                    "type", synapse.getType(),
                                    "timeStamp", timeStamp))
                    );

                }

            }

            batch.writeTransaction();
        }

        //delay to allow transactions to complete before taking count
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final String metaNodeString = "MATCH (m:Meta{dataset:$dataset}) SET " +
                "m.lastDatabaseEdit=$timeStamp," +
                "m.roiInfo=$roiInfo, " +
                "m.superLevelRois=$superLevelRois, " +
                "m.totalPreCount=$totalPreCount, " +
                "m.totalPostCount=$totalPostCount";

        long totalPreCount;
        long totalPostCount;
        RoiInfo newRoiInfo = new RoiInfo();
        try (Session session = driver.session()) {
            totalPreCount = session.readTransaction(tx -> getTotalPreCount(tx, dataset));
            totalPostCount = session.readTransaction(tx -> getTotalPostCount(tx, dataset));
            for (String roi : updatedRoiInfo.getSetOfRois()) {
                long roiPreCount = session.readTransaction(tx -> getRoiPreCount(tx, dataset, roi));
                long roiPostCount = session.readTransaction(tx -> getRoiPostCount(tx, dataset, roi));
                newRoiInfo.addSynapseCountsForRoi(roi, roiPreCount, roiPostCount);
            }
        }

        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(
                    metaNodeString,
                    parameters(
                            "dataset", dataset,
                            "timeStamp", timeStamp,
                            "roiInfo", newRoiInfo.getAsJsonString(),
                            "superLevelRois", updatedSuperLevelRois,
                            "totalPreCount", totalPreCount,
                            "totalPostCount", totalPostCount
                    )

            ));
            batch.writeTransaction();
        }

        LOG.info("addSynapses: exit");
    }

    /**
     * Adds SynapsesTo relationship between Synapse nodes as specified by a <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">connections JSON file</a>.
     *
     * @param dataset                dataset name
     * @param synapticConnectionList list of {@link SynapticConnection} objects
     * @param timeStamp              time stamp for load
     */
    public void addSynapsesTo(final String dataset, final List<SynapticConnection> synapticConnectionList, final LocalDateTime timeStamp) {

        LOG.info("addSynapsesTo: entry");

        // for some reason, both merge...on create set queries in the same statement results in one of them not executing properly.
        // separating each statement to properly create synapses that may not have been previously added in addSynapses
        final String preSynapseMergeText = "MERGE (s:`" + dataset + "-PreSyn`{location:$prelocation}) ON CREATE SET s.location = $prelocation, s.type=\"pre\", s.confidence=0.0, s.timeStamp=$timeStamp, s:Synapse, s:" + dataset + ", s:PreSyn, s:`" + dataset + "-PreSyn`, s:`" + dataset + "-Synapse`";
        final String postSynapseMergeText = "MERGE (t:`" + dataset + "-PostSyn`{location:$postlocation}) ON CREATE SET t.location = $postlocation, t.timeStamp=$timeStamp, t.type=\"post\", t.confidence=0.0, t:Synapse, t:" + dataset + ", t:PostSyn, t:`" + dataset + "-PostSyn`, t:`" + dataset + "-Synapse`";

        final String synapseRelationsText = "MERGE (s:`" + dataset + "-PreSyn`{location:$prelocation}) SET s.timeStamp=$timeStamp \n" +
                "MERGE (t:`" + dataset + "-PostSyn`{location:$postlocation}) SET t.timeStamp=$timeStamp \n" +
                "MERGE (s)-[:SynapsesTo]->(t)";

        try (final TransactionBatch batch = getBatch()) {
            for (SynapticConnection connection : synapticConnectionList) {
                batch.addStatement(new Statement(preSynapseMergeText,
                        parameters(
                                "prelocation", connection.getPreLocation().getAsPoint(),
                                "timeStamp", timeStamp
                        )
                ));
                batch.addStatement(new Statement(postSynapseMergeText,
                        parameters(
                                "postlocation", connection.getPostLocation().getAsPoint(),
                                "timeStamp", timeStamp
                        )
                ));
                batch.addStatement(new Statement(synapseRelationsText,
                        parameters(
                                "prelocation", connection.getPreLocation().getAsPoint(),
                                "timeStamp", timeStamp,
                                "postlocation", connection.getPostLocation().getAsPoint()
                        )
                ));
            }
            batch.writeTransaction();
        }

        //delay to allow transactions to complete before taking count
        // note pre and post counts may have changed if synapses in the connections file were not listed in the synapses file
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final String metaNodeString = "MATCH (m:Meta{dataset:$dataset}) SET " +
                "m.lastDatabaseEdit=$timeStamp," +
                "m.totalPreCount=$totalPreCount, " +
                "m.totalPostCount=$totalPostCount";

        long totalPreCount;
        long totalPostCount;
        try (Session session = driver.session()) {
            totalPreCount = session.readTransaction(tx -> getTotalPreCount(tx, dataset));
            totalPostCount = session.readTransaction(tx -> getTotalPostCount(tx, dataset));
        }

        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(
                    metaNodeString,
                    parameters(
                            "dataset", dataset,
                            "timeStamp", timeStamp,
                            "totalPreCount", totalPreCount,
                            "totalPostCount", totalPostCount
                    )

            ));
            batch.writeTransaction();
        }

        LOG.info("addSynapsesTo: exit");
    }

    /**
     * Adds Segment nodes with properties specified by a <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON file</a>.
     *
     * @param dataset                            dataset name
     * @param neuronList                         list of {@link Neuron} objects
     * @param addConnectionSetRoiInfoAndWeightHP boolean indicating if ConnectionSet nodes should have roiInfo property and weightHP should be added to ConnectsTo relationships
     * @param preHPThreshold                     high-precision threshold for presynaptic densities
     * @param postHPThreshold                    high-precision threshold for postsynaptic densities
     * @param neuronThreshold                    Neuron must have >=neuronThreshold/5 presynaptic densities or >=neuronThreshold postsynaptic densities to be given a :Neuron label
     * @param timeStamp                          time stamp for load
     */
    public void addSegments(final String dataset,
                            final List<Neuron> neuronList,
                            final boolean addConnectionSetRoiInfoAndWeightHP,
                            final double preHPThreshold,
                            final double postHPThreshold,
                            final long neuronThreshold,
                            final LocalDateTime timeStamp) {
        LOG.info("addSegments: entry");

        String roiPropertyBaseString = " n.`%s` = TRUE,";

        final String segmentText = "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) " +
                "ON CREATE SET n.bodyId = $bodyId," +
                " n:Segment," +
                " n:" + dataset + "," +
                " n.name = $name," +
                " n.type = $type," +
                " n.instance = $instance," +
                " n.status = $status," +
                " n.size = $size," +
                " n.somaLocation = $somaLocation," +
                " n.somaRadius = $somaRadius, " +
                "%s" + //placeholder for roi properties
                " n.timeStamp = $timeStamp";

        final String synapseSetText = "MERGE (s:`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId}) ON CREATE SET s.datasetBodyId=$datasetBodyId, s.timeStamp=$timeStamp, s:SynapseSet, s:" + dataset + " \n";

        final String segmentContainsSynapseSetText = "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) \n" +
                "MERGE (s:`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId}) \n" +
                "MERGE (n)-[:Contains]->(s)";

        final String synapseSetContainsSynapseText = "MERGE (s:`" + dataset + "-Synapse`{location:$location}) \n" +
                "MERGE (t:`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId}) \n" +
                "MERGE (t)-[:Contains]->(s) \n";

        final String metaNodeString = "MATCH (m:Meta{dataset:$dataset}) SET " +
                "m.lastDatabaseEdit=$timeStamp";


        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {

                // accumulates super level rois
                StringBuilder roiProperties = updateSuperRoisRoiInfoAndCreateRoiPropertyString(new HashSet<>(), new RoiInfo(), roiPropertyBaseString, neuron.getRois(), "none");

                String segmentTextWithRois = String.format(segmentText, roiProperties.toString());

                batch.addStatement(
                        new Statement(segmentTextWithRois,
                                parameters(
                                        "bodyId", neuron.getId(),
                                        "name", neuron.getName(),
                                        "type", neuron.getType(),
                                        "instance", neuron.getInstance(),
                                        "status", neuron.getStatus(),
                                        "size", neuron.getSize(),
                                        "somaLocation", neuron.getSomaLocation(),
                                        "somaRadius", neuron.getSomaRadius(),
                                        "timeStamp", timeStamp))
                );

                if (neuron.getSynapseLocationSet().size() > 0) {
                    batch.addStatement(
                            new Statement(synapseSetText,
                                    parameters(
                                            "datasetBodyId", dataset + ":" + neuron.getId(),
                                            "timeStamp", timeStamp))
                    );

                    batch.addStatement(
                            new Statement(segmentContainsSynapseSetText,
                                    parameters(
                                            "bodyId", neuron.getId(),
                                            "datasetBodyId", dataset + ":" + neuron.getId())));

                    for (Location synapseLocation : neuron.getSynapseLocationSet()) {
                        batch.addStatement(new Statement(synapseSetContainsSynapseText,
                                parameters(
                                        "location", synapseLocation.getAsPoint(),
                                        "datasetBodyId", dataset + ":" + neuron.getId()
                                )));

                    }
                }

            }

            batch.writeTransaction();
        }

        try (final TransactionBatch batch = getBatch()) {

            batch.addStatement(new Statement(
                    metaNodeString,
                    parameters(
                            "dataset", dataset,
                            "timeStamp", timeStamp
                    )

            ));
            batch.writeTransaction();

        }

        LOG.info("addSegments: exit");
    }

    public void addConnectionInfo(final String dataset,
                                  final List<Neuron> neuronList,
                                  final boolean addConnectionSetRoiInfoAndWeightHP,
                                  final double preHPThreshold,
                                  final double postHPThreshold,
                                  final long neuronThreshold) {
        final String addConnectionDetailsToSegment = "MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId})," +
                "(ss:`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId})" +
                " WITH n,ss CALL loader.addPropsAndConnectionInfoToSegment(n, ss, $dataset, $preHPThreshold, $postHPThreshold, $neuronThreshold, $addCSRoiInfoAndWeightHP) RETURN n.bodyId";
        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {

                batch.addStatement(new Statement(addConnectionDetailsToSegment,
                        parameters(
                                "bodyId", neuron.getId(),
                                "datasetBodyId", dataset + ":" + neuron.getId(),
                                "dataset", dataset,
                                "preHPThreshold", preHPThreshold,
                                "postHPThreshold", postHPThreshold,
                                "neuronThreshold", neuronThreshold,
                                "addCSRoiInfoAndWeightHP", addConnectionSetRoiInfoAndWeightHP
                        )));

                batch.writeTransaction();

                //delay to allow transactions to complete to prevent blocking
                try {
                    TimeUnit.MILLISECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

    }

    StringBuilder updateSuperRoisRoiInfoAndCreateRoiPropertyString(Set<String> datasetSuperLevelRois, RoiInfo datasetRoiInfo, String roiPropertyBaseString, Set<String> synapseOrNeuronRois, String synapseType) {
        StringBuilder roiProperties = new StringBuilder();
        if (synapseOrNeuronRois != null && synapseOrNeuronRois.size() > 0) {
            datasetSuperLevelRois.add(synapseOrNeuronRois.iterator().next()); // first listed roi will be a "super" roi
            for (String roi : synapseOrNeuronRois) {
                roiProperties.append(String.format(roiPropertyBaseString, roi));
                if (synapseType.equals("pre")) {
                    datasetRoiInfo.incrementPreForRoi(roi);
                } else if (synapseType.equals("post")) {
                    datasetRoiInfo.incrementPostForRoi(roi);
                }
            }
        }
        return roiProperties;
    }

    /**
     * Adds Skeleton and SkelNode nodes to database. Segments are connected to Skeletons via Contains relationships.
     * Skeletons are connected to SkelNodes via Contains relationships. SkelNodes point to their children with LinksTo
     * relationships.
     *
     * @param dataset      dataset name
     * @param skeletonList list of {@link Skeleton} objects
     * @param timeStamp    time of data load
     */
    public void addSkeletonNodes(final String dataset, final List<Skeleton> skeletonList, final LocalDateTime timeStamp) {

        LOG.info("addSkeletonNodes: entry");

        final String segmentMergeString = "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) ON CREATE SET " +
                "n.bodyId=$bodyId, " +
                "n.timeStamp=$timeStamp, " +
                "n:Segment, " +
                "n:" + dataset;

        final String segmentToSkeletonConnectionString = "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp, r:Skeleton, r:" + dataset + " \n" +
                "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) \n" +
                "MERGE (n)-[:Contains]->(r) \n";

        final String parentNodeString = "MERGE (p:`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.rowNumber=$pRowNumber, p.type=$pType, p.timeStamp=$timeStamp, p:SkelNode, p:" + dataset + " \n" +
                "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) \n" +
                "MERGE (r)-[:Contains]->(p) ";

        final String childNodeString = "MERGE (c:`" + dataset + "-SkelNode`{skelNodeId:$childNodeId}) ON CREATE SET c.skelNodeId=$childNodeId, c.location=$childLocation, c.radius=$childRadius, c.rowNumber=$childRowNumber, c.type=$childType, c.timeStamp=$timeStamp, c:SkelNode, c:" + dataset + " \n" +
                "MERGE (p:`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) \n" +
                "MERGE (p)-[:LinksTo]-(c)";

        try (final TransactionBatch batch = getBatch()) {
            for (Skeleton skeleton : skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                batch.addStatement(new Statement(segmentMergeString, parameters(
                        "bodyId", associatedBodyId,
                        "timeStamp", timeStamp
                )));

                batch.addStatement(new Statement(segmentToSkeletonConnectionString, parameters("bodyId", associatedBodyId,
                        "skeletonId", dataset + ":" + associatedBodyId,
                        "timeStamp", timeStamp
                )));

                for (SkelNode skelNode : skelNodeList) {

                    batch.addStatement(new Statement(parentNodeString, parameters(
                            "pLocation", skelNode.getLocationAsPoint(),
                            "pRadius", skelNode.getRadius(),
                            "skeletonId", dataset + ":" + associatedBodyId,
                            "parentSkelNodeId", skelNode.getSkelNodeId(dataset),
                            "pRowNumber", skelNode.getRowNumber(),
                            "pType", skelNode.getType(),
                            "timeStamp", timeStamp
                    )));

                    for (SkelNode childSkelNode : skelNode.getChildren()) {
                        String childNodeId = childSkelNode.getSkelNodeId(dataset);
                        batch.addStatement(new Statement(childNodeString, parameters(
                                "parentSkelNodeId", skelNode.getSkelNodeId(dataset),
                                "skeletonId", dataset + ":" + associatedBodyId,
                                "pLocation", skelNode.getLocationAsPoint(),
                                "pRadius", skelNode.getRadius(),
                                "pRowNumber", skelNode.getRowNumber(),
                                "pType", skelNode.getType(),
                                "timeStamp", timeStamp,
                                "childNodeId", childNodeId,
                                "childLocation", childSkelNode.getLocationAsPoint(),
                                "childRadius", childSkelNode.getRadius(),
                                "childRowNumber", childSkelNode.getRowNumber(),
                                "childType", childSkelNode.getType()
                        )));
                    }
                }
                LOG.info("Added full skeleton for bodyId: " + skeleton.getAssociatedBodyId());
            }
            batch.writeTransaction();
        }
        LOG.info("addSkeletonNodes: exit");
    }

    public void addMetaInfo(String dataset, MetaInfo metaInfo, final LocalDateTime timeStamp) {

        LOG.info("addMetaInfo: enter");
        String metaNodeUuidString = "MATCH (m:Meta{dataset:$dataset}) SET m.neuroglancerInfo=$neuroglancerInfo, m.uuid=$uuid, m.dvidServer=$dvidServer, m.statusDefinitions=$statusDefinitions, m.meshHost=$meshHost, m.lastDatabaseEdit=$timeStamp";
        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(metaNodeUuidString, parameters("dataset", dataset,
                    "neuroglancerInfo", metaInfo.getNeuroglancerInfo(),
                    "uuid", metaInfo.getUuid(),
                    "dvidServer", metaInfo.getDvidServer(),
                    "statusDefinitions", metaInfo.getStatusDefinitions(),
                    "meshHost", metaInfo.getMeshHost(),
                    "timeStamp", timeStamp
            )));

            batch.writeTransaction();

        }
        LOG.info("addMetaInfo: exit");
    }

    private static String getMaxInputRoi(final Transaction tx, final String dataset, Long bodyId) {

        Gson gson = new Gson();
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId}) WITH n.roiInfo AS roiJson RETURN roiJson", parameters("bodyId", bodyId));

        String synapseCountPerRoiJson = result.single().get(0).asString();
        Map<String, SynapseCounter> synapseCountPerRoi = gson.fromJson(synapseCountPerRoiJson, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        try {
            return sortRoisByPostCount(synapseCountPerRoi).first().getKey();
        } catch (NoSuchElementException nse) {
            LOG.info(("No max input roi found for " + bodyId));
            return "NONE";
        }

    }

    private static String getMaxOutputRoi(final Transaction tx, final String dataset, Long bodyId) {
        Gson gson = new Gson();
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId}) WITH n.roiInfo AS roiJson RETURN roiJson", parameters("bodyId", bodyId));

        String synapseCountPerRoiJson = result.single().get(0).asString();
        Map<String, SynapseCounter> synapseCountPerRoi = gson.fromJson(synapseCountPerRoiJson, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        try {
            return sortRoisByPreCount(synapseCountPerRoi).first().getKey();
        } catch (NoSuchElementException nse) {
            LOG.info(("No max output roi found for " + bodyId));
            return "NONE";
        }

    }

    private static List<Long> getAllNeuronBodyIds(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`) RETURN n.bodyId");
        List<Long> bodyIdList = new ArrayList<>();
        while (result.hasNext()) {
            bodyIdList.add((Long) result.next().asMap().get("n.bodyId"));
        }
        return bodyIdList;
    }

    private static List<Long> getAllNeuronBodyIdsWithoutNames(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`) WHERE NOT exists(n.name) RETURN n.bodyId");
        List<Long> bodyIdList = new ArrayList<>();
        while (result.hasNext()) {
            bodyIdList.add((Long) result.next().asMap().get("n.bodyId"));
        }
        return bodyIdList;
    }

    private static Set<String> getMetaNodeSuperLevelRois(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (m:Meta{dataset:\"" + dataset + "\"}) WITH m.superLevelRois AS rois RETURN rois");
        List<?> resultList = (List<?>) result.next().asMap().get("rois");
        Set<String> roiSet = new HashSet<>();
        for (Object aResult : resultList) {
            roiSet.add((String) aResult);
        }
        return roiSet;
    }

    private static RoiInfo getMetaNodeRoiInfo(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (m:Meta{dataset:\"" + dataset + "\"}) WITH m.roiInfo AS roiInfo RETURN roiInfo");
        String roiInfoString = (String) result.next().asMap().get("roiInfo");
        return RoiInfo.getRoiInfoFromString(roiInfoString);
    }

    private static long getTotalPreCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-PreSyn`) RETURN count(n)");
        return (long) result.next().asMap().get("count(n)");
    }

    private static long getTotalPostCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-PostSyn`) RETURN count(n)");
        return (long) result.next().asMap().get("count(n)");
    }

    private static long getRoiPreCount(final Transaction tx, final String dataset, final String roi) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-PreSyn`{`" + roi + "`:true}) RETURN count(n)");
        return (long) result.next().asMap().get("count(n)");
    }

    private static long getRoiPostCount(final Transaction tx, final String dataset, final String roi) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-PostSyn`{`" + roi + "`:true}) RETURN count(n)");
        return (long) result.next().asMap().get("count(n)");
    }

    private static SortedSet<Map.Entry<String, SynapseCounter>> entriesSortedByComparator(Map<String, SynapseCounter> map, Comparator<Map.Entry<String, SynapseCounter>> comparator) {
        SortedSet<Map.Entry<String, SynapseCounter>> sortedEntries = new TreeSet<>(comparator);
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    public static SortedSet<Map.Entry<String, SynapseCounter>> sortRoisByPostCount(Map<String, SynapseCounter> roiSynapseCountMap) {
        Comparator<Map.Entry<String, SynapseCounter>> comparator = (e1, e2) ->
                Math.toIntExact(e1.getValue().getPost() == e2.getValue().getPost() ? e1.getKey().compareTo(e2.getKey()) : e2.getValue().getPost() - e1.getValue().getPost());
        return entriesSortedByComparator(roiSynapseCountMap, comparator);
    }

    public static SortedSet<Map.Entry<String, SynapseCounter>> sortRoisByPreCount(Map<String, SynapseCounter> roiSynapseCountMap) {
        Comparator<Map.Entry<String, SynapseCounter>> comparator = (e1, e2) ->
                Math.toIntExact(e1.getValue().getPre() == e2.getValue().getPre() ? e1.getKey().compareTo(e2.getKey()) : e2.getValue().getPre() - e1.getValue().getPre());
        return entriesSortedByComparator(roiSynapseCountMap, comparator);
    }
//
//    private Set<String> getSuperLevelRoisFromSynapses(List<BodyWithSynapses> bodyList) {
//
//        Set<String> superLevelRois = new HashSet<>();
//
//        for (final BodyWithSynapses bws : bodyList) {
//
//            for (final Synapse synapse : bws.getSynapseSet()) {
//
//                Set<String> roiSet = synapse.getRois();
//                if (roiSet != null && roiSet.size() > 0) {
//                    superLevelRois.add(roiSet.iterator().next()); // first listed rois are "super" rois
//                }
//            }
//        }
//
//        return superLevelRois;
//    }
//

    //
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jImporter.class);
}