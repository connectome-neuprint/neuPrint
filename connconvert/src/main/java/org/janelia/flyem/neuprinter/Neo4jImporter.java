package org.janelia.flyem.neuprinter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.db.DbConfig;
import org.janelia.flyem.neuprinter.db.DbTransactionBatch;
import org.janelia.flyem.neuprinter.db.StdOutTransactionBatch;
import org.janelia.flyem.neuprinter.db.TransactionBatch;
import org.janelia.flyem.neuprinter.model.AutoName;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.SkelNode;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.Synapse;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.janelia.flyem.neuprinter.model.SynapseCountsPerRoi;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * A class for importing neuron and synapse information into a neuprint neo4j database.
 */
public class Neo4jImporter implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;
    private final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

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
    public void prepDatabase(String dataset) {

        LOG.info("prepDatabase: entry");
        final String[] prepTextArray = {
                "CREATE CONSTRAINT ON (n:`" + dataset + "-Neuron`) ASSERT n.bodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (n:`" + dataset + "-Segment`) ASSERT n.bodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-SynapseSet`) ASSERT s.datasetBodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-Synapse`) ASSERT s.location IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-SkelNode`) ASSERT s.skelNodeId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-Skeleton`) ASSERT s.skeletonId IS UNIQUE",
                "CREATE CONSTRAINT ON (m:Meta) ASSERT m.dataset IS UNIQUE",
                "CREATE CONSTRAINT ON (n:" + dataset + ") ASSERT n.autoName is UNIQUE",
                "CREATE CONSTRAINT ON (d:DataModel) ASSERT d.dataModelVersion IS UNIQUE",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(status)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(somaLocation)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(name)",
                "CREATE INDEX ON :`" + dataset + "-SkelNode`(location)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(pre)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(post)",
                "CREATE INDEX ON :Neuron(name)"
        };

        for (final String prepText : prepTextArray) {
            try (final TransactionBatch batch = getBatch()) {
                batch.addStatement(new Statement(prepText));
                batch.writeTransaction();
            }
        }

        LOG.info("prepDatabase: exit");

    }

    /**
     * Adds Segment nodes with properties specified by a <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON file</a>.
     *
     * @param dataset    dataset name
     * @param neuronList list of {@link Neuron} objects
     */
    public void addSegments(final String dataset,
                            final List<Neuron> neuronList) {
        //TODO: arbitrary properties
        LOG.info("addSegments: entry");

        String roiPropertyBaseString = " n.`%s` = TRUE,";

        final String segmentText = "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) " +
                "ON CREATE SET n.bodyId = $bodyId," +
                " n:Segment," +
                " n:" + dataset + "," +
                " n.name = $name," +
                " n.type = $type," +
                " n.status = $status," +
                " n.size = $size," +
                " n.somaLocation = $somaLocation," +
                " n.somaRadius = $somaRadius, " +
                "%s" + //placeholder for roi properties
                " n.timeStamp = $timeStamp";

        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {
                String status = neuron.getStatus() != null ? neuron.getStatus() : "not annotated";

                StringBuilder roiProperties = new StringBuilder();
                Set<String> roiSet = neuron.getRois();
                if (roiSet != null) {
                    for (String roi : roiSet) roiProperties.append(String.format(roiPropertyBaseString, roi));
                }

                String segmentTextWithRois = String.format(segmentText, roiProperties.toString());

                batch.addStatement(
                        new Statement(segmentTextWithRois,
                                parameters("bodyId", neuron.getId(),
                                        "name", neuron.getName(),
                                        "type", neuron.getNeuronType(),
                                        "status", status,
                                        "size", neuron.getSize(),
                                        "somaLocation", neuron.getSomaLocation(),
                                        "somaRadius", neuron.getSomaRadius(),
                                        "timeStamp", timeStamp))
                );
            }
            batch.writeTransaction();
        }

        LOG.info("addSegments: entry");
    }

    /**
     * Adds ConnectsTo relationships between Segment nodes as specified by a <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapses JSON file</a>.
     *
     * @param dataset  dataset name
     * @param bodyList list of {@link BodyWithSynapses} objects
     */
    public void addConnectsTo(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addConnectsTo: entry");

        final String connectsToText =
                "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId1}) ON CREATE SET n.bodyId = $bodyId1, n.status=$notAnnotated, n:Segment, n:" + dataset + " \n" +
                        "MERGE (m:`" + dataset + "-Segment`{bodyId:$bodyId2}) ON CREATE SET m.bodyId = $bodyId2, m.timeStamp=$timeStamp, m.status=$notAnnotated, m:Segment, m:" + dataset + " \n" +
                        "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m)";
        final String terminalCountText = "MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId} ) SET n.pre = $pre, n.post = $post, n.timeStamp=$timeStamp, n.roiInfo=$synapseCountPerRoi";

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses body : bodyList) {

                //set synapse counts per roi before adding to database
                body.setSynapseCountsPerRoi();

                for (final Long postsynapticBodyId : body.getConnectsTo().keySet()) {
                    batch.addStatement(
                            new Statement(connectsToText,
                                    parameters("bodyId1", body.getBodyId(),
                                            "bodyId2", postsynapticBodyId,
                                            "timeStamp", timeStamp,
                                            "notAnnotated", "not annotated",
                                            "weight", body.getConnectsTo().get(postsynapticBodyId)))
                    );
                }

                batch.addStatement(
                        new Statement(terminalCountText,
                                parameters("pre", body.getNumberOfPreSynapses(),
                                        "post", body.getNumberOfPostSynapses(),
                                        "bodyId", body.getBodyId(),
                                        "timeStamp", timeStamp,
                                        "synapseCountPerRoi", body.getSynapseCountsPerRoi().getAsJsonString()
                                ))
                );
            }
            batch.writeTransaction();
        }

        LOG.info("addConnectsTo: exit");
    }

    /**
     * Adds Synapse nodes to database as specified by a <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapses JSON file</a>.
     *
     * @param dataset  dataset
     * @param bodyList list of {@link BodyWithSynapses} objects
     */
    public void addSynapsesWithRois(final String dataset, final List<BodyWithSynapses> bodyList) {

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

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses bws : bodyList) {
                // issue with this body id in mb6
                if (bws.getBodyId() != 304654117 || !(dataset.equals("mb6v2") || dataset.equals("mb6"))) {

                    for (final Synapse synapse : bws.getSynapseSet()) {

                        StringBuilder roiProperties = new StringBuilder();
                        Set<String> roiSet = synapse.getRois();
                        if (roiSet != null) {
                            for (String roi : roiSet) roiProperties.append(String.format(roiPropertyBaseString, roi));
                        }

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
                }
            }
            batch.writeTransaction();
        }
        LOG.info("addSynapses: exit");
    }

    /**
     * Adds SynapsesTo relationship between Synapse nodes as specified by a <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapses JSON file</a>.
     * Uses a map of presynaptic density locations to postsynaptic density locations.
     *
     * @param dataset   dataset name
     * @param preToPost map of presynaptic density locations to postsynaptic density locations
     * @see SynapseMapper#getPreToPostMap()
     */
    public void addSynapsesTo(final String dataset, HashMap<String, Set<String>> preToPost) {

        LOG.info("addSynapsesTo: entry");

        final String synapseRelationsText = "MERGE (s:`" + dataset + "-Synapse`{location:$prelocation}) ON CREATE SET s.location = $prelocation, s:createdforsynapsesto, s.timeStamp=$timeStamp, s:Synapse, s:" + dataset + ", s:PreSyn, s:`" + dataset + "-PreSyn` \n" +
                "MERGE (t:`" + dataset + "-Synapse`{location:$postlocation}) ON CREATE SET t.location = $postlocation, t:createdforsynapsesto, t.timeStamp=$timeStamp, t:Synapse, t:" + dataset + ", s:PostSyn, s:`" + dataset + "-PostSyn` \n" +
                "MERGE (s)-[:SynapsesTo]->(t)";

        try (final TransactionBatch batch = getBatch()) {
            for (String preLoc : preToPost.keySet()) {
                for (String postLoc : preToPost.get(preLoc)) {
                    batch.addStatement(new Statement(synapseRelationsText,
                            parameters("prelocation", Synapse.convertLocationStringToPoint(preLoc),
                                    "timeStamp", timeStamp,
                                    "postlocation", Synapse.convertLocationStringToPoint(postLoc)))
                    );
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSynapsesTo: exit");
    }

    /**
     * Adds roi labels to Segment nodes.
     *
     * @param dataset  dataset name
     * @param bodyList list of {@link BodyWithSynapses} objects
     */
    public void addSegmentRois(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSegmentRois: entry");

        String roiPropertyBaseString = " n.`%s` = TRUE,";

        final String roiSegmentText = "MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId}) SET" +
                "%s" + //placeholder for roi properties
                " n.timeStamp=$timeStamp";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (Synapse synapse : bws.getSynapseSet()) {

                    StringBuilder roiProperties = new StringBuilder();
                    Set<String> roiSet = synapse.getRois();
                    if (roiSet != null) {
                        for (String roi : roiSet) roiProperties.append(String.format(roiPropertyBaseString, roi));
                    }

                    String roiSegmentTextWithRois = String.format(roiSegmentText, roiProperties);

                    batch.addStatement(new Statement(roiSegmentTextWithRois,
                            parameters("bodyId", bws.getBodyId(),
                                    "timeStamp", timeStamp,
                                    "notAnnotated", "not annotated")));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSegmentRois: exit");

    }

    /**
     * Adds automatically generated names (autoNames) and :Neuron labels to Segment nodes that have greater than
     * neuronThreshold synaptic densities. If Neuron node does not have a name, the name property
     * is also set to autoName* (the * marks it as automatically generated).
     *
     * @param dataset         dataset name
     * @param neuronThreshold Neuron must have greater than neuronThreshold synapses to be given an autoName and :Neuron label
     * @see AutoName
     */
    public void addAutoNamesAndNeuronLabels(final String dataset, int neuronThreshold) {

        LOG.info("addAutoNamesAndNeuronLabels: entry");

        List<AutoName> autoNameList = new ArrayList<>();
        List<Long> bodyIdsWithoutNames;
        final String autoNameText = "MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId}) SET n.autoName=$autoName, n:Neuron, n:`" + dataset + "-Neuron`";
        final String autoNameToNameText = "MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId}) SET n.autoName=$autoName, n.name=$autoNamePlusAsterisk, n:Neuron, n:`" + dataset + "-Neuron`";

        try (Session session = driver.session()) {

            // get body ids for generating auto-name
            List<Long> bodyIdList = session.readTransaction(tx -> getAllSegmentBodyIdsWithGreaterThanThresholdSynapses(tx, dataset, neuronThreshold));
            for (Long bodyId : bodyIdList) {
                String maxPostRoiName = session.readTransaction(tx -> getMaxInputRoi(tx, dataset, bodyId));
                String maxPreRoiName = session.readTransaction(tx -> getMaxOutputRoi(tx, dataset, bodyId));
                AutoName autoName = new AutoName(maxPostRoiName, maxPreRoiName, bodyId);
                autoNameList.add(autoName);
            }
            // get body ids above threshold without names
            bodyIdsWithoutNames = session.readTransaction(tx -> getAllSegmentBodyIdsWithGreaterThanThresholdSynapsesAndWithoutNames(tx, dataset, neuronThreshold));
        }

        try (final TransactionBatch batch = getBatch()) {
            for (AutoName autoName : autoNameList) {
                Long bodyId = autoName.getBodyId();
                if (bodyIdsWithoutNames.contains(bodyId)) {
                    batch.addStatement(new Statement(autoNameToNameText,
                            parameters("bodyId", autoName.getBodyId(),
                                    "autoName", autoName.getAutoName(),
                                    "autoNamePlusAsterisk", autoName.getAutoName() + "*")));
                } else {
                    batch.addStatement(new Statement(autoNameText,
                            parameters("bodyId", autoName.getBodyId(),
                                    "autoName", autoName.getAutoName())));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addAutoNamesAndNeuronLabels: exit");
    }

    /**
     * Adds :Neuron labels to Segment nodes that have greater than neuronThreshold synaptic densities.
     *
     * @param dataset         dataset name
     * @param neuronThreshold Neuron must have greater than neuronThreshold synapses to be given :Neuron label
     */
    public void addNeuronLabels(final String dataset, int neuronThreshold) {

        LOG.info("addNeuronLabels: entry");

        final String neuronText = "MATCH (n:`" + dataset + "-Segment`{bodyId:$bodyId}) SET n:Neuron, n:`" + dataset + "-Neuron`";

        List<Long> bodyIdList;
        try (Session session = driver.session()) {

            // get body ids for adding :Neuron label
            bodyIdList = session.readTransaction(tx -> getAllSegmentBodyIdsWithGreaterThanThresholdSynapses(tx, dataset, neuronThreshold));
        }

        try (final TransactionBatch batch = getBatch()) {
            for (Long bodyId : bodyIdList) {
                batch.addStatement(new Statement(neuronText,
                        parameters("bodyId", bodyId)));
            }
            batch.writeTransaction();
        }

        LOG.info("addNeuronLabels: exit");

    }

    /**
     * Adds SynapseSet nodes to database and connects them to Segment nodes and Synapse nodes via Contains relationships.
     *
     * @param dataset  dataset name
     * @param bodyList list of {@link BodyWithSynapses} objects
     */
    public void addSynapseSets(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSynapseSets: entry");

        final String segmentContainsSSText = "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n.status=$notAnnotated, n:Segment, n:" + dataset + " \n" +
                "MERGE (s:`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId}) ON CREATE SET s.datasetBodyId=$datasetBodyId, s.timeStamp=$timeStamp, s:SynapseSet, s:" + dataset + " \n" +
                "MERGE (n)-[:Contains]->(s)";

        final String ssContainsSynapseText = "MERGE (s:`" + dataset + "-Synapse`{location:$location}) ON CREATE SET s.location=$location, s:Synapse, s:" + dataset + " \n" +
                "MERGE (t:`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId}) ON CREATE SET t.datasetBodyId=$datasetBodyId, t:SynapseSet, t:" + dataset + " \n" +
                "MERGE (t)-[:Contains]->(s) \n";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                batch.addStatement(new Statement(segmentContainsSSText, parameters("bodyId", bws.getBodyId(),
                        "notAnnotated", "not annotated",
                        "datasetBodyId", dataset + ":" + bws.getBodyId(),
                        "timeStamp", timeStamp))
                );

                for (Synapse synapse : bws.getSynapseSet()) {
                    batch.addStatement(new Statement(ssContainsSynapseText, parameters("location", synapse.getLocationAsPoint(),
                            "bodyId", bws.getBodyId(),
                            "datasetBodyId", dataset + ":" + bws.getBodyId(),
                            "dataset", dataset)));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSynapseSets: exit");
    }

    /**
     * Adds Skeleton and SkelNode nodes to database. Segments are connected to Skeletons via Contains relationships.
     * Skeletons are connected to SkelNodes via Contains relationships. SkelNodes point to their children with LinksTo
     * relationships.
     *
     * @param dataset      dataset name
     * @param skeletonList list of {@link Skeleton} objects
     */
    public void addSkeletonNodes(final String dataset, final List<Skeleton> skeletonList) {

        LOG.info("addSkeletonNodes: entry");

        final String segmentToSkeletonConnectionString = "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n.status=$notAnnotated, n.timeStamp=$timeStamp, n:Segment, n:" + dataset + " \n" +
                "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp, r:Skeleton, r:" + dataset + " \n" +
                "MERGE (n)-[:Contains]->(r) \n";

        final String rootNodeString = "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp, r:Skeleton, r:" + dataset + " \n" +
                "MERGE (s:`" + dataset + "-SkelNode`{skelNodeId:$skelNodeId}) ON CREATE SET s.skelNodeId=$skelNodeId, s.location=$location, s.radius=$radius, s.rowNumber=$rowNumber, s.type=$type, s.timeStamp=$timeStamp, s:SkelNode, s:" + dataset + " \n" +
                "MERGE (r)-[:Contains]->(s) \n";

        final String parentNodeString = "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.timeStamp=$timeStamp, r:Skeleton, r:" + dataset + " \n" +
                "MERGE (p:`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.rowNumber=$pRowNumber, p.type=$pType, p.timeStamp=$timeStamp, p:SkelNode, p:" + dataset + " \n" +
                "MERGE (r)-[:Contains]->(p) ";

        final String childNodeString = "MERGE (p:`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.rowNumber=$pRowNumber, p.type=$pType, p.timeStamp=$timeStamp, p:SkelNode, p:" + dataset + " \n" +
                "MERGE (c:`" + dataset + "-SkelNode`{skelNodeId:$childNodeId}) ON CREATE SET c.skelNodeId=$childNodeId, c.location=$childLocation, c.radius=$childRadius, c.rowNumber=$childRowNumber, c.type=$childType, c.timeStamp=$timeStamp, c:SkelNode, c:" + dataset + " \n" +
                "MERGE (p)-[:LinksTo]-(c)";

        try (final TransactionBatch batch = getBatch()) {
            for (Skeleton skeleton : skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                batch.addStatement(new Statement(segmentToSkeletonConnectionString, parameters("bodyId", associatedBodyId,
                        "notAnnotated", "not annotated",
                        "skeletonId", dataset + ":" + associatedBodyId,
                        "timeStamp", timeStamp
                )));

                for (SkelNode skelNode : skelNodeList) {

                    if (skelNode.getParent() == null) {
                        batch.addStatement(new Statement(rootNodeString, parameters(
                                "location", skelNode.getLocationAsPoint(),
                                "radius", skelNode.getRadius(),
                                "skeletonId", dataset + ":" + associatedBodyId,
                                "skelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                                "rowNumber", skelNode.getRowNumber(),
                                "type", skelNode.getType(),
                                "timeStamp", timeStamp
                        )));
                    }

                    batch.addStatement(new Statement(parentNodeString, parameters(
                            "pLocation", skelNode.getLocationAsPoint(),
                            "pRadius", skelNode.getRadius(),
                            "skeletonId", dataset + ":" + associatedBodyId,
                            "parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                            "pRowNumber", skelNode.getRowNumber(),
                            "pType", skelNode.getType(),
                            "timeStamp", timeStamp
                    )));

                    for (SkelNode child : skelNode.getChildren()) {
                        String childNodeId = dataset + ":" + associatedBodyId + ":" + child.getLocationString();
                        batch.addStatement(new Statement(childNodeString, parameters("parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                                "skeletonId", dataset + ":" + associatedBodyId,
                                "pLocation", skelNode.getLocationAsPoint(),
                                "pRadius", skelNode.getRadius(),
                                "pRowNumber", skelNode.getRowNumber(),
                                "pType", skelNode.getType(),
                                "timeStamp", timeStamp,
                                "childNodeId", childNodeId,
                                "childLocation", child.getLocationAsPoint(),
                                "childRadius", child.getRadius(),
                                "childRowNumber", child.getRowNumber(),
                                "childType", child.getType()
                        )));
                    }
                }
                LOG.info("Added full skeleton for bodyId: " + skeleton.getAssociatedBodyId());
            }
            batch.writeTransaction();
        }
        LOG.info("addSkeletonNodes: exit");
    }

    /**
     * Adds a Meta node and DataModel node to the database (if they do not exist). The Meta node stores summary information for
     * a given dataset. The DataModel node indicates the data model version and links to all Meta nodes in the database with
     * an Is relationship.
     *
     * @param dataset          dataset name
     * @param dataModelVersion version of data model
     */
    public void createMetaNodeWithDataModelNode(final String dataset, final float dataModelVersion) {

        LOG.info("createMetaNodeWithDataModelNode: enter");

        final String metaNodeString = "MERGE (m:Meta{dataset:$dataset}) ON CREATE SET " +
                "m:" + dataset + "," +
                "m.lastDatabaseEdit=$timeStamp," +
                "m.dataset=$dataset, " +
                "m.totalPreCount=$totalPre, " +
                "m.totalPostCount=$totalPost \n" +
                "MERGE (d:DataModel{dataModelVersion:$dataModelVersion}) ON CREATE SET d.dataModelVersion=$dataModelVersion, d.timeStamp=$timeStamp \n" +
                "MERGE (m)-[:Is]->(d)";

        long totalPre;
        long totalPost;
        Set<String> roiNameSet;
        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();

        try (Session session = driver.session()) {
            totalPre = session.readTransaction(tx -> getTotalPreCount(tx, dataset));
            totalPost = session.readTransaction(tx -> getTotalPostCount(tx, dataset));
            roiNameSet = getRoiSet(session, dataset);
            for (String roi : roiNameSet) {
                int roiPreCount = session.readTransaction(tx -> getRoiPreCount(tx, dataset, roi));
                int roiPostCount = session.readTransaction(tx -> getRoiPostCount(tx, dataset, roi));
                synapseCountsPerRoi.addSynapseCountsForRoi(roi, roiPreCount, roiPostCount);
            }
        }

        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(metaNodeString, parameters("dataset", dataset,
                    "totalPre", totalPre,
                    "totalPost", totalPost,
                    "timeStamp", timeStamp,
                    "dataModelVersion", dataModelVersion
            )));

            String metaNodeRoiString = "MATCH (m:Meta{dataset:$dataset}) SET m.roiInfo=$synapseCountPerRoi ";

            batch.addStatement(new Statement(metaNodeRoiString,
                    parameters("dataset", dataset,
                            "synapseCountPerRoi", synapseCountsPerRoi.getAsJsonString()
                    )));

            batch.writeTransaction();

        }

        LOG.info("createMetaNodeWithDataModelNode: exit");

    }

    public void indexBooleanRoiProperties(String dataset) {

        LOG.info("indexBooleanRoiProperties: entry");

        Set<String> roiNameSet;
        try (Session session = driver.session()) {
            roiNameSet = getRoiSet(session, dataset);
        }

        String[] indexTextArray = new String[roiNameSet.size()];
        int i = 0;
        for (String roi : roiNameSet) {
            indexTextArray[i] = "CREATE INDEX ON :`" + dataset + "-Neuron`(`" + roi + "`)";
            i++;
        }

        for (final String indexText : indexTextArray) {
            try (final TransactionBatch batch = getBatch()) {
                batch.addStatement(new Statement(indexText));
                batch.writeTransaction();
            }
        }
        LOG.info("indexBooleanRoiProperties: exit");

    }

    private Set<String> getRoiSet(Session session, String dataset) {

        Set<String> roiNameSet;

        roiNameSet = session.readTransaction(tx -> getAllProperties(tx, dataset))
                .stream()
                .filter((p) -> (
                        !p.equals("autoName") &&
                                !p.equals("bodyId") &&
                                !p.equals("name") &&
                                !p.equals("post") &&
                                !p.equals("pre") &&
                                !p.equals("size") &&
                                !p.equals("status") &&
                                !p.equals("roiInfo") &&
                                !p.equals("timeStamp") &&
                                !p.equals("type")) &&
                        !p.equals("somaLocation") &&
                        !p.equals("somaRadius"))
                .collect(Collectors.toSet());

        return roiNameSet;

    }

    private static long getTotalPreCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`) RETURN sum(n.pre)");
        return result.single().get(0).asLong();
    }

    private static long getTotalPostCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`) RETURN sum(n.post)");
        return result.single().get(0).asLong();
    }

    private static int getRoiPreCount(final Transaction tx, final String dataset, String roi) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`{`" + roi + "`:true}) WITH apoc.convert.fromJsonMap(n.roiInfo).`" + roi + "`.pre AS pre RETURN sum(pre)");
        return result.single().get(0).asInt();
    }

    private static int getRoiPostCount(final Transaction tx, final String dataset, String roi) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`{`" + roi + "`:true}) WITH apoc.convert.fromJsonMap(n.roiInfo).`" + roi + "`.post AS post RETURN sum(post)");
        return result.single().get(0).asInt();
    }

    private static Set<String> getAllProperties(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`) WITH keys(n) AS props UNWIND props AS prop WITH DISTINCT prop ORDER BY prop RETURN prop");
        Set<String> roiSet = new HashSet<>();
        while (result.hasNext()) {
            roiSet.add(result.next().asMap().get("prop").toString());
        }
        return roiSet;
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

    private static List<Long> getAllSegmentBodyIdsWithGreaterThanThresholdSynapses(final Transaction tx, final String dataset, final int synapseThreshold) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`) WHERE (n.pre+n.post)>" + synapseThreshold + " RETURN n.bodyId ");
        List<Long> bodyIdList = new ArrayList<>();
        while (result.hasNext()) {
            bodyIdList.add((Long) result.next().asMap().get("n.bodyId"));
        }
        return bodyIdList;
    }

    private static List<Long> getAllSegmentBodyIdsWithGreaterThanThresholdSynapsesAndWithoutNames(final Transaction tx, final String dataset, final int synapseThreshold) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Segment`) WHERE (n.pre+n.post)>" + synapseThreshold + " AND (NOT exists(n.name) OR n.name=\"unknown\") RETURN n.bodyId ");
        List<Long> bodyIdList = new ArrayList<>();
        while (result.hasNext()) {
            bodyIdList.add((Long) result.next().asMap().get("n.bodyId"));
        }
        return bodyIdList;
    }

    private static SortedSet<Map.Entry<String, SynapseCounter>> entriesSortedByComparator(Map<String, SynapseCounter> map, Comparator<Map.Entry<String, SynapseCounter>> comparator) {
        SortedSet<Map.Entry<String, SynapseCounter>> sortedEntries = new TreeSet<>(comparator);
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    private static SortedSet<Map.Entry<String, SynapseCounter>> sortRoisByPostCount(Map<String, SynapseCounter> roiSynapseCountMap) {
        Comparator<Map.Entry<String, SynapseCounter>> comparator = (e1, e2) ->
                e1.getValue().getPost() == e2.getValue().getPost() ? e1.getKey().compareTo(e2.getKey()) : e2.getValue().getPost() - e1.getValue().getPost();
        return entriesSortedByComparator(roiSynapseCountMap, comparator);
    }

    private static SortedSet<Map.Entry<String, SynapseCounter>> sortRoisByPreCount(Map<String, SynapseCounter> roiSynapseCountMap) {
        Comparator<Map.Entry<String, SynapseCounter>> comparator = (e1, e2) ->
                e1.getValue().getPre() == e2.getValue().getPre() ? e1.getKey().compareTo(e2.getKey()) : e2.getValue().getPre() - e1.getValue().getPre();
        return entriesSortedByComparator(roiSynapseCountMap, comparator);
    }

    private static final Logger LOG = LoggerFactory.getLogger(Neo4jImporter.class);
}