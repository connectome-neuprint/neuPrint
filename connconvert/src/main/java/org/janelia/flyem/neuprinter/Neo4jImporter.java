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
import org.janelia.flyem.neuprinter.model.NeuronType;
import org.janelia.flyem.neuprinter.model.NeuronTypeTree;
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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporter implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;
    private final LocalDate timeStamp = LocalDate.now();

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

    //for testing
    public Neo4jImporter(final Driver driver) {
        this.driver = driver;
        this.statementsPerTransaction = 20;
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

    public void prepDatabase(String dataset) {
        //TODO: add dataset id  as prefix to all labels
        //TODO: synapse uniqueness is on location so can get rid of datasetlocation
        LOG.info("prepDatabase: entry");
        final String[] prepTextArray = {
                "CREATE CONSTRAINT ON (n:`" + dataset + "-Neuron`) ASSERT n.bodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-SynapseSet`) ASSERT s.datasetBodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-Synapse`) ASSERT s.location IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-SkelNode`) ASSERT s.skelNodeId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:`" + dataset + "-Skeleton`) ASSERT s.skeletonId IS UNIQUE",
                "CREATE CONSTRAINT ON (m:Meta) ASSERT m.dataset IS UNIQUE",
                "CREATE CONSTRAINT ON (n:" + dataset + ") ASSERT n.autoName is UNIQUE",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(status)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(somaLocation)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(name)",
                "CREATE INDEX ON :`" + dataset + "-SkelNode`(location)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(pre)",
                "CREATE INDEX ON :`" + dataset + "-Neuron`(post)"
        };

        for (final String prepText : prepTextArray) {
            try (final TransactionBatch batch = getBatch()) {
                batch.addStatement(new Statement(prepText));
                batch.writeTransaction();
            }
        }

        LOG.info("prepDatabase: exit");

    }

    //TODO: arbitrary properties
    public void addNeurons(final String dataset,
                           final List<Neuron> neuronList) {

        final String neuronText = "MERGE (n:Neuron:" + dataset + ":`" + dataset + "-Neuron`{bodyId:$bodyId}) " +
                "ON CREATE SET n.bodyId = $bodyId," +
                " n.name = $name," +
                " n.type = $type," +
                " n.status = $status," +
                " n.size = $size," +
                " n.somaLocation = $somaLocation," +
                " n.somaRadius = $somaRadius, " +
                " n.timeStamp = $timeStamp \n" +
                " WITH n \n" +
                " CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
                " RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {
                String status = neuron.getStatus() != null ? neuron.getStatus() : "not annotated";
                batch.addStatement(
                        new Statement(neuronText,
                                parameters("bodyId", neuron.getId(),
                                        "name", neuron.getName(),
                                        "type", neuron.getNeuronType(),
                                        "status", status,
                                        "size", neuron.getSize(),
                                        "somaLocation", neuron.getSomaLocation(),
                                        "somaRadius", neuron.getSomaRadius(),
                                        "timeStamp", timeStamp,
                                        "rois", neuron.getRoisWithAndWithoutDatasetPrefix(dataset)))
                );
            }
            batch.writeTransaction();
        }

    }

    public void addConnectsTo(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addConnectsTo: entry");

        final String connectsToText =
                "MERGE (n:Neuron:" + dataset + ":`" + dataset + "-Neuron`{bodyId:$bodyId1}) ON CREATE SET n.bodyId = $bodyId1, n.status=$notAnnotated\n" +
                        "MERGE (m:Neuron:" + dataset + ":`" + dataset + "-Neuron`{bodyId:$bodyId2}) ON CREATE SET m.bodyId = $bodyId2, m.timeStamp=$timeStamp, m.status=$notAnnotated \n" +
                        "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m)";
        final String terminalCountText = "MATCH (n:Neuron:" + dataset + " {bodyId:$bodyId} ) SET n.pre = $pre, n.post = $post, n.timeStamp=$timeStamp, n.synapseCountPerRoi=$synapseCountPerRoi";

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

    public void addSynapsesWithRois(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSynapses: entry");

        final String preSynapseText =
                "MERGE (s:Synapse:PreSyn:" + dataset + ":`" + dataset + "-Synapse`:`" + dataset + "-PreSyn`{location:$location}) " +
                        " ON CREATE SET s.location=$location, " +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        " s.timeStamp=$timeStamp \n" +
                        " WITH s \n" +
                        " CALL apoc.create.addLabels(id(s),$rois) YIELD node \n" +
                        " RETURN node";

        final String postSynapseText =
                "MERGE (s:Synapse:PostSyn:" + dataset + ":`" + dataset + "-Synapse`:`" + dataset + "-PostSyn`{location:$location}) " +
                        " ON CREATE SET s.location=$location, " +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        " s.timeStamp=$timeStamp \n" +
                        " WITH s \n" +
                        " CALL apoc.create.addLabels(id(s),$rois) YIELD node \n" +
                        " RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses bws : bodyList) {
                // issue with this body id in mb6
                if (bws.getBodyId() != 304654117 || !(dataset.equals("mb6v2") || dataset.equals("mb6"))) {
                    for (final Synapse synapse : bws.getSynapseSet()) {
                        if (synapse.getType().equals("pre")) {

                            batch.addStatement(new Statement(
                                    preSynapseText,
                                    parameters("location", synapse.getLocationAsPoint(),
                                            "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                            "confidence", synapse.getConfidence(),
                                            "type", synapse.getType(),
                                            "timeStamp", timeStamp,
                                            "rois", synapse.getRoiPtsWithAndWithoutDatasetPrefix(dataset)))
                            );
                        } else if (synapse.getType().equals("post")) {
                            batch.addStatement(new Statement(
                                    postSynapseText,
                                    parameters("location", synapse.getLocationAsPoint(),
                                            "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                            "confidence", synapse.getConfidence(),
                                            "type", synapse.getType(),
                                            "timeStamp", timeStamp,
                                            "rois", synapse.getRoiPtsWithAndWithoutDatasetPrefix(dataset)))
                            );

                        }
                    }
                }
            }
            batch.writeTransaction();
        }
        LOG.info("addSynapses: exit");
    }

    public void addSynapsesTo(final String dataset, HashMap<String, List<String>> preToPost) {

        LOG.info("addSynapsesTo: entry");

        final String synapseRelationsText = "MERGE (s:Synapse:" + dataset + ":`" + dataset + "-Synapse`{location:$prelocation}) ON CREATE SET s.location = $prelocation, s:createdforsynapsesto, s.timeStamp=$timeStamp \n" +
                "MERGE (t:Synapse:" + dataset + ":`" + dataset + "-Synapse`{location:$postlocation}) ON CREATE SET t.location = $postlocation, t:createdforsynapsesto, t.timeStamp=$timeStamp \n" +
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

    public void addNeuronRois(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addNeuronRois: entry");

        final String roiNeuronText = "MERGE (n:Neuron:" + dataset + ":`" + dataset + "-Neuron`{bodyId:$bodyId}) ON CREATE SET n.bodyId = $bodyId, n.timeStamp=$timeStamp, n.status=$notAnnotated \n" +
                "WITH n \n" +
                "CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (Synapse synapse : bws.getSynapseSet()) {
                    batch.addStatement(new Statement(roiNeuronText, parameters("bodyId", bws.getBodyId(),
                            "timeStamp", timeStamp,
                            "notAnnotated", "not annotated",
                            "rois", synapse.getRoisWithAndWithoutDatasetPrefix(dataset))));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addNeuronRois: exit");

    }

    public void addAutoNames(final String dataset, int autoNameThreshold) {

        LOG.info("addAutoNames: entry");

        List<AutoName> autoNameList = new ArrayList<>();
        List<Long> bodyIdsWithoutNames;
        final String autoNameText = "MATCH (n:`" + dataset + "-Neuron`{bodyId:$bodyId}) SET n.autoName=$autoName";
        final String autoNameToNameText = "MATCH (n:`" + dataset + "-Neuron`{bodyId:$bodyId}) SET n.autoName=$autoName, n.name=$autoName ";

        try (Session session = driver.session()) {

            // get body ids for generating auto-name
            List<Long> bodyIdList = session.readTransaction(tx -> getAllNeuronBodyIdsWithGreaterThanThresholdSynapses(tx, dataset, autoNameThreshold));
            for (Long bodyId : bodyIdList) {
                String maxPostRoiName = session.readTransaction(tx -> getMaxInputRoi(tx, dataset, bodyId));
                String maxPreRoiName = session.readTransaction(tx -> getMaxOutputRoi(tx, dataset, bodyId));
                AutoName autoName = new AutoName(maxPostRoiName, maxPreRoiName, bodyId);
                autoNameList.add(autoName);
            }
            // get body ids above threshold without names
            bodyIdsWithoutNames = session.readTransaction(tx -> getAllNeuronBodyIdsWithGreaterThanThresholdSynapsesAndWithoutNames(tx, dataset, autoNameThreshold));
        }

        try (final TransactionBatch batch = getBatch()) {
            for (AutoName autoName : autoNameList) {
                Long bodyId = autoName.getBodyId();
                if (bodyIdsWithoutNames.contains(bodyId)) {
                    batch.addStatement(new Statement(autoNameToNameText,
                            parameters("bodyId", autoName.getBodyId(),
                                    "autoName", autoName.getAutoName())));
                } else {
                    batch.addStatement(new Statement(autoNameText,
                            parameters("bodyId", autoName.getBodyId(),
                                    "autoName", autoName.getAutoName())));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addAutoNames: exit");
    }

    public void addSynapseSets(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSynapseSets: entry");

        final String neuronContainsSSText = "MERGE (n:Neuron:" + dataset + ":`" + dataset + "-Neuron`{bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n.status=$notAnnotated \n" +
                "MERGE (s:SynapseSet:" + dataset + ":`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId}) ON CREATE SET s.datasetBodyId=$datasetBodyId, s.timeStamp=$timeStamp \n" +
                "MERGE (n)-[:Contains]->(s)";

        final String ssContainsSynapseText = "MERGE (s:Synapse:" + dataset + ":`" + dataset + "-Synapse`{location:$location}) ON CREATE SET s.location=$location \n" +
                "MERGE (t:SynapseSet:" + dataset + ":`" + dataset + "-SynapseSet`{datasetBodyId:$datasetBodyId}) ON CREATE SET t.datasetBodyId=$datasetBodyId \n" +
                "MERGE (t)-[:Contains]->(s) \n";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                batch.addStatement(new Statement(neuronContainsSSText, parameters("bodyId", bws.getBodyId(),
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

    public void addSkeletonNodes(final String dataset, final List<Skeleton> skeletonList) {

        LOG.info("addSkeletonNodes: entry");

        final String neuronToSkeletonConnectionString = "MERGE (n:Neuron:" + dataset + ":`" + dataset + "-Neuron`{bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n.status=$notAnnotated \n" +
                "MERGE (r:Skeleton:" + dataset + ":`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp \n" +
                "MERGE (n)-[:Contains]->(r) \n";

        final String rootNodeString = "MERGE (r:Skeleton:" + dataset + ":`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp \n" +
                "MERGE (s:SkelNode:" + dataset + ":`" + dataset + "-SkelNode`{skelNodeId:$skelNodeId}) ON CREATE SET s.skelNodeId=$skelNodeId, s.location=$location, s.radius=$radius, s.rowNumber=$rowNumber, s.type=$type \n" +
                "MERGE (r)-[:Contains]->(s) \n";

        final String parentNodeString = "MERGE (r:Skeleton:" + dataset + ":`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.timeStamp=$timeStamp \n" +
                "MERGE (p:SkelNode:" + dataset + ":`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.rowNumber=$pRowNumber, p.type=$pType \n" +
                "MERGE (r)-[:Contains]->(p) ";

        final String childNodeString = "MERGE (p:SkelNode:" + dataset + ":`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.rowNumber=$pRowNumber, p.type=$pType \n" +
                "MERGE (c:SkelNode:" + dataset + ":`" + dataset + "-SkelNode`{skelNodeId:$childNodeId}) ON CREATE SET c.skelNodeId=$childNodeId, c.location=$childLocation, c.radius=$childRadius, c.rowNumber=$childRowNumber, c.type=$childType \n" +
                "MERGE (p)-[:LinksTo]-(c)";

        try (final TransactionBatch batch = getBatch()) {
            for (Skeleton skeleton : skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                batch.addStatement(new Statement(neuronToSkeletonConnectionString, parameters("bodyId", associatedBodyId,
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

    public void addCellTypeTree(final String dataset, final HashMap<String, NeuronTypeTree> neuronTypeTreeMap) {

        LOG.info("addCellTypeTree: enter");

        final String cellTypeTreeString = "MERGE (nc:NeuronClass:" + dataset + " {neuronClassId:$neuronClassId}) ON CREATE SET nc.neuronClassId=$neuronClassId, nc.neuronClass=$neuronClass, nc.timeStamp=$timeStamp \n" +
                "MERGE (nt:NeuronType:" + dataset + "{neuronTypeId:$neuronTypeId}) ON CREATE SET nt.neuronTypeId=$neuronTypeId, nt.neuronType=$neuronType, nt.description=$description, nt.putativeNeurotransmitter=$neurotransmitter, nt.timeStamp=$timeStamp \n" +
                "MERGE (nc)-[:Contains]->(nt)";

        try (final TransactionBatch batch = getBatch()) {

            for (String neuronClass : neuronTypeTreeMap.keySet()) {
                for (NeuronType neuronType : neuronTypeTreeMap.get(neuronClass).getNeuronTypeList()) {

                    batch.addStatement(new Statement(cellTypeTreeString, parameters("neuronClassId", dataset + neuronClass,
                            "neuronClass", neuronClass,
                            "neuronType", neuronType.getCellType(),
                            "neuronTypeId", dataset + neuronType.getCellType(),
                            "description", neuronType.getCellDescription(),
                            "neurotransmitter", neuronType.getPutativeTransmitter(),
                            "timeStamp", timeStamp
                    )));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addCellTypeTree: exit");

    }

    public void createMetaNode(final String dataset) {

        LOG.info("createMetaNode: enter");

        final String metaNodeString = "MERGE (m:Meta:" + dataset + " {dataset:$dataset}) ON CREATE SET m.lastDatabaseEdit=$timeStamp," +
                "m.dataset=$dataset, m.totalPreCount=$totalPre, m.totalPostCount=$totalPost";

        long totalPre;
        long totalPost;
        List<String> roiNameList;
        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();
        //TODO: change the many properties to a json
        //TODO: dataset node that points to all dataset metas. has a version of the datamodel (1.0)
        try (Session session = driver.session()) {
            totalPre = session.readTransaction(tx -> getTotalPreCount(tx, dataset));
            totalPost = session.readTransaction(tx -> getTotalPostCount(tx, dataset));
            roiNameList = session.readTransaction(tx -> getAllRois(tx, dataset))
                    .stream()
                    .filter((l) -> (!l.equals("Neuron") && !l.startsWith(dataset)))
                    .collect(Collectors.toList());
            for (String roi : roiNameList) {
                int roiPreCount = session.readTransaction(tx -> getRoiPreCount(tx, dataset, roi));
                int roiPostCount = session.readTransaction(tx -> getRoiPostCount(tx, dataset, roi));
                synapseCountsPerRoi.addSynapseCountsForRoi(roi, roiPreCount, roiPostCount);
            }
        }

        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(metaNodeString, parameters("dataset", dataset,
                    "totalPre", totalPre,
                    "totalPost", totalPost,
                    "timeStamp", timeStamp
//                    "rois", roiNameList
//                            .stream()
//                            .filter((l) -> (!l.equals("seven_column_roi") && !l.equals("kc_alpha_roi")))
//                            .collect(Collectors.toList())
                    //TODO: might be able to get rid of roi list since can get from keys on json for meta node
            )));

            String metaNodeRoiString = "MATCH (m:Meta:" + dataset + " {dataset:$dataset}) SET m.synapseCountPerRoi=$synapseCountPerRoi ";

            batch.addStatement(new Statement(metaNodeRoiString,
                    parameters("dataset", dataset,
                            "synapseCountPerRoi", synapseCountsPerRoi.getAsJsonString()
                    )));

            batch.writeTransaction();

        }

        LOG.info("createMetaNode: exit");

    }

    private static long getTotalPreCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`) RETURN sum(n.pre)");
        return result.single().get(0).asLong();
    }

    private static long getTotalPostCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`) RETURN sum(n.post)");
        return result.single().get(0).asLong();
    }

    private static int getRoiPreCount(final Transaction tx, final String dataset, final String roi) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`:`" + dataset + "-" + roi + "`) WITH apoc.convert.fromJsonMap(n.synapseCountPerRoi).`" + roi + "`.pre AS pre RETURN sum(pre)");
        return result.single().get(0).asInt();
    }

    private static int getRoiPostCount(final Transaction tx, final String dataset, final String roi) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`:`" + dataset + "-" + roi + "`) WITH apoc.convert.fromJsonMap(n.synapseCountPerRoi).`" + roi + "`.post AS post RETURN sum(post)");
        return result.single().get(0).asInt();
    }

    private static List<String> getAllRois(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`) WITH labels(n) AS labels UNWIND labels AS label WITH DISTINCT label ORDER BY label RETURN label");
        List<String> roiList = new ArrayList<>();
        while (result.hasNext()) {
            roiList.add(result.next().asMap().get("label").toString());
        }
        return roiList;
    }

    private static String getMaxInputRoi(final Transaction tx, final String dataset, Long bodyId) {

        Gson gson = new Gson();
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`{bodyId:$bodyId}) WITH n.synapseCountPerRoi AS roiJson RETURN roiJson", parameters("bodyId", bodyId));

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
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`{bodyId:$bodyId}) WITH n.synapseCountPerRoi AS roiJson RETURN roiJson", parameters("bodyId", bodyId));

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

    private static List<Long> getAllNeuronBodyIdsWithGreaterThanThresholdSynapses(final Transaction tx, final String dataset, final int synapseThreshold) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`) WHERE (n.pre+n.post)>" + synapseThreshold + " RETURN n.bodyId ");
        List<Long> bodyIdList = new ArrayList<>();
        while (result.hasNext()) {
            bodyIdList.add((Long) result.next().asMap().get("n.bodyId"));
        }
        return bodyIdList;
    }

    private static List<Long> getAllNeuronBodyIdsWithGreaterThanThresholdSynapsesAndWithoutNames(final Transaction tx, final String dataset, final int synapseThreshold) {
        StatementResult result = tx.run("MATCH (n:`" + dataset + "-Neuron`) WHERE (n.pre+n.post)>" + synapseThreshold + " AND (NOT exists(n.name) OR n.name=\"unknown\") RETURN n.bodyId ");
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