package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.result.NodeResult;
import com.google.gson.Gson;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.SkelNode;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.Synapse;
import org.janelia.flyem.neuprinter.model.SynapseCountsPerRoi;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//TODO: get rid of history nodes? or replace with something else
public class ProofreaderProcedures {

    //Node names
    private static final String META = "Meta";
    private static final String NEURON = "Neuron";
    private static final String SEGMENT = "Segment";
    private static final String SKELETON = "Skeleton";
    private static final String SYNAPSE = "Synapse";
    private static final String CONNECTION_SET = "ConnectionSet";
    private static final String SYNAPSE_SET = "SynapseSet";
    private static final String SYNAPSE_STORE = "SynapseStore";
    private static final String POST_SYN = "PostSyn";
    private static final String PRE_SYN = "PreSyn";
    //Property names
    private static final String BODY_ID = "bodyId";
    private static final String CONFIDENCE = "confidence";
    private static final String DATASET = "dataset";
    private static final String DATASET_BODY_ID = "datasetBodyId";
    private static final String DATASET_BODY_IDs = "datasetBodyIds";
    private static final String LOCATION = "location";
    private static final String MERGED_BODY_ID = "mergedBodyId";
    private static final String POST = "post";
    private static final String PRE = "pre";
    private static final String SIZE = "size";
    private static final String NAME = "name";
    private static final String STATUS = "status";
    private static final String ROI_INFO = "roiInfo";
    private static final String TIME_STAMP = "timeStamp";
    private static final String TYPE = "type";
    private static final String WEIGHT = "weight";
    private static final String SOMA_LOCATION = "somaLocation";
    private static final String SOMA_RADIUS = "somaRadius";
    private static final String MUTATION_UUID_ID = "mutationUuidAndId";
    //Relationship names
    private static final String CLEAVED_TO = "CleavedTo";
    private static final String CONNECTS_TO = "ConnectsTo";
    private static final String CONTAINS = "Contains";
    private static final String FROM = "From";
    private static final String LINKS_TO = "LinksTo";
    private static final String MERGED_TO = "MergedTo";
    private static final String SPLIT_TO = "SplitTo";
    private static final String SYNAPSES_TO = "SynapsesTo";
    private static final String HAS = "Has";
    //prefixes on History Node properties
    private static final String STORED = "stored";
    private static final String CLEAVED = "cleaved";
    private static final String MERGED = "merged";
    private static final String SPLIT = "split";

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

//    @Procedure(value = "proofreader.updateNeuronsFromJson", mode = Mode.WRITE)
//    @Description("proofreader.updateNeuronsFromJson")
//    public Stream<NodeResult> updateNeuronsFromJson(@Name("updateNeuronsFilePathOrJson") String updateNeuronsFilePathOrJson, @Name("datasetLabel") String datasetLabel, @Name(value = "isFilePath", defaultValue = "false") boolean isFilePath) { //@Name("updateNeuronsJson") String updateNeuronsJson
//        // TODO : update meta node with new dvid info
//        if (updateNeuronsFilePathOrJson == null || datasetLabel == null) {
//            log.error("proofreader.updateNeuronsFromJson: Missing input arguments.");
//            throw new RuntimeException("Missing input arguments.");
//        }
//
//        Gson gson = new Gson();
//
//        UpdateNeuronsAction updateNeuronsAction;
//        if (isFilePath) {
//            try (BufferedReader reader = new BufferedReader(new FileReader(updateNeuronsFilePathOrJson))) {
//                updateNeuronsAction = gson.fromJson(reader, UpdateNeuronsAction.class);
//            } catch (Exception e) {
//                e.printStackTrace();
//                throw new RuntimeException("Error reading file");
//            }
//        } else {
//            updateNeuronsAction = gson.fromJson(updateNeuronsFilePathOrJson, UpdateNeuronsAction.class);
//        }
//
//        // deletion phase : for each neuron
//
//        // delete synapse sets and delete ConnectionSets
//        for (Long deletedBodyId : updateNeuronsAction.getDeletedNeurons()) {
//                deleteSegment(deletedBodyId, datasetLabel);
//                System.out.println("complete transaction.");
//        }
//
//        // check that this mutation hasn't been done before
//        String mutationKey;
//        for (NeuronUpdate neuronUpdate : updateNeuronsAction.getUpdatedNeurons()) {
//            mutationKey = neuronUpdate.getMutationUuid() + ":" + neuronUpdate.getMutationId();
//                Node existingMutatedNode = dbService.findNode(Label.label(datasetLabel + "-" + SEGMENT), MUTATION_UUID_ID, mutationKey);
//                if (existingMutatedNode != null) {
//                    log.error("Mutation already found in the database: " + neuronUpdate.toString());
//                    throw new RuntimeException("Mutation already found in the database: " + neuronUpdate.toString());
//            }
//
//        }
//
//        for (Long updatedNeuronBodyIds : updateNeuronsAction.getUpdatedNeuronsBodyIds()) {
//                deleteSegment(updatedNeuronBodyIds, datasetLabel);
//        }
//
//        for (NeuronUpdate neuronUpdate : updateNeuronsAction.getUpdatedNeurons()) {
//
//            // create a new node and synapse set for that node
//
////            try (Transaction tx = dbService.beginTx()) {
//
//                final long newNeuronBodyId = neuronUpdate.getBodyId();
//                final Node newNeuron = dbService.createNode(Label.label(SEGMENT),
//                        Label.label(datasetLabel),
//                        Label.label(datasetLabel + "-" + SEGMENT));
//                newNeuron.setProperty(BODY_ID, newNeuronBodyId);
//                final Node newSynapseSet = createSynapseSetForSegment(newNeuron, datasetLabel);
//
//                // add appropriate synapses via synapse sets; add each synapse to the new body's synapseset
//                // completely add everything here so that there's nothing left on the synapse store
//                Set<Long> synapseSources = neuronUpdate.getSynapseSources();
//                // add the new body id to the synapse sources
//                synapseSources.add(newNeuronBodyId);
//                Set<Synapse> currentSynapses = neuronUpdate.getCurrentSynapses();
//                Set<Synapse> notFoundSynapses = new HashSet<>(currentSynapses);
//
//                for (Long synapseSource : synapseSources) {
//                    Node sourceSynapseSet = dbService.findNode(Label.label(SYNAPSE_SET), DATASET_BODY_ID, STORED + ":" + datasetLabel + ":" + synapseSource);
//                    if (sourceSynapseSet != null) {
//                        lookForSynapsesOnSynapseSet(newSynapseSet, sourceSynapseSet, currentSynapses, notFoundSynapses);
//                    } else {
//                        log.info("No synapse set found for source: " + datasetLabel + ":" + synapseSource);
//                    }
//                }
//
//                // if not all synapses were found, go through all possible synapse sets
//                if (!notFoundSynapses.isEmpty()) {
//                    log.warn("Not all synapses were found on the synapse sources for neuron update. Mutation UUID: " + neuronUpdate.getMutationUuid() + " Mutation ID: " + neuronUpdate.getMutationId() + " Synapse(s): " + notFoundSynapses);
//                    if (synapseStore.hasRelationship(RelationshipType.withName(HAS))) {
//                        for (Relationship hasRel : synapseStore.getRelationships(RelationshipType.withName(HAS), Direction.OUTGOING)) {
//                            Node synapseSet = hasRel.getEndNode();
//                            // don't look at synapse sets that have already been checked
//                            String datasetBodyId = (String) synapseSet.getProperty(DATASET_BODY_ID);
//                            String[] splitDatasetBodyId = datasetBodyId.split(":");
//                            if (!synapseSources.contains(Long.parseLong(splitDatasetBodyId[2]))) {
//                                lookForSynapsesOnSynapseSet(newSynapseSet, synapseSet, notFoundSynapses, notFoundSynapses);
//                            }
//                        }
//                    }
//                }
//
//                if (!notFoundSynapses.isEmpty()) {
//                    String errorMessage = "Some synapses were not found on the synapse store for neuron update. Mutation UUID: " + neuronUpdate.getMutationUuid() + " Mutation ID: " + neuronUpdate.getMutationId() + " Synapse(s): " + notFoundSynapses;
//                    log.error(errorMessage);
//                    throw new RuntimeException(errorMessage);
//                }
//
//                // check if any synapse sets are empty, if so can delete them
//                if (synapseStore.hasRelationship(RelationshipType.withName(HAS), Direction.OUTGOING)) {
//                    for (Relationship hasRel : synapseStore.getRelationships(RelationshipType.withName(HAS), Direction.OUTGOING)) {
//                        Node synapseSet = hasRel.getEndNode();
//                        if (!synapseSet.hasRelationship(RelationshipType.withName(CONTAINS))) {
//                            hasRel.delete();
//                            synapseSet.delete();
//                        }
//                    }
//                }
//
//                // from synapses, derive connectsto, connection sets, rois/roiInfo, pre/post counts
//                final ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
//                final SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();
//                long preCount = 0L;
//                long postCount = 0L;
//
//                if (newSynapseSet.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
//
//                    for (Relationship synapseContainsRel : newSynapseSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
//                        // get the synapse
//                        final Node synapseNode = synapseContainsRel.getEndNode();
//                        // get the synapse type
//                        final String synapseType = (String) synapseNode.getProperty(TYPE);
//                        // get synapse rois for adding to the body and roiInfo
//                        final List<String> synapseRois = getSynapseNodeRoiList(synapseNode);
//
//                        if (synapseType.equals(PRE)) {
//                            for (String roi : synapseRois) {
//                                synapseCountsPerRoi.incrementPreForRoi(roi);
//                            }
//                            preCount++;
//                        } else if (synapseType.equals(POST)) {
//                            for (String roi : synapseRois) {
//                                synapseCountsPerRoi.incrementPostForRoi(roi);
//                            }
//                            postCount++;
//                        }
//
//                        if (synapseNode.hasRelationship(RelationshipType.withName(SYNAPSES_TO))) {
//                            for (Relationship synapticRelationship : synapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
//                                Node synapticPartner = synapticRelationship.getOtherNode(synapseNode);
//
//                                Node connectedSegment = getSegmentThatContainsSynapse(synapticPartner);
//
//                                if (connectedSegment != null) {
//                                    if (synapseType.equals(PRE)) {
//                                        connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(newNeuron, connectedSegment, synapseNode, synapticPartner);
//                                    } else if (synapseType.equals(POST)) {
//                                        connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(connectedSegment, newNeuron, synapticPartner, synapseNode);
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//
//                // add synapse and synaptic partners to connection set; set connectsto relationships
//                createConnectionSetsAndConnectsToRelationships(connectsToRelationshipMap, datasetLabel);
//
//                // add roi boolean properties and roi info
//                addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(newNeuron, synapseCountsPerRoi);
//                newNeuron.setProperty(ROI_INFO, synapseCountsPerRoi.getAsJsonString());
//
//                // update pre and post on body; other properties
//                newNeuron.setProperty(PRE, preCount);
//                newNeuron.setProperty(POST, postCount);
//                newNeuron.setProperty(SIZE, neuronUpdate.getSize());
//                newNeuron.setProperty(MUTATION_UUID_ID, neuronUpdate.getMutationUuid() + ":" + neuronUpdate.getMutationId());
//
//                // check for optional properties; update neuron if present; decide if there should be a neuron label (has name, has soma, has pre+post>10)
//                boolean isNeuron = false;
//                if (neuronUpdate.getStatus() != null) {
//                    newNeuron.setProperty(STATUS, neuronUpdate.getStatus());
//                }
//                if (neuronUpdate.getName() != null) {
//                    newNeuron.setProperty(NAME, neuronUpdate.getName());
//                    isNeuron = true;
//                }
//                if (neuronUpdate.getSoma() != null) {
//                    newNeuron.setProperty(SOMA_RADIUS, neuronUpdate.getSoma().getRadius());
//                    Map<String, Object> parametersMap = new HashMap<>();
//                    List<Integer> somaLocation = neuronUpdate.getSoma().getLocation();
//                    parametersMap.put("x", somaLocation.get(0));
//                    parametersMap.put("y", somaLocation.get(1));
//                    parametersMap.put("z", somaLocation.get(2));
//                    Result locationResult = dbService.execute("WITH neuprint.locationAs3dCartPoint($x,$y,$z) AS loc RETURN loc", parametersMap);
//                    Point somaLocationPoint = (Point) locationResult.next().get("loc");
//                    newNeuron.setProperty(SOMA_LOCATION, somaLocationPoint);
//                    isNeuron = true;
//                }
//
//                if (preCount + postCount > 10) {
//                    isNeuron = true;
//                }
//
//                if (isNeuron) {
//                    newNeuron.addLabel(Label.label(NEURON));
//                    newNeuron.addLabel(Label.label(datasetLabel + "-" + NEURON));
//                }
//
////            add skeleton?
//
//                log.info("Completed neuron update: " + neuronUpdate.toString());
//
////                tx.success();
////            }
//        }
//
//        return Stream.of(new NodeResult(null));
//    }

    @Procedure(value = "proofreader.updateProperties", mode = Mode.WRITE)
    @Description("proofreader.updateProperties : Update properties on a Neuron/Segment node.")
    public void updateProperties(@Name("neuronJsonObject") String neuronJsonObject, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.updateProperties: entry");

        try {

            if (neuronJsonObject == null || datasetLabel == null) {
                log.error("proofreader.updateProperties: Missing input arguments.");
                throw new RuntimeException("proofreader.updateProperties: Missing input arguments.");
            }

            Gson gson = new Gson();

            Neuron neuron = gson.fromJson(neuronJsonObject, Neuron.class);

            Node neuronNode = dbService.findNode(Label.label(datasetLabel + "-" + SEGMENT), "bodyId", neuron.getId());

            if (neuronNode == null) {
                log.warn("Neuron with id " + neuron.getId() + " not found in database. Aborting update.");
            } else {

                if (neuron.getStatus() != null) {
                    neuronNode.setProperty(STATUS, neuron.getStatus());
                    log.info("Updated status for neuron " + neuron.getId() + ".");
                }

                if (neuron.getName() != null) {
                    neuronNode.setProperty(NAME, neuron.getName());

                    // adding a name makes it a Neuron
                    neuronNode.addLabel(Label.label(NEURON));
                    neuronNode.addLabel(Label.label(datasetLabel + "-" + NEURON));
                    dbService.execute("MATCH (m:Meta{dataset:\"" + datasetLabel + "\"}) WITH keys(apoc.convert.fromJsonMap(m.roiInfo)) AS rois MATCH (n:`" + datasetLabel + "-" + NEURON + "`{bodyId:" + neuron.getId() + "}) SET n.clusterName=neuprint.roiInfoAsName(n.roiInfo, n.pre, n.post, 0.10, rois) RETURN n.bodyId, n.clusterName");

                    log.info("Updated name for neuron " + neuron.getId() + ".");
                }

                if (neuron.getSize() != null) {
                    neuronNode.setProperty(SIZE, neuron.getSize());
                    log.info("Updated size for neuron " + neuron.getId() + ".");
                }

                if (neuron.getSoma() != null) {
                    Map<String, Object> parametersMap = new HashMap<>();
                    List<Integer> somaLocationList = neuron.getSoma().getLocation();
                    parametersMap.put("x", somaLocationList.get(0));
                    parametersMap.put("y", somaLocationList.get(1));
                    parametersMap.put("z", somaLocationList.get(2));
                    Result locationResult = dbService.execute("WITH neuprint.locationAs3dCartPoint($x,$y,$z) AS loc RETURN loc", parametersMap);
                    Point somaLocationPoint = (Point) locationResult.next().get("loc");
                    neuronNode.setProperty(SOMA_LOCATION, somaLocationPoint);
                    neuronNode.setProperty(SOMA_RADIUS, neuron.getSoma().getRadius());
                    log.info("Updated soma for neuron " + neuron.getId() + ".");
                }
            }

            log.info("proofreader.updateProperties: exit");

        } catch (Exception e) {
            log.error("Error running proofreader.updateProperties: " + e);
            throw new RuntimeException("Error running proofreader.updateProperties: " + e);
        }

    }

    @Procedure(value = "proofreader.deleteNeuron", mode = Mode.WRITE)
    @Description("proofreader.deleteNeuron(bodyId, datasetLabel) : Delete a neuron from the database.")
    public void deleteNeuron(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteNeuron: entry");

        try {

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteNeuron: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteNeuron: Missing input arguments.");
            }

            deleteSegment(bodyId, datasetLabel);

        } catch (Exception e) {
            log.error("Error running proofreader.deleteNeuron: " + e.getMessage());
            throw new RuntimeException("Error running proofreader.deleteNeuron: " + e.getMessage());
        }

        log.info("proofreader.deleteNeuron: exit");

    }

    @Procedure(value = "proofreader.updateNeuron", mode = Mode.WRITE)
    @Description("proofreader.updateNeuron(neuronUpdateJsonObject, datasetLabel): add a neuron with properties, synapses, and connections specified by an input JSON.")
    public void updateNeuron(@Name("neuronUpdateJson") String neuronUpdateJson, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.updateNeuron: entry");

        try {

        Gson gson = new Gson();
//        UpdateNeuronsAction updateNeuronsAction = gson.fromJson(neuronUpdateJson, UpdateNeuronsAction.class);
        NeuronUpdate neuronUpdate = gson.fromJson(neuronUpdateJson, NeuronUpdate.class);

        if (neuronUpdateJson == null || datasetLabel == null) {
            log.error("proofreader.updateNeuron: Missing input arguments.");
            throw new RuntimeException("proofreader.deleteNeuronupdateNeuron: Missing input arguments.");
        }

        // check that this mutation hasn't been done before (in order to be unique, needs to include uuid+mutationid+bodyId)
        String mutationKey = neuronUpdate.getMutationUuid() + ":" + neuronUpdate.getMutationId() + ":" + neuronUpdate.getBodyId();
        Node existingMutatedNode = dbService.findNode(Label.label(datasetLabel + "-" + SEGMENT), MUTATION_UUID_ID, mutationKey);
        if (existingMutatedNode != null) {
            log.error("Mutation already found in the database: " + neuronUpdate.toString());
            throw new RuntimeException("Mutation already found in the database: " + neuronUpdate.toString());
        }

        log.info("Beginning update: " + neuronUpdate);
        // create a new node and synapse set for that node
        final long newNeuronBodyId = neuronUpdate.getBodyId();
        final Node newNeuron = dbService.createNode(Label.label(SEGMENT),
                Label.label(datasetLabel),
                Label.label(datasetLabel + "-" + SEGMENT));

        try {
            newNeuron.setProperty(BODY_ID, newNeuronBodyId);
        } catch (org.neo4j.graphdb.ConstraintViolationException cve) {
            log.error("Body id " + newNeuronBodyId + " already exists in database. Aborting update for mutation with id : " + mutationKey);
            throw new RuntimeException("Body id " + newNeuronBodyId + " already exists in database. Aborting update for mutation with id : " + mutationKey);
        }

        final Node newSynapseSet = createSynapseSetForSegment(newNeuron, datasetLabel);

        // add appropriate synapses via synapse sets; add each synapse to the new body's synapseset
        // completely add everything here so that there's nothing left on the synapse store
        // add the new body id to the synapse sources

        Set<Synapse> currentSynapses = neuronUpdate.getCurrentSynapses();
        Set<Synapse> notFoundSynapses = new HashSet<>(currentSynapses);

        // from synapses, derive connectsto, connection sets, rois/roiInfo, pre/post counts
        final ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
        final SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();
        long preCount = 0L;
        long postCount = 0L;

        for (Synapse synapse : currentSynapses) {

            // get the synapse
            List<Integer> synapseLocation = synapse.getLocation();
            Map<String, Object> parametersMap = new HashMap<>();
            parametersMap.put("x", synapseLocation.get(0));
            parametersMap.put("y", synapseLocation.get(1));
            parametersMap.put("z", synapseLocation.get(2));
            Result locationResult = dbService.execute("WITH neuprint.locationAs3dCartPoint($x,$y,$z) AS loc RETURN loc", parametersMap);
            Point synapseLocationPoint = (Point) locationResult.next().get("loc");
            Node synapseNode = dbService.findNode(Label.label(datasetLabel + "-" + SYNAPSE), LOCATION, synapseLocationPoint);

            if (synapseNode == null) {
                log.error("Synapse not found in database: " + synapse);
                throw new RuntimeException("Synapse not found in database: " + synapse);
            }

            if (synapseNode.hasRelationship(RelationshipType.withName(CONTAINS))) {
                Node bodyWithSynapse = getSegmentThatContainsSynapse(synapseNode);
                Long bodyWithSynapseId = (Long) bodyWithSynapse.getProperty(BODY_ID);
                log.error("Synapse is already assigned to another body. body id: " + bodyWithSynapseId + ", synapse: " + synapse);
                throw new RuntimeException("Synapse is already assigned to another body. body id: " + bodyWithSynapseId + ", synapse: " + synapse);
            }

            // add synapse to the new synapse set
            newSynapseSet.createRelationshipTo(synapseNode, RelationshipType.withName(CONTAINS));
            // remove this synapse from the not found set
            notFoundSynapses.remove(synapse);

            // get the synapse type
            final String synapseType = (String) synapseNode.getProperty(TYPE);
            // get synapse rois for adding to the body and roiInfo
            final List<String> synapseRois = getSynapseNodeRoiList(synapseNode);

            if (synapseType.equals(PRE)) {
                for (String roi : synapseRois) {
                    synapseCountsPerRoi.incrementPreForRoi(roi);
                }
                preCount++;
            } else if (synapseType.equals(POST)) {
                for (String roi : synapseRois) {
                    synapseCountsPerRoi.incrementPostForRoi(roi);
                }
                postCount++;
            }

            if (synapseNode.hasRelationship(RelationshipType.withName(SYNAPSES_TO))) {
                for (Relationship synapticRelationship : synapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                    Node synapticPartner = synapticRelationship.getOtherNode(synapseNode);

                    Node connectedSegment = getSegmentThatContainsSynapse(synapticPartner);

                    if (connectedSegment != null) {
                        if (synapseType.equals(PRE)) {
                            connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(newNeuron, connectedSegment, synapseNode, synapticPartner);
                        } else if (synapseType.equals(POST)) {
                            connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(connectedSegment, newNeuron, synapticPartner, synapseNode);
                        }
                    }
                }
            }

        }

        if (!notFoundSynapses.isEmpty()) {
            log.error("Some synapses were not found for neuron update. Mutation UUID: " + neuronUpdate.getMutationUuid() + " Mutation ID: " + neuronUpdate.getMutationId() + " Synapse(s): " + notFoundSynapses);
            throw new RuntimeException("Some synapses were not found for neuron update. Mutation UUID: " + neuronUpdate.getMutationUuid() + " Mutation ID: " + neuronUpdate.getMutationId() + " Synapse(s): " + notFoundSynapses);
        }

        log.info("Found and added all synapses to synapse set for body id " + newNeuronBodyId);
        log.info("Completed making map of ConnectsTo relationships.");

        // add synapse and synaptic partners to connection set; set connectsto relationships
        createConnectionSetsAndConnectsToRelationships(connectsToRelationshipMap, datasetLabel);
        log.info("Completed creating ConnectionSets and ConnectsTo relationships.");

        // add roi boolean properties and roi info
        addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(newNeuron, synapseCountsPerRoi);
        newNeuron.setProperty(ROI_INFO, synapseCountsPerRoi.getAsJsonString());
        log.info("Completed updating roi information.");

        // update pre and post on body; other properties
        newNeuron.setProperty(PRE, preCount);
        newNeuron.setProperty(POST, postCount);
        newNeuron.setProperty(SIZE, neuronUpdate.getSize());
        newNeuron.setProperty(MUTATION_UUID_ID, mutationKey);

        // check for optional properties; update neuron if present; decide if there should be a neuron label (has name, has soma, has pre+post>10)
        boolean isNeuron = false;
        if (neuronUpdate.getStatus() != null) {
            newNeuron.setProperty(STATUS, neuronUpdate.getStatus());
            isNeuron = true;
        }
        if (neuronUpdate.getName() != null) {
            newNeuron.setProperty(NAME, neuronUpdate.getName());
            isNeuron = true;
        }
        if (neuronUpdate.getSoma() != null) {
            newNeuron.setProperty(SOMA_RADIUS, neuronUpdate.getSoma().getRadius());
            Map<String, Object> parametersMap = new HashMap<>();
            List<Integer> somaLocation = neuronUpdate.getSoma().getLocation();
            parametersMap.put("x", somaLocation.get(0));
            parametersMap.put("y", somaLocation.get(1));
            parametersMap.put("z", somaLocation.get(2));
            Result locationResult = dbService.execute("WITH neuprint.locationAs3dCartPoint($x,$y,$z) AS loc RETURN loc", parametersMap);
            Point somaLocationPoint = (Point) locationResult.next().get("loc");
            newNeuron.setProperty(SOMA_LOCATION, somaLocationPoint);
            isNeuron = true;
        }

        if (preCount + postCount > 10) {
            isNeuron = true;
        }

        if (isNeuron) {
            newNeuron.addLabel(Label.label(NEURON));
            newNeuron.addLabel(Label.label(datasetLabel + "-" + NEURON));
            dbService.execute("MATCH (m:Meta{dataset:\"" + datasetLabel + "\"}) WITH keys(apoc.convert.fromJsonMap(m.roiInfo)) AS rois MATCH (n:`" + datasetLabel + "-" + NEURON + "`{bodyId:" + neuronUpdate.getBodyId() + "}) SET n.clusterName=neuprint.roiInfoAsName(n.roiInfo, n.pre, n.post, 0.10, rois) RETURN n.bodyId, n.clusterName");
        }

        // update meta node

        Node metaNode = dbService.findNode(Label.label(META), "dataset", datasetLabel);
        metaNode.setProperty("latestMutationId", neuronUpdate.getMutationId());
        metaNode.setProperty("uuid", neuronUpdate.getMutationUuid());

//            add skeleton?

        log.info("Completed neuron update with uuid " + neuronUpdate.getMutationUuid() + ", mutation id " + neuronUpdate.getMutationId() + ", body id " + neuronUpdate.getBodyId() + ".");

        } catch (Exception e) {
            log.error("Error running proofreader.updateNeuron: " + e);
            throw new RuntimeException("Error running proofreader.updateNeuron: " + e);
        }

        log.info("proofreader.updateNeuron: exit");

    }

//    private void mergeSynapseSets(Node synapseSet1, Node synapseSet2) {
//
//        //add both synapse sets to the new node, collect them for adding to apoc merge procedure
//        Map<String, Object> parametersMap = new HashMap<>();
//        if (node1SynapseSetNode != null) {
//            newNode.createRelationshipTo(node1SynapseSetNode, RelationshipType.withName(CONTAINS));
//            parametersMap.put("ssnode1", node1SynapseSetNode);
//        }
//        if (node2SynapseSetNode != null) {
//            newNode.createRelationshipTo(node2SynapseSetNode, RelationshipType.withName(CONTAINS));
//            parametersMap.put("ssnode2", node2SynapseSetNode);
//        }
//
//        // merge the two synapse nodes using apoc if there are two synapseset nodes. inherits the datasetBodyId of the first node
//        if (parametersMap.containsKey("ssnode1") && parametersMap.containsKey("ssnode2")) {
//            dbService.execute("CALL apoc.refactor.mergeNodes([$ssnode1, $ssnode2], {properties:{datasetBodyId:\"discard\"}}) YIELD node RETURN node", parametersMap).next().get("node");
//            //delete the extra relationship between new node and new synapse set node
//            newNode.getRelationships(RelationshipType.withName(CONTAINS)).iterator().next().delete();
//        }
//    }

    private void lookForSynapsesOnSynapseSet(Node newSynapseSet, Node sourceSynapseSet, Set<Synapse> currentSynapses, Set<Synapse> notFoundSynapses) {

        if (sourceSynapseSet.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            for (Relationship containsRel : sourceSynapseSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                Node synapseNode = containsRel.getEndNode();
                List<Integer> synapseLocation = getNeo4jPointLocationAsLocationList((Point) synapseNode.getProperty(LOCATION));
                Synapse synapse = new Synapse(synapseLocation.get(0), synapseLocation.get(1), synapseLocation.get(2));

                if (currentSynapses.contains(synapse)) {
                    // remove this synapse from its old synapse set
                    containsRel.delete();
                    // add it to the new synapse set
                    newSynapseSet.createRelationshipTo(synapseNode, RelationshipType.withName(CONTAINS));
                    // remove this synapse from the not found set
                    notFoundSynapses.remove(synapse);
                }
            }
        } else {
            log.info("Empty synapse set found for source with id: " + sourceSynapseSet.getProperty(DATASET_BODY_ID));
        }
    }

    private void deleteSegment(long bodyId, String datasetLabel) {

        final Node neuron = dbService.findNode(Label.label(datasetLabel + "-" + SEGMENT), BODY_ID, bodyId);

        if (neuron == null) {
            log.info("Segment with body ID " + bodyId + " not found in database. Continuing update...");
        } else {
            acquireWriteLockForSegmentSubgraph(neuron);

            if (neuron.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                for (Relationship neuronContainsRel : neuron.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                    final Node containedNode = neuronContainsRel.getEndNode();
                    if (containedNode.hasLabel(Label.label(SYNAPSE_SET))) {
                        // delete the connection to the current node
                        neuronContainsRel.delete();
                        // delete relationships to synapses
                        if (containedNode.hasRelationship(RelationshipType.withName(CONTAINS))) {
                            for (Relationship synapseSetContainsRel : containedNode.getRelationships(RelationshipType.withName(CONTAINS))) {
                                synapseSetContainsRel.delete();
                            }
                        }
                        //delete synapse set
                        containedNode.delete();
                    } else if (containedNode.hasLabel(Label.label(CONNECTION_SET))) {
                        // delete relationships of connection set
                        containedNode.getRelationships().forEach(Relationship::delete);
                        // delete connectionset
                        containedNode.delete();
                    } else if (containedNode.hasLabel(Label.label(SKELETON))) {
                        // delete neuron relationship to skeleton
                        neuronContainsRel.delete();
                        // delete skeleton and skelnodes
                        deleteSkeleton(containedNode);
                    }
                }
            }

            // delete ConnectsTo relationships
            if (neuron.hasRelationship(RelationshipType.withName(CONNECTS_TO))) {
                neuron.getRelationships(RelationshipType.withName(CONNECTS_TO)).forEach(Relationship::delete);
            }

            // delete Neuron/Segment node
            neuron.delete();
            log.info("Deleted segment with body Id: " + bodyId);

        }

    }

    private void deleteSkeleton(final Node skeletonNode) {

        for (Relationship skeletonRelationship : skeletonNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            Node skelNode = skeletonRelationship.getEndNode();
            //delete LinksTo relationships
            for (Relationship skelNodeLinksToRelationship : skelNode.getRelationships(RelationshipType.withName(LINKS_TO))) {
                skelNodeLinksToRelationship.delete();
            }
            //delete SkelNode Contains relationship to Skeleton
            skeletonRelationship.delete();
            //delete SkelNode
            skelNode.delete();
        }
        //delete Skeleton
        skeletonNode.delete();

    }

    private Node createAndGetSynapseStoreNode(final String datasetLabel) {

        Node synapseStore = dbService.findNode(Label.label(SYNAPSE_STORE), DATASET, datasetLabel);

        if (synapseStore == null) {
            synapseStore = dbService.createNode(Label.label(SYNAPSE_STORE), Label.label(datasetLabel));
            synapseStore.setProperty(DATASET, datasetLabel);

        }

        return synapseStore;

    }

    private Node createSynapseSetForSegment(final Node segment, final String datasetLabel) {
        final Node newSynapseSet = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
        final long segmentBodyId = (long) segment.getProperty(BODY_ID);
        newSynapseSet.setProperty(DATASET_BODY_ID, datasetLabel + ":" + segmentBodyId);
        segment.createRelationshipTo(newSynapseSet, RelationshipType.withName(CONTAINS));
        return newSynapseSet;
    }

    private Node getSegmentThatContainsSynapse(Node synapseNode) {
        Node connectedSegment;

        final Node[] connectedSynapseSet = new Node[1];
        if (synapseNode.hasRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
            synapseNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING).forEach(r -> {
                if (r.getStartNode().hasLabel(Label.label(SYNAPSE_SET)))
                    connectedSynapseSet[0] = r.getStartNode();
            });
            connectedSegment = connectedSynapseSet[0].getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING).getStartNode();
        } else {
            log.info("Synapse does not belong to a body: " + synapseNode.getAllProperties());
            connectedSegment = null;
        }

        return connectedSegment;
    }

    private void createConnectionSetsAndConnectsToRelationships(ConnectsToRelationshipMap connectsToRelationshipMap, String datasetLabel) {

        for (String connectionKey : connectsToRelationshipMap.getSetOfConnectionKeys()) {
            final ConnectsToRelationship connectsToRelationship = connectsToRelationshipMap.getConnectsToRelationshipByKey(connectionKey);
            final Node startNode = connectsToRelationship.getStartNode();
            final Node endNode = connectsToRelationship.getEndNode();
            final long weight = connectsToRelationship.getWeight();
            // create a ConnectsTo relationship
            addConnectsToRelationship(startNode, endNode, weight);

            // create a ConnectionSet node
            final Node connectionSet = dbService.createNode(Label.label(CONNECTION_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + CONNECTION_SET));
            final long startNodeBodyId = (long) startNode.getProperty(BODY_ID);
            final long endNodeBodyId = (long) endNode.getProperty(BODY_ID);
            connectionSet.setProperty(DATASET_BODY_IDs, datasetLabel + ":" + startNodeBodyId + ":" + endNodeBodyId);

            // connect it to start and end bodies
            startNode.createRelationshipTo(connectionSet, RelationshipType.withName(CONTAINS));
            endNode.createRelationshipTo(connectionSet, RelationshipType.withName(CONTAINS));

            // add synapses to ConnectionSet
            for (final Node synapse : connectsToRelationship.getSynapsesInConnectionSet()) {
                // connection set Contains synapse
                connectionSet.createRelationshipTo(synapse, RelationshipType.withName(CONTAINS));
            }

        }

    }

    private void addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(Node segment, SynapseCountsPerRoi synapseCountsPerRoi) {
        for (String roi : synapseCountsPerRoi.getSetOfRois()) {
            segment.setProperty(roi, true);
        }
    }

//    @Procedure(value = "proofreader.mergeNeuronsFromJson", mode = Mode.WRITE)
//    @Description("proofreader.mergeNeuronsFromJson(mergeJson,datasetLabel) : merge neurons/segments from json file containing single mergeaction json")
//    public Stream<NodeResult> mergeNeuronsFromJson(@Name("mergeJson") String mergeJson, @Name("datasetLabel") String datasetLabel) {
//
//        if (mergeJson == null || datasetLabel == null) {
//            log.error("proofreader.mergeNeuronsFromJson: Missing input arguments.");
//            throw new RuntimeException("Missing input arguments.");
//        }
//
//        // TODO: investigate situation in which timeStamps do not get add to history or merged nodes. possibly just add them to nodes during procedure. is this happening to other, more important nodes?
//        // TODO: history nodes need to have unique ids?
//        // TODO: can make more efficient by never transferring anything from result node
//        // TODO: how does this work with batched transactions if there is a conflict?
//
//        final Gson gson = new Gson();
//        final MergeAction mergeAction = gson.fromJson(mergeJson, MergeAction.class);
//        if (!mergeAction.getAction().equals("merge")) {
//            log.error("proofreader.mergeNeuronsFromJson: Action was not \"merge\".");
//            throw new RuntimeException("Action was not \"merge\".");
//        }
//
//        List<Node> mergedBodies = new ArrayList<>();
//        Set<Long> mergedBodiesNotFound = new HashSet<>();
//        for (Long mergedBodyId : mergeAction.getBodiesMerged()) {
//            try {
//                Node mergedBody = acquireSegmentFromDatabase(mergedBodyId, datasetLabel);
//                mergedBodies.add(mergedBody);
//            } catch (NoSuchElementException | NullPointerException nse) {
//                mergedBodiesNotFound.add(mergedBodyId);
//            }
//        }
//
//        if (mergedBodiesNotFound.size() > 0) {
//            log.info(String.format("proofreader.mergeNeuronsFromJson: The following bodyIds were not found in dataset %s. Ignoring these bodies in merge procedure: " + mergedBodiesNotFound, datasetLabel));
//        }
//
//        Node targetBody;
//        try {
//            targetBody = acquireSegmentFromDatabase(mergeAction.getTargetBodyId(), datasetLabel);
//        } catch (NoSuchElementException | NullPointerException nse) {
//            log.info(String.format("proofreader.mergeNeuronsFromJson: Target body %d does not exist in dataset %s. Creating target body node...", mergeAction.getTargetBodyId(), datasetLabel));
//            targetBody = dbService.createNode(Label.label(SEGMENT), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SEGMENT));
//            targetBody.setProperty(BODY_ID, mergeAction.getTargetBodyId());
//            if (mergeAction.getTargetBodySize() != null) {
//                targetBody.setProperty(SIZE, mergeAction.getTargetBodySize());
//            }
//            if (mergeAction.getTargetBodyName() != null) {
//                targetBody.setProperty(NAME, mergeAction.getTargetBodyName());
//            }
//            if (mergeAction.getTargetBodyStatus() != null) {
//                targetBody.setProperty(STATUS, mergeAction.getTargetBodyStatus());
//            }
//        }
//
//        // grab write locks upfront
//        acquireWriteLockForSegmentSubgraph(targetBody);
//        for (Node body : mergedBodies) {
//            acquireWriteLockForSegmentSubgraph(body);
//        }
//
//        final Node newNode = recursivelyMergeNodes(targetBody, mergedBodies, mergeAction.getTargetBodySize(), mergeAction.getTargetBodyName(), mergeAction.getTargetBodyStatus(), datasetLabel, mergeAction);
//
//        log.info(String.format("Completed mergeAction for dataset %s, DVID UUID %s, mutationId %d. targetBodyId: %d, mergedBodies: " + mergeAction.getBodiesMerged() + ". Bodies not found in neo4j: " + mergedBodiesNotFound,
//                datasetLabel,
//                mergeAction.getDvidUuid(),
//                mergeAction.getMutationId(),
//                mergeAction.getTargetBodyId()));
//
//        return Stream.of(new NodeResult(newNode));
//    }
//
//    @Procedure(value = "proofreader.cleaveNeuronFromJson", mode = Mode.WRITE)
//    @Description("proofreader.cleaveNeuronFromJson(cleaveJson,datasetLabel) : cleave neuron/segment from json file containing a single cleaveaction json")
//    public Stream<NodeListResult> cleaveNeuronFromJson(@Name("cleaveJson") String cleaveJson, @Name("datasetLabel") String datasetLabel) {
//
//        if (cleaveJson == null || datasetLabel == null) {
//            log.error("proofreader.cleaveNeuronsFromJson: Missing input arguments.");
//            throw new RuntimeException("Missing input arguments.");
//        }
//
//        final Gson gson = new Gson();
//        final CleaveOrSplitAction cleaveOrSplitAction = gson.fromJson(cleaveJson, CleaveOrSplitAction.class);
//
//        Node originalBody;
//        try {
//            originalBody = acquireSegmentFromDatabase(cleaveOrSplitAction.getOriginalBodyId(), datasetLabel);
//        } catch (NoSuchElementException | NullPointerException nse) {
//            log.info(String.format("proofreader.cleaveNeuronsFromJson: Target body %d does not exist in dataset %s. Creating target body node..", cleaveOrSplitAction.getOriginalBodyId(), datasetLabel));
//            originalBody = dbService.createNode(Label.label(SEGMENT), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SEGMENT));
//            originalBody.setProperty("bodyId", cleaveOrSplitAction.getOriginalBodyId());
//        }
//
//        // grab write locks upfront
//        acquireWriteLockForSegmentSubgraph(originalBody);
//
//        //check if action is cleave or split
//        String typeOfAction = null;
//        String historyRelationshipType = null;
//        if (cleaveOrSplitAction.getAction().equals("cleave")) {
//            typeOfAction = CLEAVED;
//            historyRelationshipType = CLEAVED_TO;
//        } else if (cleaveOrSplitAction.getAction().equals("split")) {
//            typeOfAction = SPLIT;
//            historyRelationshipType = SPLIT_TO;
//        }
//        if (typeOfAction == null) {
//            log.error("proofreader.cleaveNeuronsFromJson: Unknown action type. Available actions for json are \"cleave\" or \"split\"");
//            throw new RuntimeException("Unknown action type. Available actions for json are \"cleave\" or \"split\"");
//        }
//
//        // create new segment nodes with properties
//        final Node cleavedOriginalBody = copyPropertiesToNewNodeForCleaveSplitOrMerge(originalBody, typeOfAction);
//        final Node cleavedNewBody = dbService.createNode();
//        cleavedNewBody.setProperty(BODY_ID, cleaveOrSplitAction.getNewBodyId());
//        cleavedNewBody.setProperty(SIZE, cleaveOrSplitAction.getNewBodySize());
//        log.info(String.format("Cleaving %s from %s...", cleavedNewBody.getProperty(BODY_ID), cleavedOriginalBody.getProperty(BODY_ID)));
//
//        // original segment synapse set
//        Node originalSynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(originalBody);
//        if (originalSynapseSetNode == null) {
//            log.info(String.format("proofreader.cleaveNeuronsFromJson: No %s node on original body. Creating one...", SYNAPSE_SET));
//            originalSynapseSetNode = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
//            originalSynapseSetNode.setProperty(DATASET_BODY_ID, datasetLabel + ":" + cleaveOrSplitAction.getOriginalBodyId());
//            originalBody.createRelationshipTo(originalSynapseSetNode, RelationshipType.withName(CONTAINS));
////            throw new RuntimeException(String.format("No %s node on original body.", SYNAPSE_SET));
//        }
//        //create a synapse set for the new body with unique ID
//        Node newBodySynapseSetNode = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
//        newBodySynapseSetNode.setProperty(DATASET_BODY_ID, datasetLabel + ":" + cleaveOrSplitAction.getNewBodyId());
//        //connect both synapse sets to the cleaved nodes
//        cleavedOriginalBody.createRelationshipTo(originalSynapseSetNode, RelationshipType.withName(CONTAINS));
//        cleavedNewBody.createRelationshipTo(newBodySynapseSetNode, RelationshipType.withName(CONTAINS));
//
//        //move synapses when necessary
//        final ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
//        final Set<Synapse> originalBodySynapseSet = new HashSet<>();
//        final Set<Synapse> newBodySynapseSet = cleaveOrSplitAction.getNewBodySynapses();
//        SynapseCounter originalBodySynapseCounter = new SynapseCounter();
//        SynapseCounter newBodySynapseCounter = new SynapseCounter();
//
//        //need to set roi for new body synapses from database
//        for (Synapse synapse : newBodySynapseSet) {
//            setSynapseRoisFromDatabase(synapse, datasetLabel);
//        }
//
//        if (originalSynapseSetNode.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
//            for (Relationship synapseRelationship : originalSynapseSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
//                Node synapseNode = synapseRelationship.getEndNode();
//                try {
//                    //create a synapse object from synapse node
//                    List<Integer> synapseLocation = getNeo4jPointLocationAsLocationList((Point) synapseNode.getProperty(LOCATION));
//                    Synapse synapse = new Synapse((String) synapseNode.getProperty(TYPE),
//                            synapseLocation.get(0),
//                            synapseLocation.get(1),
//                            synapseLocation.get(2),
//                            getSynapseNodeRoiList(synapseNode));
//                    if (newBodySynapseSet.contains(synapse)) {
//                        //delete connection to original synapse set
//                        synapseRelationship.delete();
//                        //add connection to new synapse set
//                        newBodySynapseSetNode.createRelationshipTo(synapseNode, RelationshipType.withName(CONTAINS));
//                        //store connects to relationship to add later
//                        addSynapseToConnectsToRelationshipMap(synapseNode, synapse, cleavedNewBody, connectsToRelationshipMap);
//                        incrementSynapseCounterWithSynapse(synapse, newBodySynapseCounter);
//                    } else {
//                        //keep synapse in place but keep track of which synapse this is. necessary?
//                        originalBodySynapseSet.add(synapse);
//                        addSynapseToConnectsToRelationshipMap(synapseNode, synapse, cleavedOriginalBody, connectsToRelationshipMap);
//                        incrementSynapseCounterWithSynapse(synapse, originalBodySynapseCounter);
//                    }
//                } catch (org.neo4j.graphdb.NotFoundException nfe) {
//                    nfe.printStackTrace();
//                    log.error(String.format("proofreader.cleaveNeuronsFromJson: %s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
//                    throw new RuntimeException(String.format("%s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
//                }
//            }
//        }
//
//        //recreate connects to relationships
//        addConnectsToRelationshipsFromMap(connectsToRelationshipMap);
//
//        //recreate synapseCountPerRoi
//        //cleavedNewBody
//        SynapseCountsPerRoi newBodySynapseCountsPerRoi = BodyWithSynapses.getSynapseCountersPerRoiFromSynapseSet(newBodySynapseSet);
//        cleavedNewBody.setProperty(ROI_INFO, newBodySynapseCountsPerRoi.getAsJsonString());
//        //cleavedOriginalBody
//        SynapseCountsPerRoi originalBodySynapseCountsPerRoi = BodyWithSynapses.getSynapseCountersPerRoiFromSynapseSet(originalBodySynapseSet);
//        cleavedOriginalBody.setProperty(ROI_INFO, originalBodySynapseCountsPerRoi.getAsJsonString());
//
//        //add proper roi labels to Segment
//        addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(cleavedNewBody, newBodySynapseCountsPerRoi);
//        addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(cleavedOriginalBody, originalBodySynapseCountsPerRoi);
//        //update pre, post properties for new nodes
//        cleavedNewBody.setProperty(PRE, newBodySynapseCounter.getPre());
//        cleavedNewBody.setProperty(POST, newBodySynapseCounter.getPost());
//        cleavedOriginalBody.setProperty(PRE, originalBodySynapseCounter.getPre());
//        cleavedOriginalBody.setProperty(POST, originalBodySynapseCounter.getPost());
//
//        //delete old skeleton
//        deleteSkeletonForNode(originalBody);
//        //delete connects to
//        removeConnectsToRelationshipsForNode(originalBody);
//
//        collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(originalBody);
//
//        // create a new history node
//        Node newBodyHistoryNode = dbService.createNode(Label.label(HISTORY), Label.label(datasetLabel));
//        Node originalBodyHistoryNode = dbService.createNode(Label.label(HISTORY), Label.label(datasetLabel));
//        // connect history node to new Segment
//        cleavedNewBody.createRelationshipTo(newBodyHistoryNode, RelationshipType.withName(FROM));
//        cleavedOriginalBody.createRelationshipTo(originalBodyHistoryNode, RelationshipType.withName(FROM));
//        // connect ghost nodes to history Segment
//        originalBody.createRelationshipTo(originalBodyHistoryNode, RelationshipType.withName(historyRelationshipType));
//        originalBody.createRelationshipTo(newBodyHistoryNode, RelationshipType.withName(historyRelationshipType));
//
//        //add dviduuid and mutationid, remove labels, all properties to cleaved or split
//        originalBody.setProperty("dvidUuid", cleaveOrSplitAction.getDvidUuid());
//        originalBody.setProperty("mutationId", cleaveOrSplitAction.getMutationId());
//        convertAllPropertiesToCleavedOrSplitProperties(originalBody, typeOfAction);
//        removeAllLabels(originalBody);
//        List<String> except = new ArrayList<>();
//        except.add(MERGED_TO);
//        except.add(CLEAVED_TO);
//        except.add(SPLIT_TO);
//        removeAllRelationshipsExceptTypesWithName(originalBody, except);
//
//        //add dataset and Segment labels to new nodes
//        cleavedOriginalBody.addLabel(Label.label(SEGMENT));
//        cleavedOriginalBody.addLabel(Label.label(datasetLabel));
//        cleavedOriginalBody.addLabel(Label.label(datasetLabel + "-" + SEGMENT));
//        cleavedNewBody.addLabel(Label.label(SEGMENT));
//        cleavedNewBody.addLabel(Label.label(datasetLabel));
//        cleavedNewBody.addLabel(Label.label(datasetLabel + "-" + SEGMENT));
//
//        List<Node> listOfCleavedNodes = new ArrayList<>();
//        listOfCleavedNodes.add(cleavedNewBody);
//        listOfCleavedNodes.add(cleavedOriginalBody);
//
//        log.info(String.format("Completed cleaveOrSplitAction for dataset %s, DVID UUID %s, mutationId %d, actionType: %s. originalBodyId: %d. Created new body with bodyId: %d.",
//                datasetLabel,
//                cleaveOrSplitAction.getDvidUuid(),
//                cleaveOrSplitAction.getMutationId(),
//                cleaveOrSplitAction.getAction(),
//                cleaveOrSplitAction.getOriginalBodyId(),
//                cleaveOrSplitAction.getNewBodyId()));
//
//        return Stream.of(new NodeListResult(listOfCleavedNodes));
//
//    }

    @Procedure(value = "proofreader.addSkeleton", mode = Mode.WRITE)
    @Description("proofreader.addSkeleton(fileUrl,datasetLabel) : load skeleton from provided url and connect to its associated neuron/segment (note: file URL must contain body id of neuron/segment) ")
    public Stream<NodeResult> addSkeleton(@Name("fileUrl") String fileUrlString, @Name("datasetLabel") String datasetLabel) {

        if (fileUrlString == null || datasetLabel == null) return Stream.empty();

        String bodyIdPattern = ".*/(.*?)[._]swc";
        Pattern rN = Pattern.compile(bodyIdPattern);
        Matcher mN = rN.matcher(fileUrlString);
        mN.matches();
        Long bodyId = Long.parseLong(mN.group(1));

        Skeleton skeleton = new Skeleton();
        URL fileUrl;

        try {
            fileUrl = new URL(fileUrlString);
        } catch (MalformedURLException e) {
            log.error(String.format("proofreader.addSkeleton: Malformed URL: %s", e.getMessage()));
            throw new RuntimeException(String.format("Malformed URL: %s", e.getMessage()));
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileUrl.openStream()))) {
            skeleton.fromSwc(reader, bodyId);
        } catch (IOException e) {
            log.error(String.format("proofreader.addSkeleton: IOException: %s", e.getMessage()));
            throw new RuntimeException(String.format("IOException: %s", e.getMessage()));
        }

        Node segment;
        try {
            segment = acquireSegmentFromDatabase(bodyId, datasetLabel);
        } catch (NoSuchElementException | NullPointerException nse) {
            log.error(String.format("proofreader.addSkeleton: Body %d does not exist in dataset %s. Aborting addSkeleton.", bodyId, datasetLabel));
            throw new RuntimeException(String.format("proofreader.addSkeleton: Body %d does not exist in dataset %s. Aborting addSkeleton.", bodyId, datasetLabel));
        }

        // grab write locks upfront
        acquireWriteLockForSegmentSubgraph(segment);

        Node skeletonNode = addSkeletonNodes(datasetLabel, skeleton);

        return Stream.of(new NodeResult(skeletonNode));

    }

//    @Procedure(value = "proofreader.deleteNeuron", mode = Mode.WRITE)
//    @Description("proofreader.deleteNeuron(neuronBodyId,datasetLabel) : delete neuron/segment with body Id and associated nodes (except Synapses) from given dataset. ")
//    public void deleteNeuron(@Name("neuronBodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {
//        if (bodyId == null || datasetLabel == null) {
//            log.error("Must provide both a bodyId and dataset label.");
//            throw new RuntimeException("Must provide both a bodyId and dataset label.");
//        }
//
//        final Node segment = acquireSegmentFromDatabase(bodyId, datasetLabel);
//
//        // grab write locks upfront
//        acquireWriteLockForSegmentSubgraph(segment);
//
//        //delete History
////        removeHistoryForNode(neuron);
//
//        //delete connectsTo relationships
//        removeConnectsToRelationshipsForNode(segment);
//
//        //delete synapse set and synapses
//        removeSynapseSetsAndContainsRelationshipsToSynapses(segment);
//
//        //delete skeleton
//        deleteSkeletonForNode(segment);
//
//        segment.delete();
//
//    }

//    @Procedure(value = "proofreader.addNeuron", mode = Mode.WRITE)
//    @Description("proofreader.addNeuron(neuronJson,datasetLabel) : add neuron/segment specified in json to given dataset.")
//    public Stream<NodeResult> addNeuron(@Name("neuronJson") String neuronJson, @Name("datasetLabel") String datasetLabel) {
//        if (neuronJson == null || datasetLabel == null) {
//            log.error("Must provide a neuron json and a dataset label.");
//            throw new RuntimeException("Must provide a neuron json and a dataset label.");
//        }
//        //grab write locks on connected nodes (synapses, neurons)
//
//        //add neuron node with properties and labels
//
//        //add connects to relationships
//
//        //add synapseset and connect to synapses
//
//        //add skeleton?
//
//        return Stream.of(new NodeResult(null));
//
//    }

//    private Node recursivelyMergeNodes(Node resultNode, List<Node> mergedBodies, Long newNodeSize, String newNodeName, String newNodeStatus, String datasetLabel, MergeAction mergeAction) {
//        if (mergedBodies.size() == 0) {
//            if ((!resultNode.hasProperty(SIZE) || (resultNode.hasProperty(SIZE) && !resultNode.getProperty(SIZE).equals(newNodeSize))) && newNodeSize != null) {
//                resultNode.setProperty(SIZE, newNodeSize);
//            }
//            if ((!resultNode.hasProperty(NAME) || (resultNode.hasProperty(NAME) && !resultNode.getProperty(NAME).equals(newNodeName))) && newNodeName != null) {
//                resultNode.setProperty(NAME, newNodeName);
//            }
//            if ((!resultNode.hasProperty(STATUS) || (resultNode.hasProperty(STATUS) && !resultNode.getProperty(STATUS).equals(newNodeStatus))) && newNodeStatus != null) {
//                resultNode.setProperty(STATUS, newNodeStatus);
//            }
//            return resultNode;
//        } else {
//            Node mergedNode = mergeTwoNodesOntoNewNode(resultNode, mergedBodies.get(0), newNodeSize, newNodeName, newNodeStatus, datasetLabel, mergeAction);
//            return recursivelyMergeNodes(mergedNode, mergedBodies.subList(1, mergedBodies.size()), newNodeSize, newNodeName, newNodeStatus, datasetLabel, mergeAction);
//        }
//    }
//

    private void addConnectsToRelationship(Node startNode, Node endNode, long weight) {
        // create a ConnectsTo relationship
        Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(CONNECTS_TO));
        relationship.setProperty(WEIGHT, weight);
    }
//
//    private void addSynapseToConnectsToRelationshipMap(Node synapseNode, Synapse synapse, Node segmentWithSynapse, ConnectsToRelationshipMap connectsToRelationshipMap) {
//        // get the node that this synapse connects to
//        List<Node> listOfConnectedSegments = getPostOrPreSynapticSegmentGivenSynapse(synapseNode);
//        // add to the connectsToRelationshipMap
//        for (Node connectedSegment : listOfConnectedSegments) {
//            if (synapse.getType().equals(PRE)) {
//                // if synapse is a presynapse, then the connection is from (currentsegment)->(connectedsegment) and the weight increase is the number of postsynapses that this presynapse synapses to
//                connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(segmentWithSynapse, connectedSegment, synapseNode);
//            } else if (synapse.getType().equals(POST)) {
//                // if the synapse is a postsynapse, then the connection is from (connectedsegment)->(currentsegment) and the weight of this connection increases by one
//                connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(connectedSegment, segmentWithSynapse, synapseNode);
//            }
//        }
//    }

//    private void mergeSynapseSetsOntoNewNodeAndRemoveFromPreviousNodes(Node node1, Node node2, Node newNode) {
//
//        //get the synapse sets for node1 and node2, remove them from original nodes
//        Node node1SynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node1);
//        Node node2SynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node2);
//
//        //add both synapse sets to the new node, collect them for adding to apoc merge procedure
//        Map<String, Object> parametersMap = new HashMap<>();
//        if (node1SynapseSetNode != null) {
//            newNode.createRelationshipTo(node1SynapseSetNode, RelationshipType.withName(CONTAINS));
//            parametersMap.put("ssnode1", node1SynapseSetNode);
//        }
//        if (node2SynapseSetNode != null) {
//            newNode.createRelationshipTo(node2SynapseSetNode, RelationshipType.withName(CONTAINS));
//            parametersMap.put("ssnode2", node2SynapseSetNode);
//        }
//
//        // merge the two synapse nodes using apoc if there are two synapseset nodes. inherits the datasetBodyId of the first node
//        if (parametersMap.containsKey("ssnode1") && parametersMap.containsKey("ssnode2")) {
//            dbService.execute("CALL apoc.refactor.mergeNodes([$ssnode1, $ssnode2], {properties:{datasetBodyId:\"discard\"}}) YIELD node RETURN node", parametersMap).next().get("node");
//            //delete the extra relationship between new node and new synapse set node
//            newNode.getRelationships(RelationshipType.withName(CONTAINS)).iterator().next().delete();
//        }
//
//    }

//    private Set<Synapse> createSynapseSetForSegment(Node segment, String datasetLabel) {
//
//        Node synapseSetNode = getSynapseSetNodeForSegment(segment);
//        if (synapseSetNode == null) {
//            log.info(String.format("ProofreaderProcedures createSynapseSetForSegment: No %s on neuron %s. Creating one...", SYNAPSE_SET, segment.getProperty("bodyId")));
//            synapseSetNode = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
//            synapseSetNode.setProperty(DATASET_BODY_ID, datasetLabel + ":" + segment.getProperty("bodyId"));
//            segment.createRelationshipTo(synapseSetNode, RelationshipType.withName(CONTAINS));
////            throw new RuntimeException(String.format("No %s on neuron.", SYNAPSE_SET));
//        }
//
//        Set<Synapse> synapseSet = new HashSet<>();
//        //make sure there are synapses attached to the synapse set
//        if (synapseSetNode.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
//            for (Relationship synapseRelationship : synapseSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
//                Node synapseNode = synapseRelationship.getEndNode();
//                try {
//                    List<Integer> synapseLocation = getNeo4jPointLocationAsLocationList((Point) synapseNode.getProperty(LOCATION));
//                    Synapse synapse = new Synapse((String) synapseNode.getProperty(TYPE),
//                            synapseLocation.get(0),
//                            synapseLocation.get(1),
//                            synapseLocation.get(2),
//                            getSynapseNodeRoiList(synapseNode));
//                    synapseSet.add(synapse);
//                } catch (org.neo4j.graphdb.NotFoundException nfe) {
//                    nfe.printStackTrace();
//                    log.error(String.format("ProofreaderProcedures createSynapseSetForSegment: %s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
//                    throw new RuntimeException(String.format("%s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
//                }
//            }
//        }
//
//        return synapseSet;
//    }

    private void collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(Node node) {

        Node existingHistoryNode = getHistoryNodeForNode(node);

        if (existingHistoryNode != null) {
            for (Relationship mergedToRelationship : existingHistoryNode.getRelationships(RelationshipType.withName(MERGED_TO))) {
                //get the preexisting ghost node
                Node ghostNode = mergedToRelationship.getStartNode();
                //connect it to result body ghost
                ghostNode.createRelationshipTo(node, RelationshipType.withName(MERGED_TO));
                //remove its connection to the old history node
                mergedToRelationship.delete();
            }
            for (Relationship cleavedToRelationship : existingHistoryNode.getRelationships(RelationshipType.withName(CLEAVED_TO))) {
                //get the preexisting ghost node
                Node ghostNode = cleavedToRelationship.getStartNode();
                //connect it to result body ghost
                ghostNode.createRelationshipTo(node, RelationshipType.withName(CLEAVED_TO));
                //remove its connection to the old history node
                cleavedToRelationship.delete();
            }
            for (Relationship cleavedToRelationship : existingHistoryNode.getRelationships(RelationshipType.withName(SPLIT_TO))) {
                //get the preexisting ghost node
                Node ghostNode = cleavedToRelationship.getStartNode();
                //connect it to result body ghost
                ghostNode.createRelationshipTo(node, RelationshipType.withName(SPLIT_TO));
                //remove its connection to the old history node
                cleavedToRelationship.delete();
            }
            //delete the old history node
            existingHistoryNode.delete();
        }

    }

    private Node getHistoryNodeForNode(Node node) {

        Node historyNode = null;
        for (Relationship historyRelationship : node.getRelationships(RelationshipType.withName(FROM))) {
            historyNode = historyRelationship.getEndNode();
        }
        return historyNode;

    }

//    private List<String> getRoisForSynapse(Synapse synapse, String datasetLabel) {
//
//        Map<String, Object> roiQueryResult;
//        Map<String, Object> parametersMap = new HashMap<>();
//        parametersMap.put("x", (double) synapse.getX());
//        parametersMap.put("y", (double) synapse.getY());
//        parametersMap.put("z", (double) synapse.getZ());
//
//        try {
//            roiQueryResult = dbService.execute("MATCH (s:`" + datasetLabel + "-Synapse`) WHERE s.location=neuprint.locationAs3dCartPoint($x,$y,$z) WITH keys(s) AS props " +
//                    "RETURN filter(prop IN props WHERE " +
//                    "NOT prop=\"type\" AND " +
//                    "NOT prop=\"confidence\" AND " +
//                    "NOT prop=\"location\" AND " +
//                    "NOT prop=\"timeStamp\") AS l", parametersMap).next();
//        } catch (java.util.NoSuchElementException nse) {
//            nse.printStackTrace();
//            log.error(String.format("ProofreaderProcedures getRoisForSynapse: Error using proofreader procedures: %s not found in the dataset.", SYNAPSE));
//            throw new RuntimeException(String.format("Error using proofreader procedures: %s not found in the dataset.", SYNAPSE));
//        }
//
//        return (ArrayList<String>) roiQueryResult.get("l");
//    }
//
//    private void setSynapseRoisFromDatabase(Synapse synapse, String datasetLabel) {
//        List<String> roiList = getRoisForSynapse(synapse, datasetLabel);
//        if (roiList.size() == 0) {
//            log.info(String.format("ProofreaderProcedures setSynapseRoisFromDatabase: No roi found on %s: %s", SYNAPSE, synapse));
//            //throw new RuntimeException(String.format("No roi found on %s: %s", SYNAPSE, synapse));
//        }
//        synapse.addRoiList(new ArrayList<>(roiList));
//    }

    private List<String> getSynapseNodeRoiList(Node synapseNode) {
        Map<String, Object> synapseNodeProperties = synapseNode.getAllProperties();
        return synapseNodeProperties.keySet().stream()
                .filter(p -> (
                        !p.equals(TIME_STAMP) &&
                                !p.equals(LOCATION) &&
                                !p.equals(TYPE) &&
                                !p.equals(CONFIDENCE))
                )
                .collect(Collectors.toList());
    }

    private Node acquireSegmentFromDatabase(Long nodeBodyId, String datasetLabel) throws NoSuchElementException, NullPointerException {
        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put(BODY_ID, nodeBodyId);
        Map<String, Object> nodeQueryResult;
        Node foundNode;
//        try {
        nodeQueryResult = dbService.execute("MATCH (node:`" + datasetLabel + "-Segment`{bodyId:$bodyId}) RETURN node", parametersMap).next();
//        } catch (java.util.NoSuchElementException nse) {
//            nse.printStackTrace();
//            log.error(String.format("%d not found in dataset %s.", nodeBodyId, datasetLabel));
//            throw new RuntimeException(String.format("Error using proofreader procedures: All neuron nodes must exist in the dataset and be labeled :%s-%s.", datasetLabel, NEURON));
//        }
//
//        try {
        foundNode = (Node) nodeQueryResult.get("node");
//        } catch (NullPointerException npe) {
//            npe.printStackTrace();
//            log.error(String.format("%d not found in dataset %s.", nodeBodyId, datasetLabel));
//            throw new RuntimeException(String.format("Error using proofreader procedures: All neuron nodes must exist in the dataset and be labeled :%s-%s.", datasetLabel, NEURON));
//        }

        return foundNode;
    }

//    private List<Node> getPostOrPreSynapticSegmentGivenSynapse(Node synapse) {
//        // list so that multiple connections per neuron are each counted as 1 additional weight
//        List<Node> listOfConnectedSegments = new ArrayList<>();
//        if (synapse.hasRelationship(RelationshipType.withName(SYNAPSES_TO))) {
//            for (Relationship synapsesToRelationship : synapse.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
//                Node connectedSynapse = synapsesToRelationship.getOtherNode(synapse);
//                Node connectedSegment;
//                try {
//                    final Node[] connectedSynapseSet = new Node[1];
//                    connectedSynapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING).forEach(r -> {
//                        if (r.getStartNode().hasLabel(Label.label(SYNAPSE_SET)))
//                            connectedSynapseSet[0] = r.getStartNode();
//                    });
//                    connectedSegment = connectedSynapseSet[0].getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING).getStartNode();
//                } catch (NoSuchElementException nse) {
//                    log.error(String.format("Connected %s has no %s or connected %ss: %s", SYNAPSE, SYNAPSE_SET, SEGMENT, connectedSynapse.getAllProperties()));
//                    throw new RuntimeException(String.format("Connected %s has no %s or connected %ss: %s", SYNAPSE, SYNAPSE_SET, SEGMENT, connectedSynapse.getAllProperties()));
//                }
//                listOfConnectedSegments.add(connectedSegment);
//            }
//        }
//        return listOfConnectedSegments;
//    }

    private List<Integer> getNeo4jPointLocationAsLocationList(Point neo4jPoint) {
        return neo4jPoint.getCoordinate().getCoordinate().stream().map(d -> (int) Math.round(d)).collect(Collectors.toList());
    }

    private void removeAllLabels(Node node) {
        Iterable<Label> nodeLabels = node.getLabels();
        for (Label label : nodeLabels) {
            node.removeLabel(label);
        }
    }

    private void removeAllRelationshipsExceptTypesWithName(Node node, List<String> except) {
        Iterable<Relationship> nodeRelationships = node.getRelationships();
        for (Relationship relationship : nodeRelationships) {
            if (!except.contains(relationship.getType().name())) {
                relationship.delete();
            }
        }
    }

    private void removeAllRelationships(Node node) {
        Iterable<Relationship> nodeRelationships = node.getRelationships();
        for (Relationship relationship : nodeRelationships) {
            relationship.delete();
        }
    }

    private Node addSkeletonNodes(final String dataset, final Skeleton skeleton) {

        final String segmentToSkeletonConnectionString = "MERGE (n:`" + dataset + "-Segment`{bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n:Segment, n:" + dataset + " \n" +
                "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r:Skeleton, r:" + dataset + " \n" +
                "MERGE (n)-[:Contains]->(r) \n";

        final String rootNodeString = "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r:Skeleton, r:" + dataset + " \n" +
                "MERGE (s:`" + dataset + "-SkelNode`{skelNodeId:$skelNodeId}) ON CREATE SET s.skelNodeId=$skelNodeId, s.location=neuprint.locationAs3dCartPoint($x,$y,$z), s.radius=$radius, s.rowNumber=$rowNumber, s.type=$type, s:SkelNode, s:" + dataset + " \n" +
                "MERGE (r)-[:Contains]->(s) \n";

        final String parentNodeString = "MERGE (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) ON CREATE SET r:Skeleton, r:" + dataset + " \n" +
                "MERGE (p:`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=neuprint.locationAs3dCartPoint($pX,$pY,$pZ), p.radius=$pRadius, p.rowNumber=$pRowNumber, p.type=$pType, p:SkelNode, p:" + dataset + " \n" +
                "MERGE (r)-[:Contains]->(p) ";

        final String childNodeString = "MERGE (p:`" + dataset + "-SkelNode`{skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=neuprint.locationAs3dCartPoint($pX,$pY,$pZ), p.radius=$pRadius, p.rowNumber=$pRowNumber, p.type=$pType, p:SkelNode, p:" + dataset + " \n" +
                "MERGE (c:`" + dataset + "-SkelNode`{skelNodeId:$childNodeId}) ON CREATE SET c.skelNodeId=$childNodeId, c.location=neuprint.locationAs3dCartPoint($childX,$childY,$childZ), c.radius=$childRadius, c.rowNumber=$childRowNumber, c.type=$childType, c:SkelNode, c:" + dataset + " \n" +
                "MERGE (p)-[:LinksTo]-(c)";

        Long associatedBodyId = skeleton.getAssociatedBodyId();
        List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

        Map<String, Object> segmentToSkeletonParametersMap = new HashMap<String, Object>() {{
            put(BODY_ID, associatedBodyId);
            put("skeletonId", dataset + ":" + associatedBodyId);
        }};

        dbService.execute(segmentToSkeletonConnectionString, segmentToSkeletonParametersMap);

        for (SkelNode skelNode : skelNodeList) {

            if (skelNode.getParent() == null) {
                Map<String, Object> rootNodeStringParametersMap = new HashMap<String, Object>() {{
                    put("x", (double) skelNode.getX());
                    put("y", (double) skelNode.getY());
                    put("z", (double) skelNode.getZ());
                    put("radius", skelNode.getRadius());
                    put("skelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString());
                    put("skeletonId", dataset + ":" + associatedBodyId);
                    put("rowNumber", skelNode.getRowNumber());
                    put("type", skelNode.getType());
                }};

                dbService.execute(rootNodeString, rootNodeStringParametersMap);
            }

            Map<String, Object> parentNodeStringParametersMap = new HashMap<String, Object>() {{
                put("pX", (double) skelNode.getX());
                put("pY", (double) skelNode.getY());
                put("pZ", (double) skelNode.getZ());
                put("pRadius", skelNode.getRadius());
                put("parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString());
                put("skeletonId", dataset + ":" + associatedBodyId);
                put("pRowNumber", skelNode.getRowNumber());
                put("pType", skelNode.getType());
            }};

            dbService.execute(parentNodeString, parentNodeStringParametersMap);

            for (SkelNode child : skelNode.getChildren()) {
                String childNodeId = dataset + ":" + associatedBodyId + ":" + child.getLocationString();

                Map<String, Object> childNodeStringParametersMap = new HashMap<String, Object>() {{
                    put("parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString());
                    put("skeletonId", dataset + ":" + associatedBodyId);
                    put("pX", (double) skelNode.getX());
                    put("pY", (double) skelNode.getY());
                    put("pZ", (double) skelNode.getZ());
                    put("pRadius", skelNode.getRadius());
                    put("pRowNumber", skelNode.getRowNumber());
                    put("pType", skelNode.getType());
                    put("childNodeId", childNodeId);
                    put("childX", (double) child.getX());
                    put("childY", (double) child.getY());
                    put("childZ", (double) child.getZ());
                    put("childRadius", child.getRadius());
                    put("childRowNumber", child.getRowNumber());
                    put("childType", skelNode.getType());
                }};

                dbService.execute(childNodeString, childNodeStringParametersMap);

            }

        }
        Map<String, Object> getSkeletonParametersMap = new HashMap<String, Object>() {{
            put("skeletonId", dataset + ":" + associatedBodyId);
        }};

        Map<String, Object> nodeQueryResult = dbService.execute("MATCH (r:`" + dataset + "-Skeleton`{skeletonId:$skeletonId}) RETURN r", getSkeletonParametersMap).next();
        return (Node) nodeQueryResult.get("r");
    }
//
//    private void removeHistoryForNode(Node node) {
//        Node historyNode = getHistoryNodeForNode(node);
//        if (historyNode==null) {
//            //no history node so return
//            return;
//        }
//
//        // delete from relationship
//        Relationship fromRelationship = node.getSingleRelationship(RelationshipType.withName(FROM),Direction.OUTGOING);
//        fromRelationship.delete();
//
//        //go one degree out
//        for (Relationship historyRelationship: historyNode.getRelationships()) {
//            //get ghost node attached to this relationship
//            Node ghostNode = historyRelationship.getStartNode();
//            //delete relationship to history node
//            historyRelationship.delete();
//            //if this is the only relationship this node has then delete it
//            if (ghostNode.getDegree()==0) {
//                ghostNode.delete();
//            }
//        }
//
//
//    }

    private void acquireWriteLockForNode(Node node) {
        try (Transaction tx = dbService.beginTx()) {
            tx.acquireWriteLock(node);
            tx.success();
        }
    }

    private void acquireWriteLockForRelationship(Relationship relationship) {
        try (Transaction tx = dbService.beginTx()) {
            tx.acquireWriteLock(relationship);
            tx.success();
        }
    }

    private void acquireWriteLockForSegmentSubgraph(Node segment) {
        // neuron
        acquireWriteLockForNode(segment);
        // connects to relationships and 1-degree connections
        for (Relationship connectsToRelationship : segment.getRelationships(RelationshipType.withName(CONNECTS_TO))) {
            acquireWriteLockForRelationship(connectsToRelationship);
            acquireWriteLockForNode(connectsToRelationship.getOtherNode(segment));
        }
        // skeleton and synapse set and connection sets
        for (Relationship containsRelationship : segment.getRelationships(RelationshipType.withName(CONTAINS))) {
            acquireWriteLockForRelationship(containsRelationship);
            Node skeletonOrSynapseSetOrConnectionSetNode = containsRelationship.getEndNode();
            acquireWriteLockForNode(skeletonOrSynapseSetOrConnectionSetNode);
            // skel nodes and synapses
            for (Relationship skelNodeOrSynapseRelationship : skeletonOrSynapseSetOrConnectionSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                acquireWriteLockForRelationship(skelNodeOrSynapseRelationship);
                Node skelNodeOrSynapseNode = skelNodeOrSynapseRelationship.getEndNode();
                acquireWriteLockForNode(skelNodeOrSynapseNode);
                // first degree relationships to synapses
                for (Relationship synapsesToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                    acquireWriteLockForRelationship(synapsesToRelationship);
                    acquireWriteLockForNode(synapsesToRelationship.getOtherNode(skelNodeOrSynapseNode));
                }
                // links to relationships for skel nodes
                for (Relationship linksToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName(LINKS_TO), Direction.OUTGOING)) {
                    acquireWriteLockForRelationship(linksToRelationship);
                }
            }
        }
    }
}

