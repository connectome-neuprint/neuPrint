package org.janelia.flyem.neuprintprocedures.proofreading;

import apoc.result.NodeListResult;
import apoc.result.NodeResult;
import com.google.gson.Gson;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.SkelNode;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.Synapse;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.janelia.flyem.neuprinter.model.SynapseCountsPerRoi;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
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
import java.util.ArrayList;
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
    private static final String HISTORY = "History";
    private static final String NEURON = "Neuron";
    private static final String SEGMENT = "Segment";
    private static final String SKELETON = "Skeleton";
    private static final String SYNAPSE = "Synapse";
    private static final String SYNAPSE_SET = "SynapseSet";
    private static final String POST_SYN = "PostSyn";
    private static final String PRE_SYN = "PreSyn";
    //Property names
    private static final String BODY_ID = "bodyId";
    private static final String CONFIDENCE = "confidence";
    private static final String DATASET_BODY_ID = "datasetBodyId";
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
    //Relationship names
    private static final String CLEAVED_TO = "CleavedTo";
    private static final String CONNECTS_TO = "ConnectsTo";
    private static final String CONTAINS = "Contains";
    private static final String FROM = "From";
    private static final String LINKS_TO = "LinksTo";
    private static final String MERGED_TO = "MergedTo";
    private static final String SPLIT_TO = "SplitTo";
    private static final String SYNAPSES_TO = "SynapsesTo";
    //prefixes on History Node properties
    private static final String CLEAVED = "cleaved";
    private static final String MERGED = "merged";
    private static final String SPLIT = "split";

//TODO: Decide when to add :Neuron label

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @Procedure(value = "proofreader.mergeNeuronsFromJson", mode = Mode.WRITE)
    @Description("proofreader.mergeNeuronsFromJson(mergeJson,datasetLabel) : merge neurons/segments from json file containing single mergeaction json")
    public Stream<NodeResult> mergeNeuronsFromJson(@Name("mergeJson") String mergeJson, @Name("datasetLabel") String datasetLabel) {

        if (mergeJson == null || datasetLabel == null) {
            log.error("proofreader.mergeNeuronsFromJson: Missing input arguments.");
            throw new RuntimeException("Missing input arguments.");
        }

        // TODO: investigate situation in which timeStamps do not get add to history or merged nodes. possibly just add them to nodes during procedure. is this happening to other, more important nodes?
        // TODO: history nodes need to have unique ids?
        // TODO: can make more efficient by never transferring anything from result node
        // TODO: how does this work with batched transactions if there is a conflict?

        final Gson gson = new Gson();
        final MergeAction mergeAction = gson.fromJson(mergeJson, MergeAction.class);
        if (!mergeAction.getAction().equals("merge")) {
            log.error("proofreader.mergeNeuronsFromJson: Action was not \"merge\".");
            throw new RuntimeException("Action was not \"merge\".");
        }

        List<Node> mergedBodies = new ArrayList<>();
        Set<Long> mergedBodiesNotFound = new HashSet<>();
        for (Long mergedBodyId : mergeAction.getBodiesMerged()) {
            try {
                Node mergedBody = acquireSegmentFromDatabase(mergedBodyId, datasetLabel);
                mergedBodies.add(mergedBody);
            } catch (NoSuchElementException | NullPointerException nse) {
                mergedBodiesNotFound.add(mergedBodyId);
            }
        }

        if (mergedBodiesNotFound.size() > 0) {
            log.info(String.format("proofreader.mergeNeuronsFromJson: The following bodyIds were not found in dataset %s. Ignoring these bodies in merge procedure: " + mergedBodiesNotFound, datasetLabel));
        }

        Node targetBody;
        try {
            targetBody = acquireSegmentFromDatabase(mergeAction.getTargetBodyId(), datasetLabel);
        } catch (NoSuchElementException | NullPointerException nse) {
            log.info(String.format("proofreader.mergeNeuronsFromJson: Target body %d does not exist in dataset %s. Creating target body node and synapse set...", mergeAction.getTargetBodyId(), datasetLabel));
            targetBody = dbService.createNode(Label.label(SEGMENT), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SEGMENT));
            targetBody.setProperty(BODY_ID, mergeAction.getTargetBodyId());
            if (mergeAction.getTargetBodySize() != null) {
                targetBody.setProperty(SIZE, mergeAction.getTargetBodySize());
            }
            if (mergeAction.getTargetBodyName() != null) {
                targetBody.setProperty(NAME, mergeAction.getTargetBodyName());
            }
            if (mergeAction.getTargetBodyStatus() != null) {
                targetBody.setProperty(STATUS, mergeAction.getTargetBodyStatus());
            }
            Node targetBodySynapseSet = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
            targetBodySynapseSet.setProperty(DATASET_BODY_ID, datasetLabel + ":" + mergeAction.getTargetBodyId());
            targetBody.createRelationshipTo(targetBodySynapseSet, RelationshipType.withName(CONTAINS));
        }

        // grab write locks upfront
        acquireWriteLockForSegmentSubgraph(targetBody);
        for (Node body : mergedBodies) {
            acquireWriteLockForSegmentSubgraph(body);
        }

        final Node newNode = recursivelyMergeNodes(targetBody, mergedBodies, mergeAction.getTargetBodySize(), mergeAction.getTargetBodyName(), mergeAction.getTargetBodyStatus(), datasetLabel, mergeAction);

        // throws an error if the synapses from the json and the synapses in the database for the resulting body do not match
//        compareMergeActionSynapseSetWithDatabaseSynapseSet(newNode, mergeAction, datasetLabel);

        log.info(String.format("Completed mergeAction for dataset %s, DVID UUID %s, mutationId %d. targetBodyId: %d, mergedBodies: " + mergeAction.getBodiesMerged() + ". Bodies not found in neo4j: " + mergedBodiesNotFound,
                datasetLabel,
                mergeAction.getDvidUuid(),
                mergeAction.getMutationId(),
                mergeAction.getTargetBodyId()));

        return Stream.of(new NodeResult(newNode));
    }

    @Procedure(value = "proofreader.cleaveNeuronFromJson", mode = Mode.WRITE)
    @Description("proofreader.cleaveNeuronFromJson(cleaveJson,datasetLabel) : cleave neuron/segment from json file containing a single cleaveaction json")
    public Stream<NodeListResult> cleaveNeuronFromJson(@Name("cleaveJson") String cleaveJson, @Name("datasetLabel") String datasetLabel) {

        if (cleaveJson == null || datasetLabel == null) {
            log.error("proofreader.cleaveNeuronsFromJson: Missing input arguments.");
            throw new RuntimeException("Missing input arguments.");
        }

        final Gson gson = new Gson();
        final CleaveOrSplitAction cleaveOrSplitAction = gson.fromJson(cleaveJson, CleaveOrSplitAction.class);

        Node originalBody;
        try {
            originalBody = acquireSegmentFromDatabase(cleaveOrSplitAction.getOriginalBodyId(), datasetLabel);
        } catch (NoSuchElementException | NullPointerException nse) {
            log.info(String.format("proofreader.cleaveNeuronsFromJson: Target body %d does not exist in dataset %s. Creating target body node and synapse set...", cleaveOrSplitAction.getOriginalBodyId(), datasetLabel));
            originalBody = dbService.createNode(Label.label(SEGMENT), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SEGMENT));
            originalBody.setProperty("bodyId", cleaveOrSplitAction.getOriginalBodyId());
            Node originalBodySynapseSet = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
            originalBodySynapseSet.setProperty(DATASET_BODY_ID, datasetLabel + ":" + cleaveOrSplitAction.getOriginalBodyId());
            originalBody.createRelationshipTo(originalBodySynapseSet, RelationshipType.withName(CONTAINS));
        }

        // grab write locks upfront
        acquireWriteLockForSegmentSubgraph(originalBody);

        //check if action is cleave or split
        String typeOfAction = null;
        String historyRelationshipType = null;
        if (cleaveOrSplitAction.getAction().equals("cleave")) {
            typeOfAction = CLEAVED;
            historyRelationshipType = CLEAVED_TO;
        } else if (cleaveOrSplitAction.getAction().equals("split")) {
            typeOfAction = SPLIT;
            historyRelationshipType = SPLIT_TO;
        }
        if (typeOfAction == null) {
            log.error("proofreader.cleaveNeuronsFromJson: Unknown action type. Available actions for json are \"cleave\" or \"split\"");
            throw new RuntimeException("Unknown action type. Available actions for json are \"cleave\" or \"split\"");
        }

        // create new segment nodes with properties
        final Node cleavedOriginalBody = copyPropertiesToNewNodeForCleaveSplitOrMerge(originalBody, typeOfAction);
        final Node cleavedNewBody = dbService.createNode();
        cleavedNewBody.setProperty(BODY_ID, cleaveOrSplitAction.getNewBodyId());
        cleavedNewBody.setProperty(SIZE, cleaveOrSplitAction.getNewBodySize());
        log.info(String.format("Cleaving %s from %s...", cleavedNewBody.getProperty(BODY_ID), cleavedOriginalBody.getProperty(BODY_ID)));

        // original segment synapse set
        Node originalSynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(originalBody);
        if (originalSynapseSetNode == null) {
            log.info(String.format("proofreader.cleaveNeuronsFromJson: No %s node on original body. Creating one...", SYNAPSE_SET));
            originalSynapseSetNode = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
            originalSynapseSetNode.setProperty(DATASET_BODY_ID, datasetLabel + ":" + cleaveOrSplitAction.getOriginalBodyId());
            originalBody.createRelationshipTo(originalSynapseSetNode, RelationshipType.withName(CONTAINS));
//            throw new RuntimeException(String.format("No %s node on original body.", SYNAPSE_SET));
        }
        //create a synapse set for the new body with unique ID
        Node newBodySynapseSetNode = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
        newBodySynapseSetNode.setProperty(DATASET_BODY_ID, datasetLabel + ":" + cleaveOrSplitAction.getNewBodyId());
        //connect both synapse sets to the cleaved nodes
        cleavedOriginalBody.createRelationshipTo(originalSynapseSetNode, RelationshipType.withName(CONTAINS));
        cleavedNewBody.createRelationshipTo(newBodySynapseSetNode, RelationshipType.withName(CONTAINS));

        //move synapses when necessary
        final ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
        final Set<Synapse> originalBodySynapseSet = new HashSet<>();
        final Set<Synapse> newBodySynapseSet = cleaveOrSplitAction.getNewBodySynapses();
        SynapseCounter originalBodySynapseCounter = new SynapseCounter();
        SynapseCounter newBodySynapseCounter = new SynapseCounter();

        //need to set roi for new body synapses from database
        for (Synapse synapse : newBodySynapseSet) {
            setSynapseRoisFromDatabase(synapse, datasetLabel);
        }

        if (originalSynapseSetNode.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            for (Relationship synapseRelationship : originalSynapseSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                Node synapseNode = synapseRelationship.getEndNode();
                try {
                    //create a synapse object from synapse node
                    List<Integer> synapseLocation = getNeo4jPointLocationAsLocationList((Point) synapseNode.getProperty(LOCATION));
                    Synapse synapse = new Synapse((String) synapseNode.getProperty(TYPE),
                            synapseLocation.get(0),
                            synapseLocation.get(1),
                            synapseLocation.get(2),
                            getSynapseNodeRoiSet(synapseNode));
                    if (newBodySynapseSet.contains(synapse)) {
                        //delete connection to original synapse set
                        synapseRelationship.delete();
                        //add connection to new synapse set
                        newBodySynapseSetNode.createRelationshipTo(synapseNode, RelationshipType.withName(CONTAINS));
                        //store connects to relationship to add later
                        addSynapseToConnectsToRelationshipMap(synapseNode, synapse, cleavedNewBody, connectsToRelationshipMap);
                        incrementSynapseCounterWithSynapse(synapse, newBodySynapseCounter);
                    } else {
                        //keep synapse in place but keep track of which synapse this is. necessary?
                        originalBodySynapseSet.add(synapse);
                        addSynapseToConnectsToRelationshipMap(synapseNode, synapse, cleavedOriginalBody, connectsToRelationshipMap);
                        incrementSynapseCounterWithSynapse(synapse, originalBodySynapseCounter);
                    }
                } catch (org.neo4j.graphdb.NotFoundException nfe) {
                    nfe.printStackTrace();
                    log.error(String.format("proofreader.cleaveNeuronsFromJson: %s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
                    throw new RuntimeException(String.format("%s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
                }
            }
        }

        //recreate connects to relationships
        addConnectsToRelationshipsFromMap(connectsToRelationshipMap);

        //recreate synapseCountPerRoi
        //cleavedNewBody
        SynapseCountsPerRoi newBodySynapseCountsPerRoi = BodyWithSynapses.getSynapseCountersPerRoiFromSynapseSet(newBodySynapseSet);
        cleavedNewBody.setProperty(ROI_INFO, newBodySynapseCountsPerRoi.getAsJsonString());
        //cleavedOriginalBody
        SynapseCountsPerRoi originalBodySynapseCountsPerRoi = BodyWithSynapses.getSynapseCountersPerRoiFromSynapseSet(originalBodySynapseSet);
        cleavedOriginalBody.setProperty(ROI_INFO, originalBodySynapseCountsPerRoi.getAsJsonString());

        //add proper roi labels to Segment
        addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(cleavedNewBody, newBodySynapseCountsPerRoi);
        addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(cleavedOriginalBody, originalBodySynapseCountsPerRoi);
        //update pre, post properties for new nodes
        cleavedNewBody.setProperty(PRE, newBodySynapseCounter.getPre());
        cleavedNewBody.setProperty(POST, newBodySynapseCounter.getPost());
        cleavedOriginalBody.setProperty(PRE, originalBodySynapseCounter.getPre());
        cleavedOriginalBody.setProperty(POST, originalBodySynapseCounter.getPost());

        //delete old skeleton
        deleteSkeletonForNode(originalBody);
        //delete connects to
        removeConnectsToRelationshipsForNode(originalBody);

        collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(originalBody);

        // create a new history node
        Node newBodyHistoryNode = dbService.createNode(Label.label(HISTORY), Label.label(datasetLabel));
        Node originalBodyHistoryNode = dbService.createNode(Label.label(HISTORY), Label.label(datasetLabel));
        // connect history node to new Segment
        cleavedNewBody.createRelationshipTo(newBodyHistoryNode, RelationshipType.withName(FROM));
        cleavedOriginalBody.createRelationshipTo(originalBodyHistoryNode, RelationshipType.withName(FROM));
        // connect ghost nodes to history Segment
        originalBody.createRelationshipTo(originalBodyHistoryNode, RelationshipType.withName(historyRelationshipType));
        originalBody.createRelationshipTo(newBodyHistoryNode, RelationshipType.withName(historyRelationshipType));

        //add dviduuid and mutationid, remove labels, all properties to cleaved or split
        originalBody.setProperty("dvidUuid", cleaveOrSplitAction.getDvidUuid());
        originalBody.setProperty("mutationId", cleaveOrSplitAction.getMutationId());
        convertAllPropertiesToCleavedOrSplitProperties(originalBody, typeOfAction);
        removeAllLabels(originalBody);
        List<String> except = new ArrayList<>();
        except.add(MERGED_TO);
        except.add(CLEAVED_TO);
        except.add(SPLIT_TO);
        removeAllRelationshipsExceptTypesWithName(originalBody, except);

        //add dataset and Segment labels to new nodes
        cleavedOriginalBody.addLabel(Label.label(SEGMENT));
        cleavedOriginalBody.addLabel(Label.label(datasetLabel));
        cleavedOriginalBody.addLabel(Label.label(datasetLabel + "-" + SEGMENT));
        cleavedNewBody.addLabel(Label.label(SEGMENT));
        cleavedNewBody.addLabel(Label.label(datasetLabel));
        cleavedNewBody.addLabel(Label.label(datasetLabel + "-" + SEGMENT));

        List<Node> listOfCleavedNodes = new ArrayList<>();
        listOfCleavedNodes.add(cleavedNewBody);
        listOfCleavedNodes.add(cleavedOriginalBody);

        log.info(String.format("Completed cleaveOrSplitAction for dataset %s, DVID UUID %s, mutationId %d, actionType: %s. originalBodyId: %d. Created new body with bodyId: %d.",
                datasetLabel,
                cleaveOrSplitAction.getDvidUuid(),
                cleaveOrSplitAction.getMutationId(),
                cleaveOrSplitAction.getAction(),
                cleaveOrSplitAction.getOriginalBodyId(),
                cleaveOrSplitAction.getNewBodyId()));

        return Stream.of(new NodeListResult(listOfCleavedNodes));

    }

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

    @Procedure(value = "proofreader.deleteNeuron", mode = Mode.WRITE)
    @Description("proofreader.deleteNeuron(neuronBodyId,datasetLabel) : delete neuron/segment with body Id and associated nodes (except Synapses) from given dataset. ")
    public void deleteNeuron(@Name("neuronBodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {
        if (bodyId == null || datasetLabel == null) {
            log.error("Must provide both a bodyId and dataset label.");
            throw new RuntimeException("Must provide both a bodyId and dataset label.");
        }

        final Node segment = acquireSegmentFromDatabase(bodyId, datasetLabel);

        // grab write locks upfront
        acquireWriteLockForSegmentSubgraph(segment);

        //delete History
//        removeHistoryForNode(neuron);

        //delete connectsTo relationships
        removeConnectsToRelationshipsForNode(segment);

        //delete synapse set and synapses
        removeSynapseSetsAndContainsRelationshipsToSynapses(segment);

        //delete skeleton
        deleteSkeletonForNode(segment);

        segment.delete();

    }

    @Procedure(value = "proofreader.addNeuron", mode = Mode.WRITE)
    @Description("proofreader.addNeuron(neuronJson,datasetLabel) : add neuron/segment specified in json to given dataset.")
    public Stream<NodeResult> addNeuron(@Name("neuronJson") String neuronJson, @Name("datasetLabel") String datasetLabel) {
        if (neuronJson == null || datasetLabel == null) {
            log.error("Must provide a neuron json and a dataset label.");
            throw new RuntimeException("Must provide a neuron json and a dataset label.");
        }
        //grab write locks on connected nodes (synapses, neurons)

        //add neuron node with properties and labels

        //add connects to relationships

        //add synapseset and connect to synapses

        //add skeleton?

        return Stream.of(new NodeResult(null));

    }

    private Node recursivelyMergeNodes(Node resultNode, List<Node> mergedBodies, Long newNodeSize, String newNodeName, String newNodeStatus, String datasetLabel, MergeAction mergeAction) {
        if (mergedBodies.size() == 0) {
            if ((!resultNode.hasProperty(SIZE) || (resultNode.hasProperty(SIZE) && !resultNode.getProperty(SIZE).equals(newNodeSize))) && newNodeSize != null) {
                resultNode.setProperty(SIZE, newNodeSize);
            }
            if ((!resultNode.hasProperty(NAME) || (resultNode.hasProperty(NAME) && !resultNode.getProperty(NAME).equals(newNodeName))) && newNodeName != null) {
                resultNode.setProperty(NAME, newNodeName);
            }
            if ((!resultNode.hasProperty(STATUS) || (resultNode.hasProperty(STATUS) && !resultNode.getProperty(STATUS).equals(newNodeStatus))) && newNodeStatus != null) {
                resultNode.setProperty(STATUS, newNodeStatus);
            }
            return resultNode;
        } else {
            Node mergedNode = mergeTwoNodesOntoNewNode(resultNode, mergedBodies.get(0), newNodeSize, newNodeName, newNodeStatus, datasetLabel, mergeAction);
            return recursivelyMergeNodes(mergedNode, mergedBodies.subList(1, mergedBodies.size()), newNodeSize, newNodeName, newNodeStatus, datasetLabel, mergeAction);
        }
    }

    private Node mergeTwoNodesOntoNewNode(Node node1, Node node2, Long newNodeSize, String newNodeName, String newNodeStatus, String datasetLabel, MergeAction mergeAction) {

        //create a new node that will be a copy of node1
        //copy properties over to new node (note: must add properties before adding labels or will violate uniqueness constraint)
        final Node newNode = copyPropertiesToNewNodeForCleaveSplitOrMerge(node1, MERGED);
        // add size, name, status property from json to new node
        if (newNodeSize != null) {
            newNode.setProperty(SIZE, newNodeSize);
        }
        if (newNodeName != null) {
            newNode.setProperty(NAME, newNodeName);
        }
        if (newNodeStatus != null) {
            newNode.setProperty(STATUS, newNodeStatus);
        }
        log.info(String.format("Merging %s with %s...", node1.getProperty(BODY_ID), node2.getProperty(BODY_ID)));

        //move all relationships from original result body Id (A) to new node
        //move all relationships from 1st merged node (B) to new node
        mergeConnectsToRelationshipsToNewNodeAndDeletePreviousConnectsToRelationships(node1, node2, newNode);
        log.info(String.format("Merged %s relationships from original neurons onto new neuron.", CONNECTS_TO));
        mergeSynapseSetsOntoNewNodeAndRemoveFromPreviousNodes(node1, node2, newNode);
        log.info(String.format("Merged %s nodes from original neurons onto new neuron.", SYNAPSE_SET));

        //regenerate synapseCountPerRoi property
        Set<Synapse> mergedSynapseSet = createSynapseSetForSegment(newNode, datasetLabel);
        SynapseCountsPerRoi synapseCountsPerRoi = BodyWithSynapses.getSynapseCountersPerRoiFromSynapseSet(mergedSynapseSet);
        newNode.setProperty(ROI_INFO, synapseCountsPerRoi.getAsJsonString());

        //add boolean roi props
        for (String roi : synapseCountsPerRoi.getSetOfRois()) {
            newNode.setProperty(roi, true);
        }

        //add pre and post count from merged node to new node
        setSummedLongProperty(PRE, newNode, node2, newNode);
        setSummedLongProperty(POST, newNode, node2, newNode);

        log.info("Regenerated pre, post, and roiInfo properties on new node.");

        //delete skeletons for both nodes
        deleteSkeletonForNode(node2);
        deleteSkeletonForNode(node1);
        log.info(String.format("Deleted %ss for original nodes.", SKELETON));

        //store history
        collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(node1);
        collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(node2);

        // create a new history node
        Node newHistoryNode = dbService.createNode(Label.label(HISTORY), Label.label(datasetLabel));
        // connect history node to new Segment
        newNode.createRelationshipTo(newHistoryNode, RelationshipType.withName(FROM));
        // connect ghost nodes to history Segment
        node1.createRelationshipTo(newHistoryNode, RelationshipType.withName(MERGED_TO));
        node2.createRelationshipTo(newHistoryNode, RelationshipType.withName(MERGED_TO));

        // create ghost nodes for original bodies: property names converted to "merged" property names,
        // all labels removed, all relationships removed (except need to deal with history)
        // add dviduuid and mutationid
        node1.setProperty("dvidUuid", mergeAction.getDvidUuid());
        node1.setProperty("mutationId", mergeAction.getMutationId());
        node2.setProperty("dvidUuid", mergeAction.getDvidUuid());
        node2.setProperty("mutationId", mergeAction.getMutationId());
        convertAllPropertiesToMergedProperties(node1);
        convertAllPropertiesToMergedProperties(node2);
        // copy all the labels to the new node
        transferLabelsToNode(node1, newNode);
        transferLabelsToNode(node2, newNode);
        removeAllLabels(node1);
        removeAllLabels(node2);
        List<String> except = new ArrayList<>();
        except.add(MERGED_TO);
        except.add(CLEAVED_TO);
        except.add(SPLIT_TO);
        removeAllRelationshipsExceptTypesWithName(node1, except);
        removeAllRelationshipsExceptTypesWithName(node2, except);

        log.info("Created History and ghost nodes for merge.");

        return newNode;
    }

    private Node copyPropertiesToNewNodeForCleaveSplitOrMerge(Node originalNode, String typeOfCopy) throws IllegalArgumentException {
        // typeOfCopy can be "merge", "cleave", or "split"
        final Node newNode = dbService.createNode();

        String ghostNodePropertiesPrefix;
        switch (typeOfCopy) {
            case MERGED:
                ghostNodePropertiesPrefix = MERGED;
                break;
            case CLEAVED:
                ghostNodePropertiesPrefix = CLEAVED;
                break;
            case SPLIT:
                ghostNodePropertiesPrefix = SPLIT;
                break;
            default:
                throw new IllegalArgumentException(String.format("Illegal typeOfCopy: must be %s, %s, or %s.", MERGED, CLEAVED, SPLIT));
        }
        //copy all the properties to the new node. if they start with merged/cleaved then remove merged/cleaved
        Map<String, Object> originalNodeProperties = originalNode.getAllProperties();
        for (String property : originalNodeProperties.keySet()) {
            String propertyWithoutMerged = property;
            if (property.startsWith(ghostNodePropertiesPrefix)) {
                propertyWithoutMerged = property.replaceFirst(ghostNodePropertiesPrefix, "");
                propertyWithoutMerged = propertyWithoutMerged.substring(0, 1).toLowerCase() + propertyWithoutMerged.substring(1);
            }
            newNode.setProperty(propertyWithoutMerged, originalNodeProperties.get(property));
        }

        return newNode;

    }

    private void mergeConnectsToRelationshipsToNewNodeAndDeletePreviousConnectsToRelationships(Node node1, Node node2, Node newNode) {
        //duplicate all relationships for this body from the other two bodies
        //get a map of all relationships to add
        ConnectsToRelationshipMap connectsToRelationshipMap = getConnectsToRelationshipMapForNodesAndDeletePreviousRelationships(node1, node2, newNode);
        addConnectsToRelationshipsFromMap(connectsToRelationshipMap);
    }

    private void addConnectsToRelationshipsFromMap(ConnectsToRelationshipMap connectsToRelationshipMap) {
        //add relationships to new node
        for (String stringKey : connectsToRelationshipMap.getNodeIdToConnectsToRelationshipHashMap().keySet()) {
            ConnectsToRelationship connectsToRelationship = connectsToRelationshipMap.getConnectsToRelationshipByKey(stringKey);
            Node startNode = connectsToRelationship.getStartNode();
            Node endNode = connectsToRelationship.getEndNode();
            Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(CONNECTS_TO));
            relationship.setProperty(WEIGHT, connectsToRelationship.getWeight());
        }
    }

    private void addSynapseToConnectsToRelationshipMap(Node synapseNode, Synapse synapse, Node segmentWithSynapse, ConnectsToRelationshipMap connectsToRelationshipMap) {
        final long weightOfOne = 1L;
        // get the node that this synapse connects to
        List<Node> listOfConnectedSegments = getPostOrPreSynapticSegmentGivenSynapse(synapseNode);
        // add to the connectsToRelationshipMap
        for (Node connectedSegment : listOfConnectedSegments) {
            if (synapse.getType().equals(PRE)) {
                connectsToRelationshipMap.insertConnectsToRelationship(segmentWithSynapse, connectedSegment, weightOfOne);
            } else if (synapse.getType().equals(POST)) {
                connectsToRelationshipMap.insertConnectsToRelationship(connectedSegment, segmentWithSynapse, weightOfOne);
            }
        }
    }

    private void mergeSynapseSetsOntoNewNodeAndRemoveFromPreviousNodes(Node node1, Node node2, Node newNode) {

        //get the synapse sets for node1 and node2, remove them from original nodes
        Node node1SynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node1);
        Node node2SynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node2);

        //add both synapse sets to the new node, collect them for adding to apoc merge procedure
        Map<String, Object> parametersMap = new HashMap<>();
        if (node1SynapseSetNode != null) {
            newNode.createRelationshipTo(node1SynapseSetNode, RelationshipType.withName(CONTAINS));
            parametersMap.put("ssnode1", node1SynapseSetNode);
        }
        if (node2SynapseSetNode != null) {
            newNode.createRelationshipTo(node2SynapseSetNode, RelationshipType.withName(CONTAINS));
            parametersMap.put("ssnode2", node2SynapseSetNode);
        }

        // merge the two synapse nodes using apoc if there are two synapseset nodes. inherits the datasetBodyId of the first node
        if (parametersMap.containsKey("ssnode1") && parametersMap.containsKey("ssnode2")) {
            dbService.execute("CALL apoc.refactor.mergeNodes([$ssnode1, $ssnode2], {properties:{datasetBodyId:\"discard\"}}) YIELD node RETURN node", parametersMap).next().get("node");
            //delete the extra relationship between new node and new synapse set node
            newNode.getRelationships(RelationshipType.withName(CONTAINS)).iterator().next().delete();
        }

    }

    private Set<Synapse> createSynapseSetForSegment(Node segment, String datasetLabel) {

        Node synapseSetNode = getSynapseSetNodeForSegment(segment);
        if (synapseSetNode == null) {
            log.info(String.format("ProofreaderProcedures createSynapseSetForSegment: No %s on neuron %s. Creating one...", SYNAPSE_SET, segment.getProperty("bodyId")));
            synapseSetNode = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
            synapseSetNode.setProperty(DATASET_BODY_ID, datasetLabel + ":" + segment.getProperty("bodyId"));
            segment.createRelationshipTo(synapseSetNode, RelationshipType.withName(CONTAINS));
//            throw new RuntimeException(String.format("No %s on neuron.", SYNAPSE_SET));
        }

        Set<Synapse> synapseSet = new HashSet<>();
        //make sure there are synapses attached to the synapse set
        if (synapseSetNode.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            for (Relationship synapseRelationship : synapseSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                Node synapseNode = synapseRelationship.getEndNode();
                try {
                    List<Integer> synapseLocation = getNeo4jPointLocationAsLocationList((Point) synapseNode.getProperty(LOCATION));
                    Synapse synapse = new Synapse((String) synapseNode.getProperty(TYPE),
                            synapseLocation.get(0),
                            synapseLocation.get(1),
                            synapseLocation.get(2),
                            getSynapseNodeRoiSet(synapseNode));
                    synapseSet.add(synapse);
                } catch (org.neo4j.graphdb.NotFoundException nfe) {
                    nfe.printStackTrace();
                    log.error(String.format("ProofreaderProcedures createSynapseSetForSegment: %s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
                    throw new RuntimeException(String.format("%s does not have %s or %s properties: %s ", SYNAPSE, TYPE, LOCATION, synapseNode.getAllProperties()));
                }
            }
        }

        return synapseSet;
    }

    private void deleteSkeletonForNode(final Node node) {

        Node nodeSkeletonNode = null;
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName(CONTAINS))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label(SKELETON))) {
                nodeSkeletonNode = containedNode;
                //delete Contains connection to original node
                nodeRelationship.delete();
            }
        }

        if (nodeSkeletonNode != null) {
            for (Relationship skeletonRelationship : nodeSkeletonNode.getRelationships(RelationshipType.withName(CONTAINS))) {
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
            nodeSkeletonNode.delete();
        }
    }

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

    private void compareMergeActionSynapseSetWithDatabaseSynapseSet(Node resultingBody, MergeAction mergeAction, String datasetLabel) {

        Node synapseSet = getSynapseSetNodeForSegment(resultingBody);
        if (synapseSet == null) {
            log.error(String.format("ProofreaderProcedures compareMergeActionSynapseSetWithDatabaseSynapseSet: No %s on the resulting body.", SYNAPSE_SET));
            throw new RuntimeException(String.format("No %s on the resulting body.", SYNAPSE_SET));
        }

        Set<Synapse> resultBodySynapseSet = createSynapseSetForSegment(resultingBody, datasetLabel);

        //compare the two sets
        Set<Synapse> mergeActionSynapseSet = new HashSet<>(mergeAction.getTargetBodySynapses());
        Set<Synapse> databaseSynapseSet = new HashSet<>(resultBodySynapseSet);

        mergeActionSynapseSet.removeAll(resultBodySynapseSet);
        databaseSynapseSet.removeAll(mergeAction.getTargetBodySynapses());

        if (mergeActionSynapseSet.size() == 0 && databaseSynapseSet.size() == 0) {
            log.info("Database and merge action synapses match.");
        } else {
            log.error(String.format("ProofreaderProcedures compareMergeActionSynapseSetWithDatabaseSynapseSet: Found the following differences between the database and merge action synapse sets: \n" +
                    "* Synapses in merge action but not in database: %s \n" +
                    "* Synapses in database but not in merge action: %s \n", mergeActionSynapseSet, databaseSynapseSet));
            throw new RuntimeException(String.format("Found the following differences between the database and merge action synapse sets: \n" +
                    "* Synapses in merge action but not in database: %s \n" +
                    "* Synapses in database but not in merge action: %s \n", mergeActionSynapseSet, databaseSynapseSet));
        }

    }

    private List<String> getRoisForSynapse(Synapse synapse, String datasetLabel) {

        Map<String, Object> roiQueryResult;
        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("x", (double) synapse.getX());
        parametersMap.put("y", (double) synapse.getY());
        parametersMap.put("z", (double) synapse.getZ());

        try {
            roiQueryResult = dbService.execute("MATCH (s:`" + datasetLabel + "-Synapse`) WHERE s.location=neuprint.locationAs3dCartPoint($x,$y,$z) WITH keys(s) AS props " +
                    "RETURN filter(prop IN props WHERE " +
                    "NOT prop=\"type\" AND " +
                    "NOT prop=\"confidence\" AND " +
                    "NOT prop=\"location\" AND " +
                    "NOT prop=\"timeStamp\") AS l", parametersMap).next();
        } catch (java.util.NoSuchElementException nse) {
            nse.printStackTrace();
            log.error(String.format("ProofreaderProcedures getRoisForSynapse: Error using proofreader procedures: %s not found in the dataset.", SYNAPSE));
            throw new RuntimeException(String.format("Error using proofreader procedures: %s not found in the dataset.", SYNAPSE));
        }

        return (ArrayList<String>) roiQueryResult.get("l");
    }

    private void setSynapseRoisFromDatabase(Synapse synapse, String datasetLabel) {
        List<String> roiList = getRoisForSynapse(synapse, datasetLabel);
        if (roiList.size() == 0) {
            log.info(String.format("ProofreaderProcedures setSynapseRoisFromDatabase: No roi found on %s: %s", SYNAPSE, synapse));
            //throw new RuntimeException(String.format("No roi found on %s: %s", SYNAPSE, synapse));
        }
        synapse.addRoiSet(new HashSet<>(roiList));
    }

    private Set<String> getSynapseNodeRoiSet(Node synapseNode) {
        Map<String, Object> synapseNodeProperties = synapseNode.getAllProperties();
        return synapseNodeProperties.keySet().stream()
                .filter(p -> (
                        !p.equals(TIME_STAMP) &&
                                !p.equals(LOCATION) &&
                                !p.equals(TYPE) &&
                                !p.equals(CONFIDENCE))
                )
                .collect(Collectors.toSet());
    }

    private ConnectsToRelationshipMap getConnectsToRelationshipMapForNodesAndDeletePreviousRelationships(final Node node1, final Node node2, final Node newNode) {

        ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
        List<Node> nodeList = new ArrayList<>();
        nodeList.add(node1);
        nodeList.add(node2);

        for (Node node : nodeList) {
            for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName(CONNECTS_TO))) {
                //get weight for relationship
                Long weight = nodeRelationship.hasProperty(WEIGHT) ? (Long) nodeRelationship.getProperty(WEIGHT) : 0L;

                Node startNode = nodeRelationship.getStartNode();
                Node endNode = nodeRelationship.getEndNode();
                //don't count node1-to-node2 and node2-to-node1 relationships twice by not counting them for the second node
                if (((startNode.equals(node1) && endNode.equals(node2)) || (startNode.equals(node2) && endNode.equals(node1))) && node.equals(node2)) {
                    continue;
                } else {
                    //replace node with newNode in start and end when necessary
                    startNode = (startNode.equals(node1) || startNode.equals(node2)) ? newNode : startNode;
                    endNode = (endNode.equals(node1) || endNode.equals(node2)) ? newNode : endNode;
                    //store this relationship. if the relationship is already present in the map, will simply add the weight
                    connectsToRelationshipMap.insertConnectsToRelationship(startNode, endNode, weight);
                }
                // delete the old relationship
                nodeRelationship.delete();
            }
        }
        return connectsToRelationshipMap;
    }

    private Node getSynapseSetForNodeAndDeleteConnectionToNode(final Node node) {
        Node nodeSynapseSetNode = null;
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName(CONTAINS))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label(SYNAPSE_SET))) {
                nodeSynapseSetNode = containedNode;
                //delete Contains connection to original node
                nodeRelationship.delete();
            }
        }
        return nodeSynapseSetNode;
    }

    private Node getSynapseSetNodeForSegment(final Node node) {
        Node nodeSynapseSetNode = null;
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName(CONTAINS))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label(SYNAPSE_SET))) {
                nodeSynapseSetNode = containedNode;
            }
        }
        return nodeSynapseSetNode;
    }

    private void transferLabelsToNode(Node originalNode, Node transferToNode) {
        for (Label originalNodeLabel : originalNode.getLabels()) {
            transferToNode.addLabel(originalNodeLabel);
        }
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

    private List<Node> getPostOrPreSynapticSegmentGivenSynapse(Node synapse) {
        // list so that multiple connections per neuron are each counted as 1 additional weight
        List<Node> listOfConnectedSegments = new ArrayList<>();
        if (synapse.hasRelationship(RelationshipType.withName(SYNAPSES_TO))) {
            for (Relationship synapsesToRelationship : synapse.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                Node connectedSynapse = synapsesToRelationship.getOtherNode(synapse);
                Node connectedSegment;
                try {
                    Node connectedSynapseSet = connectedSynapse.getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING).getStartNode();
                    connectedSegment = connectedSynapseSet.getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING).getStartNode();
                } catch (NoSuchElementException nse) {
                    log.error(String.format("Connected %s has no %s or connected %ss: %s", SYNAPSE, SYNAPSE_SET, SEGMENT, connectedSynapse.getAllProperties()));
                    throw new RuntimeException(String.format("Connected %s has no %s or connected %ss: %s", SYNAPSE, SYNAPSE_SET, SEGMENT, connectedSynapse.getAllProperties()));
                }
                listOfConnectedSegments.add(connectedSegment);
            }
        }
        return listOfConnectedSegments;
    }

    private void incrementSynapseCounterWithSynapse(Synapse synapse, SynapseCounter synapseCounter) {
        if (synapse.getType().equals(PRE)) {
            synapseCounter.incrementPre();
        } else if (synapse.getType().equals(POST)) {
            synapseCounter.incrementPost();
        }
    }

    private void addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(Node segment, SynapseCountsPerRoi synapseCountsPerRoi) {
        for (String roi : synapseCountsPerRoi.getSetOfRois()) {
            segment.setProperty(roi, true);
        }
    }

    private void setSummedLongProperty(String propertyName, Node node1, Node node2, Node newNode) {
        Long summedValue = 0L;
        if (node1.hasProperty(propertyName)) {
            Long node1Property = (Long) node1.getProperty(propertyName);
            summedValue += node1Property;
        }
        if (node2.hasProperty(propertyName)) {
            Long node2Property = (Long) node2.getProperty(propertyName);
            summedValue += node2Property;
        }
        newNode.setProperty(propertyName, summedValue);
    }

    private void convertPropertyNameToNewPropertyName(String propertyName, String newPropertyName, Node node) {
        if (node.hasProperty(propertyName)) {
            Object node1Property = node.getProperty(propertyName);
            node.setProperty(newPropertyName, node1Property);
            node.removeProperty(propertyName);
        }

    }

    private void convertAllPropertiesToMergedProperties(Node node) {
        Map<String, Object> nodeProperties = node.getAllProperties();
        for (String propertyName : nodeProperties.keySet()) {
            if (!propertyName.startsWith(MERGED)) {
                String mergedPropertyName = MERGED + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
                convertPropertyNameToNewPropertyName(propertyName, mergedPropertyName, node);
            }
        }
    }

    private void convertAllPropertiesToCleavedOrSplitProperties(Node node, String typeOfAction) {
        Map<String, Object> nodeProperties = node.getAllProperties();
        for (String propertyName : nodeProperties.keySet()) {
            if (!propertyName.startsWith(typeOfAction)) {
                String cleavedPropertyName = typeOfAction + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
                convertPropertyNameToNewPropertyName(propertyName, cleavedPropertyName, node);
            }
        }
    }

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

    private void removeConnectsToRelationshipsForNode(Node node) {
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName(CONNECTS_TO))) {
            nodeRelationship.delete();
        }
    }

    private void removeSynapseSetsAndContainsRelationshipsToSynapses(Node node) {
        Node synapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node);
        for (Relationship ssRelationship : synapseSetNode.getRelationships(RelationshipType.withName(CONTAINS))) {
            Node synapseNode = ssRelationship.getEndNode();
            removeAllRelationships(synapseNode);
        }
        synapseSetNode.delete();
    }

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
        // skeleton and synapse set
        for (Relationship containsRelationship : segment.getRelationships(RelationshipType.withName(CONTAINS))) {
            acquireWriteLockForRelationship(containsRelationship);
            Node skeletonOrSynapseSetNode = containsRelationship.getEndNode();
            acquireWriteLockForNode(skeletonOrSynapseSetNode);
            // skel nodes and synapses
            for (Relationship skelNodeOrSynapseRelationship : skeletonOrSynapseSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
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

