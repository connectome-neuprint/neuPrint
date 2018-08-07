package org.janelia.flyem.neuprintprocedures;

import apoc.result.NodeListResult;
import apoc.result.NodeResult;
import com.google.gson.Gson;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.NeuronPart;
import org.janelia.flyem.neuprinter.model.SkelNode;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.Synapse;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
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

public class ProofreaderProcedures {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @Procedure(value = "proofreader.mergeNeuronsFromJson", mode = Mode.WRITE)
    @Description("proofreader.mergeNeuronsFromJson(mergeJson,datasetLabel) : merge neurons from json file containing single mergeaction json")
    public Stream<NodeResult> mergeNeuronsFromJson(@Name("mergeJson") String mergeJson, @Name("datasetLabel") String datasetLabel) {

        if (mergeJson == null || datasetLabel == null) {
            throw new Error("Missing input arguments.");
        }

        // TODO: history nodes need to have unique ids?
        // TODO: can make more efficient by never transferring anything from result node
        // TODO: how does this work with batched transactions if there is a conflict?

        final Gson gson = new Gson();
        final MergeAction mergeAction = gson.fromJson(mergeJson, MergeAction.class);

        final List<Node> mergedBodies = mergeAction.getBodiesMerged()
                .stream()
                .map((id) -> acquireNeuronFromDatabase(id, datasetLabel))
                .collect(Collectors.toList());
        final Node targetBody = acquireNeuronFromDatabase(mergeAction.getTargetBodyId(), datasetLabel);

        // grab write locks upfront
        acquireWriteLockForNeuronSubgraph(targetBody);
        for (Node body : mergedBodies) {
            acquireWriteLockForNeuronSubgraph(body);
        }

        final Node newNode = recursivelyMergeNodes(targetBody, mergedBodies, mergeAction.getTargetBodySize(), datasetLabel);

        // throws an error if the synapses from the json and the synapses in the database for the resulting body do not match
        compareMergeActionSynapseSetWithDatabaseSynapseSet(newNode, mergeAction);

        return Stream.of(new NodeResult(newNode));
    }

    @Procedure(value = "proofreader.cleaveNeuronFromJson", mode = Mode.WRITE)
    @Description("proofreader.cleaveNeuronFromJson(cleaveJson,datasetLabel) : cleave neuron from json file containing a single cleaveaction json")
    public Stream<NodeListResult> cleaveNeuronFromJson(@Name("cleaveJson") String cleaveJson, @Name("datasetLabel") String datasetLabel) {

        if (cleaveJson == null || datasetLabel == null) {
            throw new Error("Missing input arguments.");
        }

        final Gson gson = new Gson();
        final CleaveAction cleaveAction = gson.fromJson(cleaveJson, CleaveAction.class);

        final Node originalBody = acquireNeuronFromDatabase(cleaveAction.getOriginalBodyId(), datasetLabel);

        // grab write locks upfront
        acquireWriteLockForNeuronSubgraph(originalBody);

        // create new neuron nodes with properties
        final Node cleavedOriginalBody = copyPropertiesToNewNodeForCleaveOrMerge(originalBody, "cleave");
        final Node cleavedNewBody = dbService.createNode();
        cleavedNewBody.setProperty("bodyId", cleaveAction.getNewBodyId());
        cleavedNewBody.setProperty("size", cleaveAction.getNewBodySize());
        log.info("Cleaving " + cleavedNewBody.getProperty("bodyId") + " from " + cleavedOriginalBody.getProperty("bodyId") + "...");

        // original neuron synapse set
        Node originalSynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(originalBody);
        if (originalSynapseSetNode == null) {
            throw new Error("No synapse set node on original body.");
        }
        //create a synapse set for the new body with unique ID
        Node newBodySynapseSetNode = dbService.createNode(Label.label("SynapseSet"), Label.label(datasetLabel));
        newBodySynapseSetNode.setProperty("datasetBodyId", datasetLabel + ":" + cleaveAction.getNewBodyId());
        //connect both synapse sets to the cleaved nodes
        cleavedOriginalBody.createRelationshipTo(originalSynapseSetNode, RelationshipType.withName("Contains"));
        cleavedNewBody.createRelationshipTo(newBodySynapseSetNode, RelationshipType.withName("Contains"));

        //move synapses when necessary
        final ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
        final Set<Synapse> originalBodySynapseSet = new HashSet<>();
        final Set<Synapse> newBodySynapseSet = cleaveAction.getNewBodySynapses();
        SynapseCounter originalBodySynapseCounter = new SynapseCounter();
        SynapseCounter newBodySynapseCounter = new SynapseCounter();

        //need to set roi for new body synapses from database
        for (Synapse synapse : newBodySynapseSet) {
            setSynapseRoisFromDatabase(synapse, datasetLabel);
        }

        if (originalSynapseSetNode.hasRelationship(RelationshipType.withName("Contains"), Direction.OUTGOING)) {
            for (Relationship synapseRelationship : originalSynapseSetNode.getRelationships(RelationshipType.withName("Contains"), Direction.OUTGOING)) {
                Node synapseNode = synapseRelationship.getEndNode();
                try {
                    //create a synapse object from synapse node
                    Synapse synapse = new Synapse((String) synapseNode.getProperty("type"), (int) (long) synapseNode.getProperty("x"), (int) (long) synapseNode.getProperty("y"), (int) (long) synapseNode.getProperty("z"));
                    //add roi from database
                    setSynapseRoisFromDatabase(synapse, datasetLabel);
                    if (newBodySynapseSet.contains(synapse)) {
                        //delete connection to original synapse set
                        synapseRelationship.delete();
                        //add connection to new synapse set
                        newBodySynapseSetNode.createRelationshipTo(synapseNode, RelationshipType.withName("Contains"));
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
                    throw new Error("synapse does not have type, x, y, or z properties: " + synapseNode.getAllProperties());
                }
            }
        }

        //recreate connects to relationships
        addConnectsToRelationshipsFromMap(connectsToRelationshipMap);

        //delete neuron parts
        removeNeuronPartsForNode(originalBody);

        //recreate neuron parts
        //cleavedNewBody
        List<NeuronPart> newBodyNeuronParts = BodyWithSynapses.getNeuronPartsFromSynapseSet(newBodySynapseSet);
        createNeuronPartNodesAndConnectToGivenNeuron(newBodyNeuronParts, datasetLabel, cleavedNewBody);
        //cleavedOriginalBody
        List<NeuronPart> originalBodyNeuronParts = BodyWithSynapses.getNeuronPartsFromSynapseSet(originalBodySynapseSet);
        createNeuronPartNodesAndConnectToGivenNeuron(originalBodyNeuronParts, datasetLabel, cleavedOriginalBody);

        //add proper roi labels to Neuron
        addRoiLabelsToNeuronGivenNeuronParts(cleavedNewBody, newBodyNeuronParts);
        addRoiLabelsToNeuronGivenNeuronParts(cleavedOriginalBody, originalBodyNeuronParts);

        //update pre, post properties for new nodes
        cleavedNewBody.setProperty("pre", newBodySynapseCounter.getPreCount());
        cleavedNewBody.setProperty("post", newBodySynapseCounter.getPostCount());
        cleavedOriginalBody.setProperty("pre", originalBodySynapseCounter.getPreCount());
        cleavedOriginalBody.setProperty("post", originalBodySynapseCounter.getPostCount());

        //delete old skeleton
        deleteSkeletonForNode(originalBody);
        //delete connects to
        removeConnectsToRelationshipsForNode(originalBody);

        collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(originalBody);

        // create a new history node
        Node newBodyHistoryNode = dbService.createNode(Label.label("History"), Label.label(datasetLabel));
        Node originalBodyHistoryNode = dbService.createNode(Label.label("History"), Label.label(datasetLabel));
        // connect history node to new neuron
        cleavedNewBody.createRelationshipTo(newBodyHistoryNode, RelationshipType.withName("From"));
        cleavedOriginalBody.createRelationshipTo(originalBodyHistoryNode, RelationshipType.withName("From"));
        // connect ghost nodes to history neuron
        originalBody.createRelationshipTo(originalBodyHistoryNode, RelationshipType.withName("CleavedTo"));
        originalBody.createRelationshipTo(newBodyHistoryNode, RelationshipType.withName("CleavedTo"));

        //remove labels, all properties to cleaved
        convertAllPropertiesToCleavedProperties(originalBody);
        removeAllLabels(originalBody);
        List<String> except = new ArrayList<>();
        except.add("MergedTo");
        except.add("CleavedTo");
        removeAllRelationshipsExceptTypesWithName(originalBody, except);

        //add dataset and Neuron labels to new nodes
        cleavedOriginalBody.addLabel(Label.label("Neuron"));
        cleavedOriginalBody.addLabel(Label.label(datasetLabel));
        cleavedNewBody.addLabel(Label.label("Neuron"));
        cleavedNewBody.addLabel(Label.label(datasetLabel));

        List<Node> listOfCleavedNodes = new ArrayList<>();
        listOfCleavedNodes.add(cleavedNewBody);
        listOfCleavedNodes.add(cleavedOriginalBody);
        return Stream.of(new NodeListResult(listOfCleavedNodes));

    }

    @Procedure(value = "proofreader.addSkeleton", mode = Mode.WRITE)
    @Description("proofreader.addSkeleton(fileUrl,datasetLabel) : load skeleton from provided url and connect to its associated neuron (note: file URL must contain body id of neuron) ")
    public Stream<NodeResult> addSkeleton(@Name("fileUrl") String fileUrlString, @Name("datasetLabel") String datasetLabel) {

        if (fileUrlString == null || datasetLabel == null) return Stream.empty();

        String neuronIdPattern = ".*/(.*?)[._]swc";
        Pattern rN = Pattern.compile(neuronIdPattern);
        Matcher mN = rN.matcher(fileUrlString);
        mN.matches();
        Long neuronBodyId = Long.parseLong(mN.group(1));

        Skeleton skeleton = new Skeleton();
        URL fileUrl = null;

        try {
            fileUrl = new URL(fileUrlString);
        } catch (MalformedURLException e) {
            throw new Error("Malformed URL: " + e.getMessage());
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileUrl.openStream()))) {
            skeleton.fromSwc(reader, neuronBodyId);
        } catch (IOException e) {
            throw new Error("IOException: " + e.getMessage());
        }

        final Node neuron = acquireNeuronFromDatabase(neuronBodyId, datasetLabel);

        // grab write locks upfront
        acquireWriteLockForNeuronSubgraph(neuron);

        Node skeletonNode = addSkeletonNode(datasetLabel, skeleton);

        return Stream.of(new NodeResult(skeletonNode));

    }

    @Procedure(value = "proofreader.deleteNeuron", mode = Mode.WRITE)
    @Description("proofreader.deleteNeuron(neuronBodyId,datasetLabel) : delete neuron with body Id and associated nodes from given dataset. ")
    public void deleteNeuron(@Name("neuronBodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {
        if (bodyId == null || datasetLabel == null) throw new Error("Must provide both a bodyId and dataset label.");

        final Node neuron = acquireNeuronFromDatabase(bodyId, datasetLabel);

        // grab write locks upfront
        acquireWriteLockForNeuronSubgraph(neuron);

        //delete connectsTo relationships
        removeConnectsToRelationshipsForNode(neuron);

        //delete synapse set and synapses
        removeSynapseSetsAndSynapses(neuron);

        //delete neuron parts
        removeNeuronPartsForNode(neuron);

        //delete skeleton
        deleteSkeletonForNode(neuron);

        neuron.delete();

    }

    private Node recursivelyMergeNodes(Node resultNode, List<Node> mergedBodies, Long newNodeSize, String datasetLabel) {
        if (mergedBodies.size() == 0) {
            return resultNode;
        } else {
            Node mergedNode = mergeTwoNodesOntoNewNode(resultNode, mergedBodies.get(0), newNodeSize, datasetLabel);
            return recursivelyMergeNodes(mergedNode, mergedBodies.subList(1, mergedBodies.size()), newNodeSize, datasetLabel);
        }
    }

    private Node mergeTwoNodesOntoNewNode(Node node1, Node node2, Long newNodeSize, String datasetLabel) {

        //create a new node that will be a copy of node1
        //copy properties over to new node (note: must add properties before adding labels or will violate uniqueness constraint)
        final Node newNode = copyPropertiesToNewNodeForCleaveOrMerge(node1, "merge");
        // add size property from json to new node
        newNode.setProperty("size", newNodeSize);
        log.info("Merging " + node1.getProperty("bodyId") + " with " + node2.getProperty("bodyId") + "...");

        //move all relationships from original result body Id (A) to new node
        //move all relationships from 1st merged node (B) to new node
        mergeConnectsToRelationshipsToNewNodeAndDeletePreviousConnectsToRelationships(node1, node2, newNode);
        mergeSynapseSetsOntoNewNodeAndRemoveFromPreviousNodes(node1, node2, newNode, datasetLabel);
        mergeNeuronPartsOntoNewNodeAndDeletePreviousConnections(node1, node2, newNode, datasetLabel);

        //add pre and post count from merged node to new node
        setSummedLongProperty("pre", newNode, node2, newNode);
        setSummedLongProperty("post", newNode, node2, newNode);

        //delete skeletons for both nodes
        deleteSkeletonForNode(node2);
        deleteSkeletonForNode(node1);

        //store history
        collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(node1);
        collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(node2);

        // create a new history node
        Node newHistoryNode = dbService.createNode(Label.label("History"), Label.label(datasetLabel));
        // connect history node to new neuron
        newNode.createRelationshipTo(newHistoryNode, RelationshipType.withName("From"));
        // connect ghost nodes to history neuron
        node1.createRelationshipTo(newHistoryNode, RelationshipType.withName("MergedTo"));
        node2.createRelationshipTo(newHistoryNode, RelationshipType.withName("MergedTo"));

        // create ghost nodes for original bodies: property names converted to "merged" property names, all labels removed, all relationships removed (except need to deal with history)
        convertAllPropertiesToMergedProperties(node1);
        convertAllPropertiesToMergedProperties(node2);
        // copy all the labels to the new node
        transferLabelsToNode(node1, newNode);
        transferLabelsToNode(node2, newNode);
        removeAllLabels(node1);
        removeAllLabels(node2);
        List<String> except = new ArrayList<>();
        except.add("MergedTo");
        except.add("CleavedTo");
        removeAllRelationshipsExceptTypesWithName(node1, except);
        removeAllRelationshipsExceptTypesWithName(node2, except);

        return newNode;
    }

    private Node copyPropertiesToNewNodeForCleaveOrMerge(Node originalNode, String typeOfCopy) throws IllegalArgumentException {
        // typeOfCopy can be "merge" or "cleave"
        final Node newNode = dbService.createNode();

        String ghostNodePropertiesPrefix = null;
        if (typeOfCopy.equals("merge")) {
            ghostNodePropertiesPrefix = "merged";
        } else if (typeOfCopy.equals("cleave")) {
            ghostNodePropertiesPrefix = "cleaved";
        } else {
            throw new IllegalArgumentException("Illegal typeOfCopy: must be \"merge\" or \"cleave\"");
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
        log.info("Merged ConnectsTo relationships from original neurons onto new neuron.");
    }

    private void addConnectsToRelationshipsFromMap(ConnectsToRelationshipMap connectsToRelationshipMap) {
        //add relationships to new node
        for (String stringKey : connectsToRelationshipMap.getNodeIdToConnectsToRelationshipHashMap().keySet()) {
            ConnectsToRelationship connectsToRelationship = connectsToRelationshipMap.getConnectsToRelationshipByKey(stringKey);
            Node startNode = connectsToRelationship.getStartNode();
            Node endNode = connectsToRelationship.getEndNode();
            Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName("ConnectsTo"));
            relationship.setProperty("weight", connectsToRelationship.getWeight());
        }
    }

    private void addSynapseToConnectsToRelationshipMap(Node synapseNode, Synapse synapse, Node neuronWithSynapse, ConnectsToRelationshipMap connectsToRelationshipMap) {
        // get the node that this synapse connects to
        List<Node> listOfConnectedNeurons = getPostOrPreSynapticNeuronsGivenSynapse(synapseNode);
        // add to the connectsToRelationshipMap
        for (Node connectedNeuron : listOfConnectedNeurons) {
            if (synapse.getType().equals("pre")) {
                connectsToRelationshipMap.insertConnectsToRelationship(neuronWithSynapse, connectedNeuron, 1L);
            } else if (synapse.getType().equals("post")) {
                connectsToRelationshipMap.insertConnectsToRelationship(connectedNeuron, neuronWithSynapse, 1L);
            }
        }
    }

    private void createNeuronPartNodesAndConnectToGivenNeuron(List<NeuronPart> neuronPartList, String datasetLabel, Node neuron) {
        for (NeuronPart neuronPart : neuronPartList) {
            Node neuronPartNode = dbService.createNode(Label.label("NeuronPart"), Label.label(datasetLabel));
            String neuronPartId = datasetLabel + ":" + neuron.getProperty("bodyId") + ":" + neuronPart.getRoi();
            neuronPartNode.setProperty("neuronPartId", neuronPartId);
            neuronPartNode.addLabel(Label.label(neuronPart.getRoi()));
            neuronPartNode.setProperty("pre", neuronPart.getPre());
            neuronPartNode.setProperty("post", neuronPart.getPost());
            neuronPartNode.setProperty("size", neuronPart.getPre() + neuronPart.getPost());
            neuronPartNode.createRelationshipTo(neuron, RelationshipType.withName("PartOf"));
        }
    }

    private void mergeSynapseSetsOntoNewNodeAndRemoveFromPreviousNodes(Node node1, Node node2, Node newNode, String datasetLabel) {

        //get the synapse sets for node1 and node2, remove them from original nodes
        Node node1SynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node1);
        Node node2SynapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node2);

        //add both synapse sets to the new node, collect them for adding to apoc merge procedure
        Map<String, Object> parametersMap = new HashMap<>();
        if (node1SynapseSetNode != null) {
            newNode.createRelationshipTo(node1SynapseSetNode, RelationshipType.withName("Contains"));
            parametersMap.put("ssnode1", node1SynapseSetNode);
        }
        if (node2SynapseSetNode != null) {
            newNode.createRelationshipTo(node2SynapseSetNode, RelationshipType.withName("Contains"));
            parametersMap.put("ssnode2", node2SynapseSetNode);
        }

        // merge the two synapse nodes using apoc if there are two synapseset nodes. inherits the datasetBodyId of the first node
        if (parametersMap.containsKey("ssnode1") && parametersMap.containsKey("ssnode2")) {
            Node newSynapseSetNode = (Node) dbService.execute("CALL apoc.refactor.mergeNodes([$ssnode1, $ssnode2], {properties:{datasetBodyId:\"discard\"}}) YIELD node RETURN node", parametersMap).next().get("node");
            newSynapseSetNode.addLabel(Label.label(datasetLabel));
            //delete the extra relationship between new node and new synapse set node
            newNode.getRelationships(RelationshipType.withName("Contains")).iterator().next().delete();
        }

        log.info("Merged SynapseSet nodes from original neurons onto new neuron.");
    }

    private void mergeNeuronPartsOntoNewNodeAndDeletePreviousConnections(final Node node1, final Node node2, final Node newNode, final String datasetLabel) {

        // node1 neuron parts
        Map<String, Node> node1RoiLabelToNeuronParts = new HashMap<>();
        for (Relationship node1NeuronPartRelationship : node1.getRelationships(RelationshipType.withName("PartOf"))) {
            //get the neuronpart
            Node neuronPart = node1NeuronPartRelationship.getStartNode();
            //connect to the new node
            neuronPart.createRelationshipTo(newNode, RelationshipType.withName("PartOf"));
            //delete the old node relationship
            node1NeuronPartRelationship.delete();
            //create a map of labels to neuron parts for node1
            for (Label neuronPartLabel : neuronPart.getLabels()) {
                if (!neuronPartLabel.name().equals("NeuronPart") && !neuronPartLabel.name().equals(datasetLabel)) {
                    node1RoiLabelToNeuronParts.put(neuronPartLabel.name(), neuronPart);
                }
            }
        }

        // node 2 neuron parts
        for (Relationship node2NeuronPartRelationship : node2.getRelationships(RelationshipType.withName("PartOf"))) {
            //get the neuronpart
            Node neuronPart = node2NeuronPartRelationship.getStartNode();
            //connect to the new node
            neuronPart.createRelationshipTo(newNode, RelationshipType.withName("PartOf"));
            //delete the old node relationship
            node2NeuronPartRelationship.delete();

            //if node1 neuronpart already exists for a given roi, add pre, post, and size from node2 to node1 neuronpart then delete node2 neuronpart
            for (Label neuronPartLabel : neuronPart.getLabels()) {
                if (!neuronPartLabel.name().equals("NeuronPart") && !neuronPartLabel.name().equals(datasetLabel)) {

                    //get the properties for the matching neuron part for node1 if it exists
                    Node node1NeuronPart = node1RoiLabelToNeuronParts.get(neuronPartLabel.name());

                    if (node1NeuronPart != null) {
                        Map<String, Object> node1NeuronPartProperties = node1NeuronPart.getProperties("pre", "post", "size");
                        Map<String, Object> node2NeuronPartProperties = neuronPart.getProperties("pre", "post", "size");

                        for (String propertyName : node1NeuronPartProperties.keySet()) {
                            node1NeuronPart.setProperty(propertyName, (Long) node1NeuronPartProperties.get(propertyName) + (Long) node2NeuronPartProperties.get(propertyName));
                        }

                        // found a matching roi for node1 so delete node2 neuronpart
                        neuronPart.getRelationships().forEach(Relationship::delete);
                        neuronPart.delete();
                    }
                }
            }
        }
        log.info("Merged NeuronPart nodes from original neurons onto new neuron.");
    }

    private void deleteSkeletonForNode(final Node node) {

        Node nodeSkeletonNode = null;
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("Contains"))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label("Skeleton"))) {
                nodeSkeletonNode = containedNode;
                //delete Contains connection to original node
                nodeRelationship.delete();
            }
        }

        if (nodeSkeletonNode != null) {
            for (Relationship skeletonRelationship : nodeSkeletonNode.getRelationships(RelationshipType.withName("Contains"))) {
                Node skelNode = skeletonRelationship.getEndNode();
                //delete LinksTo relationships
                for (Relationship skelNodeLinksToRelationship : skelNode.getRelationships(RelationshipType.withName("LinksTo"))) {
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
        log.info("Deleted skeletons for original nodes.");
    }

    private void collectPreviousHistoryOntoGhostAndDeleteExistingHistoryNode(Node node) {

        Node existingHistoryNode = getHistoryNodeForNode(node);

        if (existingHistoryNode != null) {
            for (Relationship mergedToRelationship : existingHistoryNode.getRelationships(RelationshipType.withName("MergedTo"))) {
                //get the preexisting ghost node
                Node ghostNode = mergedToRelationship.getStartNode();
                //connect it to result body ghost
                ghostNode.createRelationshipTo(node, RelationshipType.withName("MergedTo"));
                //remove its connection to the old history node
                mergedToRelationship.delete();
            }
            for (Relationship cleavedToRelationship : existingHistoryNode.getRelationships(RelationshipType.withName("CleavedTo"))) {
                //get the preexisting ghost node
                Node ghostNode = cleavedToRelationship.getStartNode();
                //connect it to result body ghost
                ghostNode.createRelationshipTo(node, RelationshipType.withName("CleavedTo"));
                //remove its connection to the old history node
                cleavedToRelationship.delete();
            }
            //delete the old history node
            existingHistoryNode.delete();
        }

    }

    private Node getHistoryNodeForNode(Node node) {

        Node historyNode = null;
        for (Relationship historyRelationship : node.getRelationships(RelationshipType.withName("From"))) {
            historyNode = historyRelationship.getEndNode();
        }
        return historyNode;

    }

    private void compareMergeActionSynapseSetWithDatabaseSynapseSet(Node neuron, MergeAction mergeAction) {

        Node synapseSet = getSynapseSetForNode(neuron);
        if (synapseSet == null) {
            throw new Error("No synapse set on the resulting body.");
        }

        //get the resulting bodies synapse set from the database
        Set<Synapse> resultingBodySynapseSet = new HashSet<>();
        //make sure their are synapses attached to the synapse set
        if (synapseSet.hasRelationship(RelationshipType.withName("Contains"), Direction.OUTGOING)) {
            for (Relationship synapseRelationship : synapseSet.getRelationships(RelationshipType.withName("Contains"), Direction.OUTGOING)) {
                Node synapse = synapseRelationship.getEndNode();
                try {
                    resultingBodySynapseSet.add(new Synapse((String) synapse.getProperty("type"), (int) (long) synapse.getProperty("x"), (int) (long) synapse.getProperty("y"), (int) (long) synapse.getProperty("z")));
                } catch (org.neo4j.graphdb.NotFoundException nfe) {
                    nfe.printStackTrace();
                    throw new Error("synapse does not have type, x, y, or z properties: " + synapse.getAllProperties());
                }
            }
        }

        //compare the two sets
        Set<Synapse> mergeActionSynapseSet = new HashSet<>(mergeAction.getTargetBodySynapses());
        Set<Synapse> databaseSynapseSet = new HashSet<>(resultingBodySynapseSet);
        databaseSynapseSet.removeAll(mergeAction.getTargetBodySynapses());
        mergeActionSynapseSet.removeAll(resultingBodySynapseSet);

        if (mergeActionSynapseSet.size() == 0 && databaseSynapseSet.size() == 0) {
            log.info("Database and merge action synapses match.");
        } else {
            throw new Error("Found the following differences between the database and merge action synapse sets: \n" +
                    "* Synapses in merge action but not in database: " + mergeActionSynapseSet + "\n" +
                    "* Synapses in database but not in merge action: " + databaseSynapseSet);
        }

    }

    private List<String> getRoisFromSynapse(Synapse synapse, String datasetLabel) {

        Map<String, Object> roiQueryResult = null;
        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("datasetLocation", datasetLabel + ":" + synapse.getLocationString());

        try {
            roiQueryResult = dbService.execute("MATCH (s:Synapse:" + datasetLabel + "{datasetLocation:$datasetLocation}) WITH labels(s) AS labels RETURN filter(label IN labels WHERE NOT label=\"Synapse\" AND NOT label=\"PreSyn\" AND NOT label=\"PostSyn\" AND NOT label=\"" + datasetLabel + "\") AS l", parametersMap).next();
        } catch (java.util.NoSuchElementException nse) {
            nse.printStackTrace();
            throw new Error("Error using proofreader procedures: Synapse not found in the dataset.");
        }

        List<String> roiList = (ArrayList<String>) roiQueryResult.get("l");

        return roiList;
    }

    private void setSynapseRoisFromDatabase(Synapse synapse, String datasetLabel) {
        List<String> roiList = getRoisFromSynapse(synapse, datasetLabel);
        if (roiList.size() == 0) {
            throw new Error("No roi found on synapse: " + synapse);
        }
        synapse.addRoiList(roiList);
    }

    private ConnectsToRelationshipMap getConnectsToRelationshipMapForNodesAndDeletePreviousRelationships(final Node node1, final Node node2, final Node newNode) {

        ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
        List<Node> nodeList = new ArrayList<>();
        nodeList.add(node1);
        nodeList.add(node2);

        for (Node node : nodeList) {
            for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("ConnectsTo"))) {
                //get weight for relationship
                Long weight = null;
                if (nodeRelationship.hasProperty("weight")) {
                    weight = (Long) nodeRelationship.getProperty("weight");
                } else {
                    weight = 0L;
                }

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
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("Contains"))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label("SynapseSet"))) {
                nodeSynapseSetNode = containedNode;
                //delete Contains connection to original node
                nodeRelationship.delete();
            }
        }
        return nodeSynapseSetNode;
    }

    private Node getSynapseSetForNode(final Node node) {
        Node nodeSynapseSetNode = null;
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("Contains"))) {
            Node containedNode = nodeRelationship.getEndNode();
            if (containedNode.hasLabel(Label.label("SynapseSet"))) {
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

    private Node createNewMergedNode(Node node1, Node node2, String datasetLabel) {
        //create a new body with new bodyId and properties
        //id for new body is by convention the first node Id listed in the function
        //newNode acquires dataset label
        final Node newNode = dbService.createNode();
        newNode.setProperty("bodyId", node1.getProperty("bodyId"));
        node1.setProperty("mergedBodyId", node1.getProperty("bodyId"));
        node1.removeProperty("bodyId");
        node2.setProperty("mergedBodyId", node2.getProperty("bodyId"));
        node2.removeProperty("bodyId");
        newNode.addLabel(Label.label(datasetLabel));
        newNode.addLabel(Label.label("Neuron"));
        //newNode gets all roi labels from node1 and node2
        for (Label node1Label : node1.getLabels()) {
            if (!node1Label.name().equals("Neuron") && !node1Label.name().equals(datasetLabel)) {
                newNode.addLabel(node1Label);
            }
        }
        for (Label node2Label : node2.getLabels()) {
            if (!node2Label.name().equals("Neuron") && !node2Label.name().equals(datasetLabel)) {
                newNode.addLabel(node2Label);
            }
        }
        log.info("Created a new neuron with bodyId " + newNode.getProperty("bodyId") + " in " + datasetLabel + ".");
        return newNode;
    }

    private Node acquireNeuronFromDatabase(Long nodeBodyId, String datasetLabel) throws Error {
        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("nodeBodyId", nodeBodyId);
        Map<String, Object> nodeQueryResult = null;
        Node foundNode = null;
        try {
            nodeQueryResult = dbService.execute("MATCH (node:Neuron:" + datasetLabel + "{bodyId:$nodeBodyId}) RETURN node", parametersMap).next();
        } catch (java.util.NoSuchElementException nse) {
            nse.printStackTrace();
            throw new Error("Error using proofreader procedures: All neuron nodes must exist in the dataset and be labeled :Neuron.");
        }

        try {
            foundNode = (Node) nodeQueryResult.get("node");
        } catch (NullPointerException npe) {
            npe.printStackTrace();
            throw new Error("Error using proofreader procedures: All neuron nodes must exist in the dataset and be labeled :Neuron.");
        }

        return foundNode;
    }

    private List<Node> getPostOrPreSynapticNeuronsGivenSynapse(Node synapse) {
        // list so that multiple connections per neuron are each counted as 1 additional weight
        List<Node> listOfConnectedNeurons = new ArrayList<>();
        if (synapse.hasRelationship(RelationshipType.withName("SynapsesTo"))) {
            for (Relationship synapsesToRelationship : synapse.getRelationships(RelationshipType.withName("SynapsesTo"))) {
                Node connectedSynapse = synapsesToRelationship.getOtherNode(synapse);
                Node connectedNeuron = null;
                try {
                    Node connectedSynapseSet = connectedSynapse.getSingleRelationship(RelationshipType.withName("Contains"), Direction.INCOMING).getStartNode();
                    connectedNeuron = connectedSynapseSet.getSingleRelationship(RelationshipType.withName("Contains"), Direction.INCOMING).getStartNode();
                } catch (NoSuchElementException nse) {
                    throw new Error("Connected synapse has no synapse set or connected neuron: " + connectedSynapse.getAllProperties());
                }
                listOfConnectedNeurons.add(connectedNeuron);
            }
        }
        return listOfConnectedNeurons;
    }

    private void incrementSynapseCounterWithSynapse(Synapse synapse, SynapseCounter synapseCounter) {
        if (synapse.getType().equals("pre")) {
            synapseCounter.incrementPreCount();
        } else if (synapse.getType().equals("post")) {
            synapseCounter.incrementPostCount();
        }
    }

    private void addRoiLabelsToNeuronGivenNeuronParts(Node neuron, List<NeuronPart> neuronPartList) {
        for (NeuronPart neuronPart : neuronPartList) {
            neuron.addLabel(Label.label(neuronPart.getRoi()));
        }
    }

    private void combinePropertiesOntoMergedNeuron(Node node1, Node node2, Node newNode) {

        setNode1InheritedProperty("name", node1, node2, newNode);
        setNode1InheritedProperty("type", node1, node2, newNode);
        setNode1InheritedProperty("status", node1, node2, newNode);
        setNode1InheritedProperty("somaLocation", node1, node2, newNode);
        setNode1InheritedProperty("somaRadius", node1, node2, newNode);

        setSummedLongProperty("size", node1, node2, newNode);
        setSummedLongProperty("pre", node1, node2, newNode);
        setSummedLongProperty("post", node1, node2, newNode);

        // changes sId for old nodes to mergedSId
        setSId(node1, node2, newNode);

        log.info("Properties from merged neurons added to new neuron.");

    }

    private void setNode1InheritedProperty(String propertyName, Node node1, Node node2, Node newNode) {
        if (node1.hasProperty(propertyName)) {
            Object node1Property = node1.getProperty(propertyName);
            newNode.setProperty(propertyName, node1Property);
        } else if (node2.hasProperty(propertyName)) {
            Object node2Property = node2.getProperty(propertyName);
            newNode.setProperty(propertyName, node2Property);
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

    private void setSummedIntProperty(String propertyName, Node node1, Node node2, Node newNode) {
        Integer summedValue = 0;
        if (node1.hasProperty(propertyName)) {
            Integer node1Property = (Integer) node1.getProperty(propertyName);
            summedValue += node1Property;
        }
        if (node2.hasProperty(propertyName)) {
            Integer node2Property = (Integer) node2.getProperty(propertyName);
            summedValue += node2Property;
        }
        newNode.setProperty(propertyName, summedValue);
    }

    private void setSId(Node node1, Node node2, Node newNode) {
        String propertyName = "sId";

        if (node1.hasProperty(propertyName)) {
            Object node1Property = node1.getProperty(propertyName);
            convertPropertyNameToNewPropertyName(propertyName, "mergedSId", node1);
            newNode.setProperty(propertyName, node1Property);
        } else if (node2.hasProperty(propertyName)) {
            Object node2Property = node2.getProperty(propertyName);
            convertPropertyNameToNewPropertyName(propertyName, "mergedSId", node2);
            newNode.setProperty(propertyName, node2Property);
        }

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
            if (!propertyName.startsWith("merged")) {
                String mergedPropertyName = "merged" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
                convertPropertyNameToNewPropertyName(propertyName, mergedPropertyName, node);
            }
        }
    }

    private void convertAllPropertiesToCleavedProperties(Node node) {
        Map<String, Object> nodeProperties = node.getAllProperties();
        for (String propertyName : nodeProperties.keySet()) {
            if (!propertyName.startsWith("cleaved")) {
                String cleavedPropertyName = "cleaved" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
                convertPropertyNameToNewPropertyName(propertyName, cleavedPropertyName, node);
            }
        }
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

    private Node addSkeletonNode(final String dataset, final Skeleton skeleton) {

        final String neuronToSkeletonConnectionString = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId \n" +
                "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId \n" +
                "MERGE (n)-[:Contains]->(r) \n";

        final String rootNodeString = "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId \n" +
                "MERGE (s:SkelNode:" + dataset + " {skelNodeId:$skelNodeId}) ON CREATE SET s.skelNodeId=$skelNodeId, s.location=$location, s.radius=$radius, s.x=$x, s.y=$y, s.z=$z, s.rowNumber=$rowNumber \n" +
                "MERGE (r)-[:Contains]->(s) \n";

        final String parentNodeString = "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId})\n" +
                "MERGE (p:SkelNode:" + dataset + " {skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.x=$pX, p.y=$pY, p.z=$pZ, p.rowNumber=$pRowNumber \n" +
                "MERGE (r)-[:Contains]->(p) ";

        final String childNodeString = "MERGE (p:SkelNode:" + dataset + " {skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.x=$pX, p.y=$pY, p.z=$pZ, p.rowNumber=$pRowNumber \n" +
                "MERGE (c:SkelNode:" + dataset + " {skelNodeId:$childNodeId}) ON CREATE SET c.skelNodeId=$childNodeId, c.location=$childLocation, c.radius=$childRadius, c.x=$childX, c.y=$childY, c.z=$childZ, c.rowNumber=$childRowNumber \n" +
                "MERGE (p)-[:LinksTo]-(c)";

        Long associatedBodyId = skeleton.getAssociatedBodyId();
        List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

        Map<String, Object> neuronToSkeletonParametersMap = new HashMap<String, Object>() {{
            put("bodyId", associatedBodyId);
            put("skeletonId", dataset + ":" + associatedBodyId);
        }};

        dbService.execute(neuronToSkeletonConnectionString, neuronToSkeletonParametersMap);

        for (SkelNode skelNode : skelNodeList) {

            if (skelNode.getParent() == null) {
                Map<String, Object> rootNodeStringParametersMap = new HashMap<String, Object>() {{
                    put("location", skelNode.getLocationString());
                    put("radius", skelNode.getRadius());
                    put("skelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString());
                    put("skeletonId", dataset + ":" + associatedBodyId);
                    put("x", skelNode.getLocation().get(0));
                    put("y", skelNode.getLocation().get(1));
                    put("z", skelNode.getLocation().get(2));
                    put("rowNumber", skelNode.getRowNumber());
                }};

                dbService.execute(rootNodeString, rootNodeStringParametersMap);
            }

            Map<String, Object> parentNodeStringParametersMap = new HashMap<String, Object>() {{
                put("pLocation", skelNode.getLocationString());
                put("pRadius", skelNode.getRadius());
                put("parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString());
                put("skeletonId", dataset + ":" + associatedBodyId);
                put("pX", skelNode.getLocation().get(0));
                put("pY", skelNode.getLocation().get(1));
                put("pZ", skelNode.getLocation().get(2));
                put("pRowNumber", skelNode.getRowNumber());
            }};

            dbService.execute(parentNodeString, parentNodeStringParametersMap);

            for (SkelNode child : skelNode.getChildren()) {
                String childNodeId = dataset + ":" + associatedBodyId + ":" + child.getLocationString();

                Map<String, Object> childNodeStringParametersMap = new HashMap<String, Object>() {{
                    put("parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString());
                    put("skeletonId", dataset + ":" + associatedBodyId);
                    put("pLocation", skelNode.getLocationString());
                    put("pRadius", skelNode.getRadius());
                    put("pX", skelNode.getLocation().get(0));
                    put("pY", skelNode.getLocation().get(1));
                    put("pZ", skelNode.getLocation().get(2));
                    put("pRowNumber", skelNode.getRowNumber());
                    put("childNodeId", childNodeId);
                    put("childLocation", child.getLocationString());
                    put("childRadius", child.getRadius());
                    put("childX", child.getLocation().get(0));
                    put("childY", child.getLocation().get(1));
                    put("childZ", child.getLocation().get(2));
                    put("childRowNumber", child.getRowNumber());
                }};

                dbService.execute(childNodeString, childNodeStringParametersMap);

            }

        }
        Map<String, Object> getSkeletonParametersMap = new HashMap<String, Object>() {{
            put("skeletonId", dataset + ":" + associatedBodyId);
        }};

        Map<String, Object> nodeQueryResult = dbService.execute("MATCH (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) RETURN r", getSkeletonParametersMap).next();
        return (Node) nodeQueryResult.get("r");
    }

    private void removeConnectsToRelationshipsForNode(Node node) {
        for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("ConnectsTo"))) {
            nodeRelationship.delete();
        }
        log.info("Removed all ConnectsTo relationships for node.");
    }

    private void removeSynapseSetsAndSynapses(Node node) {
        Node synapseSetNode = getSynapseSetForNodeAndDeleteConnectionToNode(node);
        for (Relationship ssRelationship : synapseSetNode.getRelationships(RelationshipType.withName("Contains"))) {
            Node synapseNode = ssRelationship.getEndNode();
            removeAllRelationships(synapseNode);
            synapseNode.delete();
        }
        synapseSetNode.delete();
        log.info("Removed all SynapseSets and Synapses for node.");
    }

    private void removeNeuronPartsForNode(Node node) {
        for (Relationship nodeNeuronPartRelationship : node.getRelationships(RelationshipType.withName("PartOf"))) {
            Node neuronPart = nodeNeuronPartRelationship.getStartNode();
            nodeNeuronPartRelationship.delete();
            neuronPart.delete();
        }
        log.info("Removed all NeuronParts for node.");
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

    private void acquireWriteLockForNeuronSubgraph(Node neuron) {
        // neuron
        acquireWriteLockForNode(neuron);
        // connects to relationships and 1-degree connections
        for (Relationship connectsToRelationship : neuron.getRelationships(RelationshipType.withName("ConnectsTo"))) {
            acquireWriteLockForRelationship(connectsToRelationship);
            acquireWriteLockForNode(connectsToRelationship.getOtherNode(neuron));
        }
        // neuronpart
        for (Relationship partOfRelationship : neuron.getRelationships(RelationshipType.withName("PartOf"))) {
            acquireWriteLockForRelationship(partOfRelationship);
            acquireWriteLockForNode(partOfRelationship.getStartNode());
        }
        // skeleton and synapse set
        for (Relationship containsRelationship : neuron.getRelationships(RelationshipType.withName("Contains"))) {
            acquireWriteLockForRelationship(containsRelationship);
            Node skeletonOrSynapseSetNode = containsRelationship.getEndNode();
            acquireWriteLockForNode(skeletonOrSynapseSetNode);
            // skel nodes and synapses
            for (Relationship skelNodeOrSynapseRelationship : skeletonOrSynapseSetNode.getRelationships(RelationshipType.withName("Contains"),Direction.OUTGOING)) {
                acquireWriteLockForRelationship(skelNodeOrSynapseRelationship);
                Node skelNodeOrSynapseNode = skelNodeOrSynapseRelationship.getEndNode();
                acquireWriteLockForNode(skelNodeOrSynapseNode);
                // first degree relationships to synapses
                for (Relationship synapsesToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName("SynapsesTo"))) {
                    acquireWriteLockForRelationship(synapsesToRelationship);
                    acquireWriteLockForNode(synapsesToRelationship.getOtherNode(skelNodeOrSynapseNode));
                }
                // links to relationships for skel nodes
                for (Relationship linksToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName("LinksTo"),Direction.OUTGOING)) {
                    acquireWriteLockForRelationship(linksToRelationship);
                }
            }
        }
    }
}

