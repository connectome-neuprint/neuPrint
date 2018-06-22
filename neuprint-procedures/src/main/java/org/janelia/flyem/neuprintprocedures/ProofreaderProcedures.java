package org.janelia.flyem.neuprintprocedures;

import apoc.result.NodeResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

public class ProofreaderProcedures {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @Procedure(value = "proofreader.mergeNeurons", mode = Mode.WRITE)
    @Description("proofreader.mergeNeurons(node1BodyId,node2BodyId,datasetLabel) merge nodes into new node with bodyId inherited from first in list")
    public Stream<NodeResult> mergeNeurons(@Name("node1BodyId") Long node1BodyId, @Name("node2BodyId") Long node2BodyId, @Name("datasetLabel") String datasetLabel) {
        if (node1BodyId == null || node2BodyId == null) return Stream.empty();
        Map<String,Object> nodeQueryResult = acquireNodesFromDatabase(node1BodyId, node2BodyId, datasetLabel);

        final Node node1 = (Node) nodeQueryResult.get("node1");
        final Node node2 = (Node) nodeQueryResult.get("node2");

        // grab write locks upfront
        try (Transaction tx = dbService.beginTx()) {
            tx.acquireWriteLock(node1);
            tx.acquireWriteLock(node2);
            tx.success();
        }

        Node newNode = createNewNode(node1, node2, datasetLabel);

        mergeConnectsToRelationships(node1,node2,newNode);

        mergeSynapseSets(node1, node2, newNode, datasetLabel);

        deleteSkeletonForNode(node1);
        deleteSkeletonForNode(node2);

        mergeNeuronParts(node1,node2,newNode,datasetLabel);

        combinePropertiesOntoMergedNeuron(node1,node2,newNode);

        convertAllPropertiesToMergedProperties(node1);
        convertAllPropertiesToMergedProperties(node2);
        removeAllLabels(node1);
        removeAllLabels(node2);
        removeAllRelationships(node1);
        removeAllRelationships(node2);
        log.info("All labels and relationships removed from original neurons.");


        node1.createRelationshipTo(newNode,RelationshipType.withName("MergedTo"));
        node2.createRelationshipTo(newNode,RelationshipType.withName("MergedTo"));
        log.info("Created MergedTo relationship between original neurons and new neuron.");


        return Stream.of(new NodeResult(newNode));


    }


    private ConnectsToRelationshipMap getConnectsToRelationshipMapForNodes(final Node node1, final Node node2, final Node newNode) {

        ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
        List<Node> nodeList = new ArrayList<>();
        nodeList.add(node1);
        nodeList.add(node2);
        for (Node node : nodeList) {
            for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("ConnectsTo"))) {
                Long weight = null;
                if (nodeRelationship.hasProperty("weight")) {
                    weight = (Long) nodeRelationship.getProperty("weight");
                } else { weight = 0L; }
                //replace node with newNode in start and end
                Node startNode = nodeRelationship.getStartNode();
                Node endNode = nodeRelationship.getEndNode();
                //don't count node1/node2 relationships twice
                if (((startNode.equals(node1) && endNode.equals(node2)) || (startNode.equals(node2) && endNode.equals(node1))) && node.equals(node2)) {
                    continue;
                } else {
                    startNode = (startNode.equals(node1) || startNode.equals(node2)) ? newNode : startNode;
                    endNode = (endNode.equals(node1) || endNode.equals(node2)) ? newNode : endNode;
                    connectsToRelationshipMap.insertConnectsToRelationship(startNode, endNode, weight);
                }
                nodeRelationship.delete();
            }
        }
        return connectsToRelationshipMap;
    }

    private Node getSynapseSetForNode(final Node node) {
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

    private void mergeNeuronParts(final Node node1,final Node node2, final Node newNode, final String datasetLabel) {
        Map<String, Node> node1LabelToNeuronParts = new HashMap<>();

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
                    node1LabelToNeuronParts.put(neuronPartLabel.name(), neuronPart);
                }
            }
        }


        for (Relationship node2NeuronPartRelationship : node2.getRelationships(RelationshipType.withName("PartOf"))) {
            //get the neuronpart
            Node neuronPart = node2NeuronPartRelationship.getStartNode();
            //connect to the new node
            neuronPart.createRelationshipTo(newNode, RelationshipType.withName("PartOf"));
            //delete the old node relationship
            node2NeuronPartRelationship.delete();
            //add pre post size from node2 to node1 neuronpart then delete node2 neuronpart
            for (Label neuronPartLabel : neuronPart.getLabels()) {
                if (!neuronPartLabel.name().equals("NeuronPart") && !neuronPartLabel.name().equals(datasetLabel)) {
                    //get the properties for the matching neuron part for node1 if it exists
                    Node node1NeuronPart = node1LabelToNeuronParts.get(neuronPartLabel.name());

                    if (node1NeuronPart!=null) {
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


    private void mergeConnectsToRelationships(Node node1, Node node2, Node newNode) {
        //duplicate all relationships for this body from the other two bodies
        ConnectsToRelationshipMap connectsToRelationshipMap = getConnectsToRelationshipMapForNodes(node1, node2, newNode);
        for (String stringKey : connectsToRelationshipMap.getNodeIdToConnectsToRelationshipHashMap().keySet()) {
            ConnectsToRelationship connectsToRelationship = connectsToRelationshipMap.getConnectsToRelationshipByKey(stringKey);
            Node startNode = connectsToRelationship.getStartNode();
            Node endNode = connectsToRelationship.getEndNode();
            Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName("ConnectsTo"));
            relationship.setProperty("weight", connectsToRelationship.getWeight());
        }
        log.info("Merged ConnectsTo relationships from original neurons onto new neuron.");
    }

    private Node createNewNode(Node node1, Node node2, String datasetLabel) {
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

    private Map<String, Object> acquireNodesFromDatabase(Long node1BodyId, Long node2BodyId, String datasetLabel) {
        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("node1BodyId", node1BodyId);
        parametersMap.put("node2BodyId", node2BodyId);
        Map<String, Object> nodeQueryResult = null;
        try {
            nodeQueryResult = dbService.execute("MATCH (node1:Neuron:" + datasetLabel + "{bodyId:$node1BodyId}), (node2:Neuron:" + datasetLabel + "{bodyId:$node2BodyId}) RETURN node1,node2", parametersMap).next();
        } catch (java.util.NoSuchElementException nse) {
            System.out.println("Error using proofreader.mergeNodes: both nodes must exist in the dataset and be labeled :Neuron.");
            System.exit(1);
        }
        log.info("Acquired neurons with bodyId " + node1BodyId + " and " + node2BodyId + " from " + datasetLabel + ".");
        return nodeQueryResult;
    }


    private void mergeSynapseSets(Node node1, Node node2, Node newNode, String datasetLabel) {
        //create relationships between synapse sets and new nodes (also deletes old relationship)
        Map<String, Object> parametersMap = new HashMap<>();
        Node node1SynapseSetNode = getSynapseSetForNode(node1);
        if (node1SynapseSetNode != null) {
            newNode.createRelationshipTo(node1SynapseSetNode, RelationshipType.withName("Contains"));
            parametersMap.put("ssnode1", node1SynapseSetNode);
        }
        Node node2SynapseSetNode = getSynapseSetForNode(node2);
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


    private void combinePropertiesOntoMergedNeuron(Node node1, Node node2, Node newNode) {

        setNode1InheritedProperty("name",node1,node2,newNode);
        setNode1InheritedProperty("type",node1,node2,newNode);
        setNode1InheritedProperty("status",node1,node2,newNode);
        setNode1InheritedProperty("somaLocation",node1,node2,newNode);
        setNode1InheritedProperty("somaRadius",node1,node2,newNode);

        setSummedLongProperty("size",node1,node2,newNode);
        setSummedLongProperty("pre",node1,node2,newNode);
        setSummedLongProperty("post",node1,node2,newNode);

        // changes sId for old nodes to mergedSId
        setSId(node1,node2,newNode);

        log.info("Properties from merged neurons added to new neuron.");



    }


    private void setNode1InheritedProperty(String propertyName, Node node1, Node node2, Node newNode) {
        if (node1.hasProperty(propertyName)) {
            Object node1Property = node1.getProperty(propertyName);
            newNode.setProperty(propertyName,node1Property);
        } else if (node2.hasProperty(propertyName)) {
            Object node2Property = node2.getProperty(propertyName);
            newNode.setProperty(propertyName,node2Property);
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
        newNode.setProperty(propertyName,summedValue);
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
        newNode.setProperty(propertyName,summedValue);
    }

    private void setSId(Node node1, Node node2, Node newNode) {
        String propertyName = "sId";
        setNode1InheritedProperty(propertyName,node1,node2,newNode);

        if (node1.hasProperty(propertyName)) {
            Object node1Property = node1.getProperty(propertyName);
            convertPropertyNameToMergedPropertyName(propertyName,"mergedSId",node1);
            newNode.setProperty(propertyName,node1Property);
        } else if (node2.hasProperty(propertyName)) {
            Object node2Property = node2.getProperty(propertyName);
            convertPropertyNameToMergedPropertyName(propertyName,"mergedSId",node2);
            newNode.setProperty(propertyName,node2Property);
        }

//        if (node1SId!=null && node2SId!=null) {
//            Integer newNodeSId = node1SId <= node2SId ? node1SId : node2SId;
//            newNode.setProperty("sId",newNodeSId);
//        } else if (node1SId!=null) {
//            Integer newNodeSId = node1SId;
//            newNode.setProperty("sId",newNodeSId);
//        } else if (node2SId!=null) {
//            Integer newNodeSId = node2SId;
//            newNode.setProperty("sId",newNodeSId);
//        }


    }

    private void convertPropertyNameToMergedPropertyName(String propertyName, String mergedPropertyName, Node node) {
        if (node.hasProperty(propertyName)) {
            Object node1Property = node.getProperty(propertyName);
            node.setProperty(mergedPropertyName,node1Property);
            node.removeProperty(propertyName);
        }

    }

    private void convertAllPropertiesToMergedProperties(Node node) {
        Map<String,Object> nodeProperties = node.getAllProperties();
        for (String propertyName : nodeProperties.keySet()) {
            if (!propertyName.startsWith("merged")) {
                String mergedPropertyName = "merged" + propertyName.substring(0,1).toUpperCase() + propertyName.substring(1);
                convertPropertyNameToMergedPropertyName(propertyName, mergedPropertyName, node);
            }
        }
        log.info("All properties from merged neurons changed to \"merged\" + property name.");

    }

    private void removeAllLabels(Node node) {
        Iterable<Label> nodeLabels = node.getLabels();
        for (Label label : nodeLabels) {
            node.removeLabel(label);
        }
    }

    private void removeAllRelationships(Node node) {
        Iterable<Relationship> nodeRelationships = node.getRelationships();
        for (Relationship relationship : nodeRelationships) {
            relationship.delete();
        }
    }

}
