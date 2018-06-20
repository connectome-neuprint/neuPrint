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
    public Stream<NodeResult> mergeNeurons(@Name("node1") Long node1BodyId, @Name("node2") Long node2BodyId, @Name("datasetLabel") String datasetLabel) {
        if (node1BodyId == null || node2BodyId == null) return Stream.empty();
        // TODO: check that all labels are copied over.
        // TODO: make sure that the dataset label exists and that the two bodyIds exist within that dataset
        Map<String,Object> parametersMap = new HashMap<>();
        parametersMap.put("node1BodyId",node1BodyId);
        parametersMap.put("node2BodyId",node2BodyId);
        Map<String,Object> nodeQueryResult = dbService.execute("MATCH (node1{bodyId:$node1BodyId}), (node2{bodyId:$node2BodyId}) RETURN node1,node2", parametersMap).next();
        final Node node1 = (Node) nodeQueryResult.get("node1");
        final Node node2 = (Node) nodeQueryResult.get("node2");
        final Node newNode = dbService.createNode();

        if (node1.hasLabel(Label.label("Neuron")) && node2.hasLabel(Label.label("Neuron"))) {

            // grab write locks upfront
            try (Transaction tx=dbService.beginTx()) {
                tx.acquireWriteLock(node1);
                tx.acquireWriteLock(node2);
                tx.success();
            }

            //create a new body with new bodyId and properties
            //id for new body is by convention the first node Id listed in the function
            newNode.setProperty("bodyId", node1.getProperty("bodyId"));
            node1.setProperty("mergedBodyId", node1.getProperty("bodyId"));
            node1.removeProperty("bodyId");
            node2.setProperty("mergedBodyId", node2.getProperty("bodyId"));
            node2.removeProperty("bodyId");

            //duplicate all relationships for this body from the other two bodies
            ConnectsToRelationshipMap connectsToRelationshipMap = getConnectsToRelationshipMapForNodes(node1, node2, newNode);
            for (String stringKey : connectsToRelationshipMap.getNodeIdToConnectsToRelationshipHashMap().keySet()) {
                ConnectsToRelationship connectsToRelationship = connectsToRelationshipMap.getConnectsToRelationshipByKey(stringKey);
                Node startNode = connectsToRelationship.getStartNode();
                Node endNode = connectsToRelationship.getEndNode();
                Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName("ConnectsTo"));
                relationship.setProperty("weight", connectsToRelationship.getWeight());
            }

            //create relationships between synapse sets and new nodes (also deletes old relationship)
            parametersMap = new HashMap<>();
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

            // merge the two synapse nodes using apoc. inherits the datasetBodyId of the first node
            if (parametersMap.containsKey("ssnode1") && parametersMap.containsKey("ssnode2")) {
                Node newSynapseSetNode = (Node) dbService.execute("CALL apoc.refactor.mergeNodes([$ssnode1, $ssnode2], {properties:{datasetBodyId:\"discard\"}}) YIELD node RETURN node", parametersMap).next().get("node");
                //delete the extra relationship between new node and new synapse set node
                newNode.getRelationships(RelationshipType.withName("Contains")).iterator().next().delete();
            }

            //delete skeleton
            //TODO: trigger call for new skeleton/update skeleton for new node
            deleteSkeletonForNode(node1);
            deleteSkeletonForNode(node2);

//            Map<String,Node> node1NeuronParts = new HashMap<>();
//
//            //neuron parts -> move to new body and recalculate?
//            //inherits the neuronPartId from the first listed body
//            //sum pre, post, size for matching neurons (i.e. if labels are the same)
//            for (Relationship node1NeuronPartRelationship : node1.getRelationships(RelationshipType.withName("PartOf"))) {
//                //get the neuronpart
//                Node neuronPart = node1NeuronPartRelationship.getStartNode();
//                //connect to the new node
//                neuronPart.createRelationshipTo(newNode,RelationshipType.withName("PartOf"));
//                //delete the old node relationship
//                node1NeuronPartRelationship.delete();
//                for (Label neuronPartLabel : neuronPart.getLabels()) {
//                    if (!neuronPartLabel.name().equals("NeuronPart") && !neuronPartLabel.name().equals(datasetLabel)) {
//                        node1NeuronParts.put(neuronPartLabel.name(),neuronPart);
//                    }
//                }
//
//            }
//            for (Relationship node2NeuronPartRelationship: node2.getRelationships(RelationshipType.withName("PartOf"))) {
//                //get the neuronpart
//                Node neuronPart = node2NeuronPartRelationship.getStartNode();
//                //connect to the new node
//                neuronPart.createRelationshipTo(newNode,RelationshipType.withName("PartOf"));
//                //delete the old node relationship
//                node2NeuronPartRelationship.delete();
//                for (Label neuronPartLabel : neuronPart.getLabels()) {
//                    if (!neuronPartLabel.name().equals("NeuronPart") && !neuronPartLabel.name().equals(datasetLabel)) {
//                        Node node1NeuronPart = node1NeuronParts.get(neuronPartLabel.name());
//                        Map<String,Object> node2NeuronPartProperties = neuronPart.getProperties("pre","post","size");
//                        Map<String,Object> node1NeuronPartProperties = node1NeuronPart.getProperties("pre","post","size");
//                        for (String propertyName: )
//                    }
//                }
//
//            }


            //



            //properties and labels on body

            //TODO: connect merged body nodes to new node with "MergedTo"

        } else {
            newNode.delete();
            System.out.println("Error using proofreader.mergeNodes: both nodes must be labeled :Neuron.");
            System.exit(1);
        }

        return Stream.of(new NodeResult(newNode));


    }


    private ConnectsToRelationshipMap getConnectsToRelationshipMapForNodes(final Node node1, final Node node2, final Node newNode) {

        ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
        List<Node> nodeList = new ArrayList<>();
        nodeList.add(node1);
        nodeList.add(node2);
        for (Node node : nodeList) {
            for (Relationship nodeRelationship : node.getRelationships(RelationshipType.withName("ConnectsTo"))) {
                    Long weight = (Long) nodeRelationship.getProperty("weight");
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
    }
}
