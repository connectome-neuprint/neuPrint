package org.janelia.flyem.neuprintloadprocedures;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.spatial.Point;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphTraversalTools {

    //Node names
    public static final String META = "Meta";
    public static final String NEURON = "Neuron";
    public static final String SEGMENT = "Segment";
    public static final String SKELETON = "Skeleton";
    public static final String SKEL_NODE = "SkelNode";
    public static final String SYNAPSE = "Synapse";
    public static final String CONNECTION_SET = "ConnectionSet";
    public static final String SYNAPSE_SET = "SynapseSet";
    public static final String POST_SYN = "PostSyn";
    public static final String PRE_SYN = "PreSyn";
    //Property names
    public static final String BODY_ID = "bodyId";
    public static final String CONFIDENCE = "confidence";
    public static final String CLUSTER_NAME = "clusterName";
    public static final String DATASET = "dataset";
    public static final String DATASET_BODY_ID = "datasetBodyId";
    public static final String DATASET_BODY_IDs = "datasetBodyIds";
    public static final String LOCATION = "location";
    public static final String POST = "post";
    public static final String PRE = "pre";
    public static final String PRE_HP_THRESHOLD = "preHPThreshold";
    public static final String POST_HP_THRESHOLD = "postHPThreshold";
    public static final String SIZE = "size";
    public static final String NAME = "name";
    public static final String STATUS = "status";
    public static final String ROI_INFO = "roiInfo";
    public static final String RADIUS = "radius";
    public static final String ROW_NUMBER = "rowNumber";
    public static final String TIME_STAMP = "timeStamp";
    public static final String TOTAL_PRE_COUNT = "totalPreCount";
    public static final String TOTAL_POST_COUNT = "totalPostCount";
    public static final String TYPE = "type";
    public static final String WEIGHT = "weight";
    public static final String WEIGHT_HP = "weightHP";
    public static final String SKELETON_ID = "skeletonId";
    public static final String SKEL_NODE_ID = "skelNodeId";
    public static final String SOMA_LOCATION = "somaLocation";
    public static final String SOMA_RADIUS = "somaRadius";
    public static final String SUPER_LEVEL_ROIS = "superLevelRois";
    public static final String MUTATION_UUID_ID = "mutationUuidAndId";
    //Relationship names
    public static final String CONNECTS_TO = "ConnectsTo";
    public static final String CONTAINS = "Contains";
    public static final String LINKS_TO = "LinksTo";
    public static final String SYNAPSES_TO = "SynapsesTo";
    public static final String TO = "To";
    public static final String FROM = "From";

    public static Node getSegment(final GraphDatabaseService dbService, final long bodyId, final String dataset) {
        return dbService.findNode(Label.label(dataset + "-" + SEGMENT), BODY_ID, bodyId);
    }

    public static Node getSynapse(final GraphDatabaseService dbService, final Point location, final String dataset) {
        return dbService.findNode(Label.label(dataset + "-" + SYNAPSE), LOCATION, location);
    }

    public static Node getSynapse(final GraphDatabaseService dbService, final Double x, final Double y, final Double z, final String dataset) {
        Point point = getLocationAs3dCartesianPoint(dbService, x, y, z);
        return getSynapse(dbService, point, dataset);
    }

    public static Node getMetaNode(final GraphDatabaseService dbService, final String dataset) {
        return dbService.findNode(Label.label(META), DATASET, dataset);
    }

    public static Node getSkeleton(final GraphDatabaseService dbService, final long bodyId, final String dataset) {
        return dbService.findNode(Label.label(dataset + "-" + SKELETON), "skeletonId", dataset + ":" + bodyId);
    }

    public static Node getSkelNode(final GraphDatabaseService dbService, final String skelNodeId, final String dataset) {
        return dbService.findNode(Label.label(dataset + "-" + SKEL_NODE), SKEL_NODE_ID, skelNodeId);
    }

    public static Node getConnectionSetNode(final GraphDatabaseService dbService, final long preBodyId, final long postBodyId, final String dataset) {
        return dbService.findNode(Label.label(dataset + "-" + CONNECTION_SET), DATASET_BODY_IDs, dataset + ":" + preBodyId + ":" + postBodyId);
    }

    public static List<Node> getConnectionSetsForSynapse(final Node synapse) {
        List<Node> connectionSetList = new ArrayList<>();
        if (synapse.hasRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
            for (Relationship containsRel : synapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
                if (containsRel.getStartNode().hasLabel(Label.label(CONNECTION_SET))) {
                    connectionSetList.add(containsRel.getStartNode());
                }
            }
        }
        return connectionSetList;
    }

    public static Set<Node> getSynapsesForConnectionSet(final Node connectionSet) {
        final Set<Node> synapseSet = new HashSet<>();
        for (final Relationship containsRel : connectionSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            synapseSet.add(containsRel.getEndNode());
        }
        return synapseSet;
    }

    public static Relationship getConnectsToRelationshipBetweenSegments(final GraphDatabaseService dbService, final Long preBodyId, final Long postBodyId, final String dataset) {

        Relationship desiredConnectsToRelationship = null;
        if (preBodyId != null && postBodyId != null && dataset != null) {
            Node preSegment = getSegment(dbService, preBodyId, dataset);
            Node postSegment = getSegment(dbService, postBodyId, dataset);
            for (Relationship connectsToRel : preSegment.getRelationships(RelationshipType.withName(CONNECTS_TO), Direction.OUTGOING)) {
                long retrievedSegmentId = connectsToRel.getEndNode().getId();
                if (retrievedSegmentId == postSegment.getId()) {
                    desiredConnectsToRelationship = connectsToRel;
                    break;
                }
            }
        } else {
            throw new RuntimeException(String.format("preBodyId, postBodyId, and dataset fields must be non-null. Were %d, %d, and %s, respectively.", preBodyId, postBodyId, dataset));
        }

        return desiredConnectsToRelationship;
    }

    public static Relationship getConnectsToRelationshipBetweenSegments(final Node preSegment, final Node postSegment, final String dataset) {

        Relationship desiredConnectsToRelationship = null;
        if (preSegment != null && postSegment != null && dataset != null) {
            for (Relationship connectsToRel : preSegment.getRelationships(RelationshipType.withName(CONNECTS_TO), Direction.OUTGOING)) {
                long retrievedSegmentId = connectsToRel.getEndNode().getId();
                if (retrievedSegmentId == postSegment.getId()) {
                    desiredConnectsToRelationship = connectsToRel;
                    break;
                }
            }
        } else {
            throw new RuntimeException(String.format("preBody and postBody must be non-null. Were %s and %s.", preSegment, postSegment));
        }

        return desiredConnectsToRelationship;
    }

    public static Node getSynapseSetForNeuron(final Node neuron) {
        for (final Relationship containsRel : neuron.getRelationships(RelationshipType.withName(CONTAINS))) {
            final Node containedNode = containsRel.getEndNode();
            if (containedNode.hasLabel(Label.label(SYNAPSE_SET))) {
                return containedNode;
            }
        }
        return null;
    }

    public static Node getSkeletonNodeForNeuron(final Node neuron) {
        if (neuron.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            for (Relationship neuronContainsRel : neuron.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                final Node containedNode = neuronContainsRel.getEndNode();
                if (containedNode.hasLabel(Label.label(SKELETON))) {
                    return containedNode;
                }
            }
        }
        return null;
    }

    public static Set<Location> getSynapseLocationSet(final Node synapseSet) {

        Set<Location> synapseLocationSet = new HashSet<>();
        synapseSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING).forEach(relationship -> {
            final Node synapse = relationship.getEndNode();
            final Point locationPoint = (Point) synapse.getProperty(LOCATION);
            final Long[] locationArray = locationPoint
                    .getCoordinate()
                    .getCoordinate()
                    .stream()
                    .map(Math::round)
                    .collect(Collectors.toList())
                    .toArray(new Long[3]);
            final Location location = new Location(locationArray);
            synapseLocationSet.add(location);
        });

        return synapseLocationSet;
    }

    public static Set<Node> getSynapseNodesFromSynapseSet(final Node synapseSet) {

        Set<Node> synapseNodeSet = new HashSet<>();
        synapseSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING).forEach(relationship -> {
            final Node synapse = relationship.getEndNode();
            synapseNodeSet.add(synapse);
        });

        return synapseNodeSet;
    }


    public static Node getSegmentThatContainsSynapse(final Node synapse) {
        Node connectedSegment;

        final Node[] connectedSynapseSet = new Node[1];
        if (synapse.hasRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
            synapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING).forEach(r -> {
                if (r.getStartNode().hasLabel(Label.label(SYNAPSE_SET)))
                    connectedSynapseSet[0] = r.getStartNode();
            });
            connectedSegment = connectedSynapseSet[0].getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING).getStartNode();
        } else {
            connectedSegment = null;
        }

        return connectedSegment;
    }

    public static Set<String> getSynapseRois(final Node synapse, final Set<String> metaNodeRoiSet) {
        Map<String, Object> synapseNodeProperties = synapse.getAllProperties();
        return synapseNodeProperties.keySet().stream()
                .filter(metaNodeRoiSet::contains)
                .collect(Collectors.toSet());
    }

    public static Set<String> getSegmentRois(final Node segment, final Set<String> metaNodeRoiSet) {
        Map<String, Object> segmentNodeProperties = segment.getAllProperties();
        return segmentNodeProperties.keySet().stream()
                .filter(metaNodeRoiSet::contains)
                .collect(Collectors.toSet());

    }

    public static Point getLocationAs3dCartesianPoint(final GraphDatabaseService dbService, Double x, Double y, Double z) {
        Map<String, Object> pointQueryResult = dbService.execute("RETURN point({ x:" + x + ", y:" + y + ", z:" + z + ", crs:'cartesian-3D'}) AS point").next();
        return (Point) pointQueryResult.get("point");
    }

}
