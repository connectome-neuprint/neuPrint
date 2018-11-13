package org.janelia.flyem.neuprintprocedures;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.spatial.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    public static final String SIZE = "size";
    public static final String NAME = "name";
    public static final String STATUS = "status";
    public static final String ROI_INFO = "roiInfo";
    public static final String RADIUS = "radius";
    public static final String ROW_NUMBER = "rowNumber";
    public static final String TIME_STAMP = "timeStamp";
    public static final String TYPE = "type";
    public static final String WEIGHT = "weight";
    public static final String SKELETON_ID = "skeletonId";
    public static final String SKEL_NODE_ID = "skelNodeId";
    public static final String SOMA_LOCATION = "somaLocation";
    public static final String SOMA_RADIUS = "somaRadius";
    public static final String MUTATION_UUID_ID = "mutationUuidAndId";
    //Relationship names
    public static final String CONNECTS_TO = "ConnectsTo";
    public static final String CONTAINS = "Contains";
    public static final String LINKS_TO = "LinksTo";
    public static final String SYNAPSES_TO = "SynapsesTo";

    public static Node getSegment(final GraphDatabaseService dbService, final Long bodyId, final String dataset) {
        return dbService.findNode(Label.label(dataset + "-" + SEGMENT), BODY_ID, bodyId);
    }

    public static Node getSynapseSetForNeuron(final Node neuron) {
        Node synapseSet = null;
        for (final Relationship containsRel : neuron.getRelationships(RelationshipType.withName(CONTAINS))) {
            final Node containedNode = containsRel.getEndNode();
            if (containedNode.hasLabel(Label.label(SYNAPSE_SET))) {
                synapseSet = containedNode;
            }
        }
        return synapseSet;
    }

    public static List<Location> getSynapseLocations(final Node synapseSet) {

        List<Location> locationList = new ArrayList<>();
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
            locationList.add(location);
        });

        return locationList;
    }

    public static Point getLocationAs3dCartesianPoint(final GraphDatabaseService dbService, Double x, Double y, Double z) {
        Map<String, Object> pointQueryResult = dbService.execute("RETURN point({ x:" + x + ", y:" + y + ", z:" + z + ", crs:'cartesian-3D'}) AS point").next();
        return (Point) pointQueryResult.get("point");
    }

    public static Map<String, SynapseCounter> getRoiInfoAsMap(String roiInfo) {
        Gson gson = new Gson();
        return gson.fromJson(roiInfo, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());
    }

}
