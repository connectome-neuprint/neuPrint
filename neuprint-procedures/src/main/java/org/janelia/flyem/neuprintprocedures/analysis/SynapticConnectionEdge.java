package org.janelia.flyem.neuprintprocedures.analysis;

import com.google.gson.annotations.SerializedName;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Map;

public class SynapticConnectionEdge {

    @SerializedName("source")
    private String sourceName;
    @SerializedName("target")
    private String targetName;
    private Long distance;
    private transient SynapticConnectionVertex source;
    private transient SynapticConnectionVertex target;

    public SynapticConnectionEdge(SynapticConnectionVertex source, SynapticConnectionVertex target, Boolean cableDistance, final GraphDatabaseService dbService, final String datasetLabel, final Long bodyId) {
        this.source = source;
        this.target = target;
        this.sourceName = source.getConnectionDescription();
        this.targetName = target.getConnectionDescription();
        setDistance(cableDistance, dbService, datasetLabel, bodyId);
    }

    public Long getDistance() {
        return distance;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getTargetName() {
        return targetName;
    }

    private void setDistance(Boolean cableDistance, final GraphDatabaseService dbService, final String datasetLabel, final Long bodyId) {
        this.distance = cableDistance ? calculateCableDistance(dbService, datasetLabel, bodyId) : calculateEuclideanDistance();
    }

    private Long calculateEuclideanDistance() {
        Long[] startCentroid = this.source.getCentroidLocation();
        Long[] endCentroid = this.target.getCentroidLocation();

        Long dx = (startCentroid[0] - endCentroid[0]);
        Long dy = (startCentroid[1] - endCentroid[1]);
        Long dz = (startCentroid[2] - endCentroid[2]);

        return Math.round(Math.sqrt(dx * dx + dy * dy + dz * dz));
    }

    private Long calculateCableDistance(final GraphDatabaseService dbService, final String datasetLabel, final Long bodyId) {


        Map<String, Object> parametersMap = new HashMap<>();
        parametersMap.put("x1", this.source.getCentroidLocation()[0]);
        parametersMap.put("y1", this.source.getCentroidLocation()[1]);
        parametersMap.put("z1", this.source.getCentroidLocation()[2]);
        parametersMap.put("x2", this.target.getCentroidLocation()[0]);
        parametersMap.put("y2", this.target.getCentroidLocation()[1]);
        parametersMap.put("z2", this.target.getCentroidLocation()[2]);
        parametersMap.put("body", bodyId);
//        try {
//            parametersMap.put("body", findCommonBodyId());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        Map<String, Object> distanceQueryResult = null;

        try {
            Map<String,Object> point1Query = dbService.execute("CALL analysis.getNearestSkelNodeOnBodyToPoint($body,\"" + datasetLabel + "\",$x1,$y1,$z1) YIELD node AS node1",parametersMap).next();
            Map<String,Object> point2Query = dbService.execute(" CALL analysis.getNearestSkelNodeOnBodyToPoint($body,\"" + datasetLabel + "\",$x2,$y2,$z2) YIELD node AS node2",parametersMap).next();
            parametersMap.put("node1", point1Query.get("node1"));
            parametersMap.put("node2", point2Query.get("node2"));
            distanceQueryResult = dbService.execute("CALL analysis.calculateSkeletonDistance(\"" + datasetLabel + "\",$node1,$node2) YIELD value RETURN value", parametersMap).next();
        } catch (Exception e) {
            System.out.println("Error getting path between SkelNodes.");
            e.printStackTrace();
        }

        return (Long) distanceQueryResult.get("value");

    }

    private Long findCommonBodyId() throws Exception {
        String[] sourceBodies = this.source.getConnectionDescription().split("_");
        String[] targetBodies = this.target.getConnectionDescription().split("_");

        if (sourceBodies[0].equals(targetBodies[0]) || sourceBodies[0].equals(targetBodies[2])) {
            //both are pre on this body or source pre matches target post
            return Long.parseLong(sourceBodies[0]);
        } else if (sourceBodies[2].equals(targetBodies[2]) || sourceBodies[2].equals(targetBodies[0])) {
            //both are post on this body or source post matches target pre
            return Long.parseLong(sourceBodies[2]);
        } else {
            throw new Exception("Synaptic connection vertices for edge have no common bodyId.");
        }

    }


    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SynapticConnectionEdge) {
            final SynapticConnectionEdge that = (SynapticConnectionEdge) o;
            isEqual = (this.source.equals(that.source) && this.target.equals(that.target)) || (this.target.equals(that.target) && this.source.equals(that.source));

        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.source.hashCode();
        result = 31 * result + this.target.hashCode();
        return result;
    }

}
