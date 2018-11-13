package org.janelia.flyem.neuprintprocedures.analysis;

import org.janelia.flyem.neuprintprocedures.Location;
import org.neo4j.graphdb.Node;

public class SkelNodeDistanceToPoint {

    private Node skelNode;
    private double distanceToPoint;
    private Location skelNodeLocation;

    public SkelNodeDistanceToPoint(Node skelNode, Location skelNodeLocation, Location location) {
        this.skelNode = skelNode;
        this.distanceToPoint = Location.getDistanceBetweenLocations(skelNodeLocation, location);
        this.skelNodeLocation = skelNodeLocation;
    }

    public double getDistanceToPoint() {
        return distanceToPoint;
    }

    public Location getSkelNodeLocation() {
        return skelNodeLocation;
    }

    public Node getSkelNode() {
        return skelNode;
    }

    @Override
    public String toString() {
        return this.distanceToPoint + " for " + this.skelNodeLocation;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SkelNodeDistanceToPoint) {
            final SkelNodeDistanceToPoint that = (SkelNodeDistanceToPoint) o;
            isEqual = this.skelNode.equals(that.skelNode);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.skelNode.hashCode();
        return result;
    }


}
