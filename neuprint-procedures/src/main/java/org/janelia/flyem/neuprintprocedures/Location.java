package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Location implements Point {

    private Long[] location;
    private CoordinateReferenceSystem crs = CoordinateReferenceSystem.Cartesian_3D;

    public Location(Long x, Long y, Long z) {
        this.location = new Long[3];
        this.location[0] = x;
        this.location[1] = y;
        this.location[2] = z;
    }

    public Location(Long[] locationArray) {
        this.location = locationArray;
    }

    public Long getX() {
        return location[0];
    }

    public Long getY() {
        return location[1];
    }

    public Long getZ() {
        return location[2];
    }

    public Long[] getLocation() {
        return location;
    }

    @Override
    public CRS getCRS() {
        return this.crs;
    }

    public List<Coordinate> getCoordinates() {
        return Collections.singletonList(new Coordinate((double) this.getX(), (double) this.getY(), (double) this.getZ()));
    }

    public List<Long> getLocationAsList() {
        return new ArrayList<>(Arrays.asList(this.location));
    }

    public static Location getSummedLocations(Location a, Location b) {
        return new Location(a.getX() + b.getX(), a.getY() + b.getY(), a.getZ() + b.getZ());
    }

    public static Double getDistanceBetweenLocations(Location a, Location b) {
        Long dx = (a.getX() - b.getX());
        Long dy = (a.getY() - b.getY());
        Long dz = (a.getZ() - b.getZ());

        return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    public static Location getCentroid(List<Location> locationList) {
        Long[] centroidArray = new Long[3];
        double numberOfLocations = (double) locationList.size();
        Long[] summedLocation = locationList.stream().reduce(new Location(0L, 0L, 0L), Location::getSummedLocations).getLocation();
        for (int i = 0; i < 3; i++) {
            centroidArray[i] = Math.round(summedLocation[i] / numberOfLocations);
        }
        return new Location(centroidArray);
    }

    @Override
    public String toString() {
        return this.getX() + ":" + this.getY() + ":" + this.getZ();
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Location) {
            final Location that = (Location) o;
            isEqual = this.location[0].equals(that.location[0]) && this.location[1].equals(that.location[1]) && this.location[2].equals(that.location[2]);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.location[0].hashCode();
        result = 31 * result + this.location[1].hashCode();
        result = 31 * result + this.location[2].hashCode();
        return result;
    }

}
