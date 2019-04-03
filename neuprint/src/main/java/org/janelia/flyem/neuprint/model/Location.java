package org.janelia.flyem.neuprint.model;

import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

public class Location {
    private Long[] location;

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

    public Point getAsPoint() {
        return Values.point(9157, location[0], location[1], location[2]).asPoint();
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
