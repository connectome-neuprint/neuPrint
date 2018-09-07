package org.janelia.flyem.neuprintprocedures.analysis;


public class Location {

    private Long[] location;
    private transient Long x;
    private transient Long y;
    private transient Long z;

    public Location(Long x, Long y, Long z) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.location = new Long[3];
        this.location[0] = x;
        this.location[1] = y;
        this.location[2] = z;
    }

    public Long getX() {
        return x;
    }

    public Long getY() {
        return y;
    }

    public Long getZ() {
        return z;
    }

    public Long[] getLocation() {
        return location;
    }

    public static Location getSummedLocations(Location a, Location b) {
        return new Location(a.x + b.x, a.y + b.y, a.z + b.z);
    }

    public static Double getDistanceBetweenLocations(Location a, Location b) {
        Long dx = (a.x - b.x);
        Long dy = (a.y - b.y);
        Long dz = (a.z - b.z);

        return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    @Override
    public String toString() {
        return this.x + ":" + this.y + ":" + this.z;
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
