package org.janelia.flyem.neuprintprocedures;


public class Location {

    private Long[] location;
    private Long x;
    private Long y;
    private Long z;

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
        return new Location(a.x + b.x, a.y + b.y,  a.z + b.z );
    }

    public static Double getDistanceBetweenLocations(Location a, Location b) {
        Long dx = (a.x - b.x);
        Long dy = (a.y - b.y);
        Long dz = (a.z - b.z);

        return Math.sqrt(dx*dx + dy*dy + dz*dz);
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Location) {
            final Location that = (Location) o;
            isEqual = this.x.equals(that.x) && this.y.equals(that.y) && this.z.equals(that.z);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.x.hashCode();
        result = 31 * result + this.y.hashCode();
        result = 31 * result + this.z.hashCode();
        return result;
    }

}
