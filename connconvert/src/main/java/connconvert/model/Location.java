package connconvert.model;

/**
 * Synapse location data.
 */
public class Location {

    private final int x;
    private final int y;
    private final int z;

    public Location() {
        this(0, 0, 0);
    }

    public Location(final int x,
                    final int y,
                    final int z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public int getZ() {
        return z;
    }

    public String getKey() {
        return x + ":" + y + ":" + z;
    }

    public int[] toArray() {
        return new int[] { x, y, z };
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Location) {
            final Location that = (Location) o;
            isEqual = (this.x == that.x) && (this.y == that.y) && (this.z == that.z);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        // hash algorithm from Josh Bloch's Effective Java 2nd Edition Item 9
        int result = (31 * 17) + x;
        result = (31 * result) + y;
        return (31 * result) + z;
    }

    @Override
    public String toString() {
        return "[ " + x + ", " + y + ", " + z + " ]";
    }
}
