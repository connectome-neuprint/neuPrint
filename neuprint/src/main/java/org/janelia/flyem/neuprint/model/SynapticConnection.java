package org.janelia.flyem.neuprint.model;

import com.google.gson.annotations.SerializedName;

public class SynapticConnection {

    @SerializedName("pre")
    private Location preLocation;

    @SerializedName("post")
    private Location postLocation;

    public SynapticConnection(Location preLocation, Location postLocation) {
        this.preLocation = preLocation;
        this.postLocation = postLocation;
    }

    public Location getPreLocation() {
        return preLocation;
    }

    public Location getPostLocation() {
        return postLocation;
    }

    @Override
    public String toString() {
        return this.preLocation + "->" + this.postLocation;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SynapticConnection) {
            final SynapticConnection that = (SynapticConnection) o;
            isEqual = this.preLocation.equals(that.preLocation) && this.postLocation.equals(that.postLocation);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.preLocation.hashCode();
        result = 31 * result + this.postLocation.hashCode();
        return result;
    }
}
