package org.janelia.flyem.connconvert.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Soma {

    @SerializedName("Location")
    private final List<Integer> location;

    @SerializedName("Radius")
    private final float radius;


    public Soma(List<Integer> location, float radius) {
        this.location = location;
        this.radius = radius;
    }

    public float getRadius() {
        return radius;
    }

    public List<Integer> getLocation() {
        return location;
    }
}
