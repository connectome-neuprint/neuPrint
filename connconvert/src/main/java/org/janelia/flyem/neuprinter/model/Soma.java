package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

import java.util.List;

public class Soma {

    @SerializedName("Location")
    private final List<Integer> location;

    @SerializedName("Radius")
    private final Float radius;

    public Soma(List<Integer> location, float radius) {
        this.location = location;
        this.radius = radius;
    }

    public Float getRadius() {
        return radius;
    }

    public List<Integer> getLocation() {
        return location;
    }

    public Point getLocationAsPoint() {
        return Values.point(9157, this.location.get(0), this.location.get(1), this.location.get(2)).asPoint();
    }
}
