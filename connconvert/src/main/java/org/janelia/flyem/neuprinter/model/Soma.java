package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

import java.util.List;

/**
 * A class representing a neuron's soma. A soma has a three-dimensional location
 * and a radius.
 */
public class Soma {

    @SerializedName("Location")
    private final List<Integer> location;

    @SerializedName("Radius")
    private final Float radius;

    /**
     * Class constructor.
     *
     * @param location 3D location of soma
     * @param radius soma's radius
     */
    public Soma(List<Integer> location, float radius) {
        this.location = location;
        this.radius = radius;
    }

    /**
     *
     * @return radius of soma
     */
    public Float getRadius() {
        return radius;
    }

    /**
     *
     * @return list of integers representing soma's 3D location
     */
    public List<Integer> getLocation() {
        return location;
    }

    /**
     *
     * @return location of this soma as a neo4j {@link Point}
     */
    Point getLocationAsPoint() {
        return Values.point(9157, this.location.get(0), this.location.get(1), this.location.get(2)).asPoint();
    }
}
