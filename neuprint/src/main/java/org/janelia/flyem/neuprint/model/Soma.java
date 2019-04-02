package org.janelia.flyem.neuprint.model;

import com.google.gson.annotations.SerializedName;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

/**
 * A class representing a neuron's soma. A soma has a three-dimensional location
 * and a radius.
 */
public class Soma {

    @SerializedName("location")
    private final Location location;

    @SerializedName("radius")
    private final Double radius;

    /**
     * Class constructor.
     *
     * @param location 3D location of soma
     * @param radius   soma's radius
     */
    public Soma(Location location, Double radius) {
        this.location = location;
        this.radius = radius;
    }

    /**
     * @return radius of soma
     */
    public Double getRadius() {
        return radius;
    }

    /**
     * @return list of integers representing soma's 3D location
     */
    public Location getLocation() {
        return location;
    }

    /**
     * @return location of this soma as a neo4j {@link Point}
     */
    Point getLocationAsPoint() {
        return Values.point(9157, this.location.getX(), this.location.getY(), this.location.getZ()).asPoint();
    }
}
