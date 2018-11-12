package org.janelia.flyem.neuprinter.model;

import com.google.gson.Gson;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

import java.util.ArrayList;
import java.util.List;

/**
 * A class representing a spherical node of a {@link Skeleton} read from an swc file.
 * A SkelNode possesses a three-dimensional location, a radius, a type, a row number (from swc file),
 * and the bodyId of its associated neuron. SkelNodes may have a parent (root nodes do not)
 * and multiple children.
 */
public class SkelNode {

    private List<Integer> location;
    private float radius;
    private Long associatedBodyId;
    private int type;
    private transient SkelNode parent; // only root doesn't have a parent
    private transient List<SkelNode> children = new ArrayList<>(); //no children means leaf node
    private int rowNumber;

    /**
     * Class constructor for initial loading into database.
     *
     * @param associatedBodyId bodyId of neuron
     * @param location         location of SkelNode center
     * @param radius           radius of SkelNode
     * @param type             type of SkelNode
     * @param parent           parent of SkelNode
     * @param rowNumber        row number of SkelNode within the swc file
     */
    public SkelNode(Long associatedBodyId, List<Integer> location, float radius, int type, SkelNode parent, int rowNumber) {
        this.associatedBodyId = associatedBodyId;
        this.location = location;
        this.radius = radius;
        this.type = type;
        this.parent = parent;
        this.rowNumber = rowNumber;
    }

    /**
     * Class constructor for reading SkelNode from the database within a stored procedure.
     *
     * @param associatedBodyId bodyId of neuron
     * @param location         location of SkelNode center
     * @param radius           radius of SkelNode
     * @param rowNumber        row number of SkelNode within the swc file
     */
    public SkelNode(Long associatedBodyId, List<Integer> location, float radius, int rowNumber) {
        this.associatedBodyId = associatedBodyId;
        this.location = location;
        this.radius = radius;
        this.rowNumber = rowNumber;
    }

    /**
     * Class constructor used for testing.
     */
    public SkelNode() {
    }

    /**
     * Produces a json from a list of SkelNodes.
     *
     * @param skelNodeList list of SkelNodes
     * @return String containing json
     */
    public static String getSkelNodeListJson(List<SkelNode> skelNodeList) {
        final Gson gson = new Gson();
        return gson.toJson(skelNodeList);
    }

    @Override
    public String toString() {
        return "SkelNode{" + " location = " + location +
                " radius = " + radius +
                " type = " + type +
                " rowNumber = " + rowNumber +
                "}";

    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SkelNode) {
            final SkelNode that = (SkelNode) o;
            isEqual = this.location.equals(that.location) && this.rowNumber == that.rowNumber;
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + location.hashCode();
        result = 31 * result + rowNumber;
        return result;
    }

    /**
     * @return list of this SkelNode's children
     */
    public List<SkelNode> getChildren() {
        return this.children;
    }

    /**
     * @return parent of this SkelNode (null if root)
     */
    public SkelNode getParent() {
        return this.parent;
    }

    /**
     * @return list of integers representing SkelNode's 3D location
     */
    public List<Integer> getLocation() {
        return this.location;
    }

    /**
     * @return the x coordinate of this SkelNode's location
     */
    public Integer getX() {
        return this.location.get(0);
    }

    /**
     * @return the y coordinate of this SkelNode's location
     */
    public Integer getY() {
        return this.location.get(1);
    }

    /**
     * @return the z coordinate of this SkelNode's location
     */
    public Integer getZ() {
        return this.location.get(2);
    }

    /**
     * @return row number of this SkelNode
     */
    public int getRowNumber() {
        return this.rowNumber;
    }

    private String locationToStringKey(List<Integer> location) {
        return location.get(0) + ":" + location.get(1) + ":" + location.get(2);
    }

    /**
     * Returns a string representation of this SkelNode's location.
     * In format "x:y:z".
     *
     * @return "x:y:z"
     */
    public String getLocationString() {
        return locationToStringKey(this.location);
    }

    /**
     * Returns this SkelNode's location as a neo4j {@link Point}.
     *
     * @return {@link Point}
     */
    public Point getLocationAsPoint() {
        return Values.point(9157, this.location.get(0), this.location.get(1), this.location.get(2)).asPoint();
    }

    /**
     * @return radius of SkelNode
     */
    public float getRadius() {
        return this.radius;
    }

    /**
     * @return type of SkelNode
     */
    public int getType() {
        return this.type;
    }

    /**
     * @return SkelNode's associated bodyId
     */
    public Long getAssociatedBodyId() {
        return this.associatedBodyId;
    }

    /**
     * Add a child to this SkelNode.
     *
     * @param child {@link SkelNode}
     */
    void addChild(SkelNode child) {
        this.children.add(child);
    }

}

