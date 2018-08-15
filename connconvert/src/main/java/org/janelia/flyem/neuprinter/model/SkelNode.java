package org.janelia.flyem.neuprinter.model;

import com.google.gson.Gson;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Point;

import java.util.ArrayList;
import java.util.List;

public class SkelNode {

    private List<Integer> location;
    private float radius;
    private Long associatedBodyId;
    private int type;
    private transient SkelNode parent; // only root doesn't have a parent
    private transient List<SkelNode> children = new ArrayList<>(); //no children means leaf node
    private int rowNumber;

    public SkelNode(Long associatedBodyId, List<Integer> location, float radius, int type, SkelNode parent, int rowNumber) {
        this.associatedBodyId = associatedBodyId;
        this.location = location;
        this.radius = radius;
        this.type = type;
        this.parent = parent;
        this.rowNumber = rowNumber;
    }

    public SkelNode(Long associatedBodyId, List<Integer> location, float radius, int rowNumber) {
        this.associatedBodyId = associatedBodyId;
        this.location = location;
        this.radius = radius;
        this.rowNumber = rowNumber;
    }

    public SkelNode() {
    }

    public static String getSkelNodeListJson(List<SkelNode> skelNodeList) {
        final Gson gson = new Gson();
        String json = gson.toJson(skelNodeList);
        return json;
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
            isEqual = this.location.equals(that.location);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + location.hashCode();
        return result;
    }

    public List<SkelNode> getChildren() {
        return this.children;
    }

    public SkelNode getParent() {
        return this.parent;
    }

    public List<Integer> getLocation() {
        return this.location;
    }

    public Integer getX() {
        return this.location.get(0);
    }

    public Integer getY() {
        return this.location.get(1);
    }

    public Integer getZ() {
        return this.location.get(2);
    }

    public int getRowNumber() {
        return this.rowNumber;
    }

    public String locationToStringKey(List<Integer> location) {
        return location.get(0) + ":" + location.get(1) + ":" + location.get(2);
    }

    public String getLocationString() {
        return locationToStringKey(this.location);
    }

    public Point getLocationAsPoint() {
        return Values.point(9157, this.location.get(0), this.location.get(1), this.location.get(2)).asPoint();
    }

    public float getRadius() {
        return this.radius;
    }

    public int getType() {
        return this.type;
    }

    public Long getAssociatedBodyId() {
        return this.associatedBodyId;
    }

    public void addChild(SkelNode child) {
        this.children.add(child);
    }

}

