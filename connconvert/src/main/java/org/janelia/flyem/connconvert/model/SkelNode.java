package org.janelia.flyem.connconvert.model;

import java.util.ArrayList;
import java.util.List;

public class SkelNode {  //TODO: should this be an abstract class? with root and rest as different types?

    private List<Integer> location;
    private float radius;
    private Long associatedBodyId;
    private int type;
    private SkelNode parent; // only root doesn't have a parent
    private List<SkelNode> children = new ArrayList<>(); //no children means leaf node
    private int rowNumber;



    public SkelNode (Long associatedBodyId, List<Integer> location, float radius, int type, SkelNode parent, int rowNumber) {
        this.associatedBodyId = associatedBodyId;
        this.location = location;
        this.radius = radius;
        this.type = type;
        this.parent = parent;
        this.rowNumber = rowNumber;
    }

    public SkelNode() {}


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
        return this.location.hashCode();
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

    public int getRowNumber() {return this.rowNumber; }

    public String locationToStringKey(List<Integer> location) {
        return location.get(0) + ":" + location.get(1) + ":" + location.get(2);
    }


    public String getLocationString() {
        return locationToStringKey(this.location);
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

