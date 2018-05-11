package org.janelia.flyem.connconvert.model;

import java.util.ArrayList;
import java.util.List;

public abstract class SkelNode {  //TODO: should this be an abstract class? with root and rest as different types?

    private List<Integer> location;
    private float radius;
    private Long associatedBodyId;
    private String type;
//    private SkelNode parent; // only root doesn't have a parent
//    private List<SkelNode> children; //no children means leaf node



    public SkelNode () {
        this.associatedBodyId = new Long(0);
        this.location = null;
        this.radius = 0.0;
        this.type = null;
//        this.parent = parent;
//        this.children = children;
    }


    @Override
    public String toString() {
        return "SkelNode{" + "location= " + location +
                "radius= " + radius +
                "type= " + type +
                "}";

    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Synapse) {
            final SkelNode that = (SkelNode) o;
            isEqual = this.location.equals(that.location); //should be only one skeleton per body
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return this.location.hashCode();
    }

//    public List<SkelNode> getChildren() {
//        return this.children;
//    }
//
//    public SkelNode getParent() {
//        return this.parent;
//    }

    public List<Integer> getLocation() {
        return this.location;
    }

    public float getRadius() {
        return this.radius;
    }

    public String getType() {
        return this.type;
    }

    public Long getAssociatedBodyId() {
        return this.associatedBodyId;
    }


}

