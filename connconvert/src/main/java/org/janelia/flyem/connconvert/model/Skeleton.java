package org.janelia.flyem.connconvert.model;

import java.util.List;

public class Skeleton {

    //entry point for skeleton

    SkelTree skelTree; //TODO: should this be a tree?
    Long associatedBodyId;


    public Skeleton(List<SkelNode> skelNodeList, Long associatedBodyId) {
        this.skelTree = skelNodeList;
        this.associatedBodyId = associatedBodyId;
    }


    @Override
    public String toString() {
        return "Skeleton{" + "associatedBodyId= " + associatedBodyId +
                "skelTree= " + skelTree +
                "}";

    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Synapse) {
            final Skeleton that = (Skeleton) o;
            isEqual = this.associatedBodyId.equals(that.associatedBodyId); //should be only one skeleton per body
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return this.associatedBodyId.hashCode();
    }



}
