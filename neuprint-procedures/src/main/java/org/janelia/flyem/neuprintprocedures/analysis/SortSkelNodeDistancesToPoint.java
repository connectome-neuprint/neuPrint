package org.janelia.flyem.neuprintprocedures.analysis;


import java.util.Comparator;

public class SortSkelNodeDistancesToPoint implements Comparator<SkelNodeDistanceToPoint> {

    public int compare(SkelNodeDistanceToPoint a, SkelNodeDistanceToPoint b) {
        return (int) Math.round(a.getDistanceToPoint()-b.getDistanceToPoint());
    }
}
