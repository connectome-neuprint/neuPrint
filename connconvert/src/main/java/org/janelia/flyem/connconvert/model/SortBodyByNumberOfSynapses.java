package org.janelia.flyem.connconvert.model;

import org.janelia.flyem.connconvert.BodyWithSynapses;

import java.util.Comparator;

public class SortBodyByNumberOfSynapses implements Comparator<BodyWithSynapses> {

    public int compare(BodyWithSynapses a, BodyWithSynapses b) {
        return (b.getNumberOfPreSynapses()+b.getNumberOfPostSynapses()) - (a.getNumberOfPreSynapses()+a.getNumberOfPostSynapses());
}
}

