package org.janelia.flyem.neuprinter.model;

import java.util.Comparator;

/**
 * A class for sorting {@link BodyWithSynapses} objects by the number of synapses they possess in descending order.
 */
public class SortBodyByNumberOfSynapses implements Comparator<BodyWithSynapses> {

    public int compare(BodyWithSynapses a, BodyWithSynapses b) {
        return (b.getNumberOfPreSynapses() + b.getNumberOfPostSynapses()) - (a.getNumberOfPreSynapses() + a.getNumberOfPostSynapses());
    }
}

