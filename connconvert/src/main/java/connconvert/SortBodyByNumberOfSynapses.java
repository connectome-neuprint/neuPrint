package connconvert;

import java.util.Comparator;

public class SortBodyByNumberOfSynapses implements Comparator<BodyWithSynapses> {

    public int compare(BodyWithSynapses a, BodyWithSynapses b) {
        return (b.getPre()+b.getPost()) - (a.getPre()+a.getPost());
}
}

