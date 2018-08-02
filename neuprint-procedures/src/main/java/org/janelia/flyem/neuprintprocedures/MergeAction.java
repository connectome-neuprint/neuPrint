package org.janelia.flyem.neuprintprocedures;

import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprinter.model.Synapse;

import java.util.List;
import java.util.Set;

public class MergeAction {

    @SerializedName("ResultBodyID")
    private Long resultBodyId;

    @SerializedName("BodiesMerged")
    private Set<Long> bodiesMerged;

    @SerializedName("ResultBodySize")
    private Long resultBodySize;

    @SerializedName("ResultBodySynapses")
    private Set<Synapse> resultBodySynapses;

    public Set<Synapse> getResultBodySynapses() {
        return resultBodySynapses;
    }

    public Set<Long> getBodiesMerged() {
        return bodiesMerged;
    }

    public Long getResultBodyId() {
        return resultBodyId;
    }

    public Long getResultBodySize() {
        return resultBodySize;
    }

    @Override
    public String toString() {
        return "resultBodyId: " + this.resultBodyId +
                ", bodiesMerged: " + this.bodiesMerged +
                ", resultBodySize: " + this.resultBodySize +
                ", resultBodySynapses: " + this.resultBodySynapses;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof MergeAction) {
            final MergeAction that = (MergeAction) o;
            isEqual = this.resultBodyId.equals(that.resultBodyId) && this.bodiesMerged.equals(that.bodiesMerged);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.resultBodyId.hashCode();
        result = 31 * result + this.bodiesMerged.hashCode();
        return result;
    }

}
