package org.janelia.flyem.neuprintprocedures;

import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprinter.model.Synapse;

import java.util.Set;

public class MergeAction {

    @SerializedName("TargetBodyID")
    private Long targetBodyId;

    @SerializedName("BodiesMerged")
    private Set<Long> bodiesMerged;

    @SerializedName("TargetBodySize")
    private Long targetBodySize;

    @SerializedName("TargetBodySynapses")
    private Set<Synapse> targetBodySynapses;

    public Set<Synapse> getTargetBodySynapses() {
        return targetBodySynapses;
    }

    public Set<Long> getBodiesMerged() {
        return bodiesMerged;
    }

    public Long getTargetBodyId() {
        return targetBodyId;
    }

    public Long getTargetBodySize() {
        return targetBodySize;
    }

    @Override
    public String toString() {
        return "targetBodyId: " + this.targetBodyId +
                ", bodiesMerged: " + this.bodiesMerged +
                ", targetBodySize: " + this.targetBodySize +
                ", targetBodySynapses: " + this.targetBodySynapses;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof MergeAction) {
            final MergeAction that = (MergeAction) o;
            isEqual = this.targetBodyId.equals(that.targetBodyId) && this.bodiesMerged.equals(that.bodiesMerged);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.targetBodyId.hashCode();
        result = 31 * result + this.bodiesMerged.hashCode();
        return result;
    }

}
