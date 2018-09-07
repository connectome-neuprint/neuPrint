package org.janelia.flyem.neuprintprocedures.proofreading;

import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprinter.model.Synapse;

import java.util.Set;

public class MergeAction {

    @SerializedName("DVIDuuid")
    private String dvidUuid;

    @SerializedName("MutationID")
    private Long mutationId;

    @SerializedName("Action")
    private String action;

    @SerializedName("TargetBodyID")
    private Long targetBodyId;

    @SerializedName("BodiesMerged")
    private Set<Long> bodiesMerged;

    @SerializedName("TargetBodySize")
    private Long targetBodySize;

    @SerializedName("TargetBodySynapses")
    private Set<Synapse> targetBodySynapses;

    @SerializedName("TargetBodyName")
    private String targetBodyName;

    @SerializedName("TargetBodyStatus")
    private String targetBodyStatus;

    public String getDvidUuid() {
        return dvidUuid;
    }

    public Long getMutationId() {
        return mutationId;
    }

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

    public String getAction() {
        return action;
    }

    public String getTargetBodyName() {
        return targetBodyName;
    }

    public String getTargetBodyStatus() {
        return targetBodyStatus;
    }

    @Override
    public String toString() {
        return "dvidUuid: " + this.dvidUuid +
                ", mutationId: " + this.mutationId +
                ", targetBodyId: " + this.targetBodyId +
                ", targetBodyName: " + this.targetBodyName +
                ", targetBodyStatus: " + this.targetBodyStatus +
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
            isEqual = this.dvidUuid.equals(that.dvidUuid) && this.mutationId.equals(that.mutationId);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.dvidUuid.hashCode();
        result = 31 * result + this.mutationId.hashCode();
        return result;
    }

}
