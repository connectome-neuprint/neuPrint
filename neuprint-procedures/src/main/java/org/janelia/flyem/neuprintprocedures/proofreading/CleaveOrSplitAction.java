package org.janelia.flyem.neuprintprocedures.proofreading;

import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprinter.model.Synapse;

import java.util.Set;

public class CleaveOrSplitAction {

    @SerializedName("DVIDuuid")
    private String dvidUuid;

    @SerializedName("MutationID")
    private Long mutationId;

    @SerializedName("Action")
    private String action;

    @SerializedName("NewBodyId")
    private Long newBodyId;

    @SerializedName("OrigBodyId")
    private Long originalBodyId;

    @SerializedName("NewBodySize")
    private Long newBodySize;

    @SerializedName("NewBodySynapses")
    private Set<Synapse> newBodySynapses;

    public String getDvidUuid() {
        return dvidUuid;
    }

    public Long getMutationId() {
        return mutationId;
    }

    public Set<Synapse> getNewBodySynapses() {
        return newBodySynapses;
    }

    public Long getNewBodyId() {
        return newBodyId;
    }

    public Long getNewBodySize() {
        return newBodySize;
    }

    public Long getOriginalBodyId() {
        return originalBodyId;
    }

    public String getAction() {
        return action;
    }

    @Override
    public String toString() {
        return "dvidUuid: " + this.dvidUuid +
                ", mutationId: " + this.mutationId +
                ", action: " + this.action +
                ", newBodyId: " + this.newBodyId +
                ", originalBodyId: " + this.originalBodyId +
                ", newBodySize: " + this.newBodySize +
                ", newBodySynapses: " + this.newBodySynapses;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof CleaveOrSplitAction) {
            final CleaveOrSplitAction that = (CleaveOrSplitAction) o;
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
