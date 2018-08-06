package org.janelia.flyem.neuprintprocedures;

import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprinter.model.Synapse;

import java.util.Set;

public class CleaveAction {

    @SerializedName("NewBodyId")
    private Long newBodyId;

    @SerializedName("OrigBodyId")
    private Long originalBodyId;

    @SerializedName("NewBodySize")
    private Long newBodySize;

    @SerializedName("NewBodySynapses")
    private Set<Synapse> newBodySynapses;

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

    @Override
    public String toString() {
        return "newBodyId: " + this.newBodyId +
                ", originalBodyId: " + this.originalBodyId +
                ", newBodySize: " + this.newBodySize +
                ", newBodySynapses: " + this.newBodySynapses;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof CleaveAction) {
            final CleaveAction that = (CleaveAction) o;
            isEqual = this.newBodyId.equals(that.newBodyId) && this.originalBodyId.equals(that.originalBodyId);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.newBodyId.hashCode();
        result = 31 * result + this.originalBodyId.hashCode();
        return result;
    }
}
