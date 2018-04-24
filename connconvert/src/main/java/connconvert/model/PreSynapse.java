package connconvert.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Pre-synaptic data.
 */
public class PreSynapse extends Synapse {

    public static final String TYPE_VALUE = "pre";
    public static final String NEO_TYPE_VALUE = "PreSyn";

    @SerializedName("ConnectsTo")
    private List<Location> connectsTo;

    public PreSynapse() {
        this.connectsTo = null;
    }

    @Override
    public boolean isPreSynapse() {
        return true;
    }

    @Override
    public String getType() {
        return TYPE_VALUE;
    }

    @Override
    public String getNeoType() {
        return NEO_TYPE_VALUE;
    }

    @Override
    public List<Location> getConnections() {
        return connectsTo;
    }

    @Override
    public int getNumberOfConnections() {
        return connectsTo.size();
    }

}
