package org.janelia.scicomp.neotool.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Post-synaptic data.
 */
public class PostSynapse extends Synapse {

    public static final String TYPE_VALUE = "post";
    public static final String LABEL_TYPE_VALUE = "PostSyn";

    @SerializedName("ConnectsFrom")
    private List<Location> connectsFrom;

    public PostSynapse() {
        this.connectsFrom = null;
    }

    @Override
    public String getType() {
        return TYPE_VALUE;
    }

    @Override
    public String getLabelType() {
        return LABEL_TYPE_VALUE;
    }

    @Override
    public List<Location> getConnections() {
        return connectsFrom;
    }

    @Override
    public int getNumberOfConnections() {
        return connectsFrom.size();
    }

}
