package org.janelia.flyem.neuprint.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprint.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;

/**
 * Class representing meta information stored on the Meta node for interaction with neuPrintExplorer. Contains information for neuroglancer, definitons of statuses used in the dataset, host for ROI meshes, and the DVID server/uuid that this dataset maps to.
 * <a href="https://github.com/connectome-neuprint/neuPrint/tree/master/meta-data" target="_blank">See examples for fib25 and mb6 datasets.</a>
 */
public class MetaInfo {

    @SerializedName("neuroglancerInfo")
    private final String neuroglancerInfo;

    @SerializedName("uuid")
    private final String uuid;

    @SerializedName("dvidServer")
    private final String dvidServer;

    @SerializedName("meshHost")
    private final String meshHost;

    @SerializedName("statusDefinitions")
    private final String statusDefinitions;

    /**
     * Class constructor
     *
     * @param neuroglancerInfo  String containing information for neuroglancer to be used within neuPrintExplorer
     * @param uuid              UUID of dataset
     * @param dvidServer        DVID server at which dataset is stored
     * @param meshHost          Host of ROI meshes to be used by the skeleton viewer within neuPrintExplorer
     * @param statusDefinitions Definitions of statuses used for this dataset
     */
    public MetaInfo(final String neuroglancerInfo, String uuid, String dvidServer, String meshHost, String statusDefinitions) {
        this.neuroglancerInfo = neuroglancerInfo;
        this.uuid = uuid;
        this.dvidServer = dvidServer;
        this.meshHost = meshHost;
        this.statusDefinitions = statusDefinitions;
    }

    public String getDvidServer() {
        return dvidServer;
    }

    public String getMeshHost() {
        return meshHost;
    }

    public String getNeuroglancerInfo() {
        return neuroglancerInfo;
    }

    public String getStatusDefinitions() {
        return statusDefinitions;
    }

    public String getUuid() {
        return uuid;
    }

    public String toJson() {
        return JsonUtils.GSON.toJson(this, META_INFO_TYPE);
    }

    @Override
    public String toString() {
        return this.toJson();
    }

    public static MetaInfo fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader, META_INFO_TYPE);
    }

    private static Type META_INFO_TYPE = new TypeToken<MetaInfo>() {
    }.getType();
}


