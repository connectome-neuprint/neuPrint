package org.janelia.flyem.neuprint.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprint.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;

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
        return JsonUtils.GSON.toJson(this,META_INFO_TYPE);
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


