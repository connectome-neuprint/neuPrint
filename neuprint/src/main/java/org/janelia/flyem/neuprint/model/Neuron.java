package org.janelia.flyem.neuprint.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprint.json.JsonUtils;
import org.neo4j.driver.v1.types.Point;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A class representing a neuron, as read from a
 * <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspec.md" target="_blank">neuron JSON file</a>.
 * Each neuron must have a unique ID. Optionally, a neuron can have size, name, type, instance, status, ROIs, a soma, and
 * a set of synaptic locations that belong to it.
 */
public class Neuron {

    //TODO: figure out how to add optional properties

    @SerializedName("id")
    private final Long id;

    @SerializedName("status")
    private final String status;

    @SerializedName("name")
    private final String name;

    @SerializedName("type")
    private final String type;

    @SerializedName("instance")
    private final String instance;

    @SerializedName("size")
    private final Long size;

    @SerializedName("rois")
    private final Set<String> rois;

    @SerializedName("soma")
    private final Soma soma;

    @SerializedName("synapseSet")
    private final Set<Location> synapseLocationSet;

    /**
     * Class constructor.
     *
     * @param id                 bodyId
     * @param status             status
     * @param name               name
     * @param type               neuron type
     * @param size               size (in voxels)
     * @param rois               rois associated with this neuron
     * @param soma               soma for this neuron
     * @param synapseLocationSet set of synaptic locations on this neuron
     */
    public Neuron(final Long id,
                  final String status,
                  final String name,
                  final String type,
                  final String instance,
                  final Long size,
                  final Set<String> rois,
                  final Soma soma,
                  final Set<Location> synapseLocationSet) {
        this.id = id;
        this.status = status;
        this.name = name;
        this.type = type;
        this.instance = instance;
        this.size = size;
        this.rois = rois;
        this.soma = soma;
        this.synapseLocationSet = synapseLocationSet;
    }

    /**
     * @return bodyId
     */
    public Long getId() {
        return id;
    }

    /**
     * @return status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * @return type of neuron
     */
    public String getType() {
        return type;
    }

    /**
     * @return instance of neuron
     */
    public String getInstance() {
        return instance;
    }

    /**
     * @return voxel size
     */
    public Long getSize() {
        return size;
    }

    /**
     * @return rois (without "-lm" suffix if present)
     */
    public Set<String> getRois() {
        return removeUnwantedRois(this.rois);
    }

    // TODO: remove these ROIs from input data and test data and remove this filter
    static Set<String> removeUnwantedRois(Set<String> rois) {
        Set<String> newRoiSet = new LinkedHashSet<>();
        if (rois != null) {
            newRoiSet = rois.stream()
                    .filter(r -> !(r.equals("seven_column_roi") || r.equals("kc_alpha_roi")))
                    .collect(Collectors.toSet());
        }
        return newRoiSet;
    }

    /**
     * Returns a list of rois in which this neuron is located with and without
     * a "dataset-" prefix.
     *
     * @param dataset the dataset in which this neuron exists
     * @return set of rois and rois prefixed with "dataset-"
     */
    public Set<String> getRoisWithAndWithoutDatasetPrefix(String dataset) {
        Set<String> rois = getRois();
        if (rois != null) {
            rois.addAll(rois.stream().map(r -> dataset + "-" + r).collect(Collectors.toSet()));
            return rois;
        } else {
            return null;
        }
    }

    /**
     * @return the {@link Soma} for this neuron
     */
    public Soma getSoma() {
        return soma;
    }

    /**
     * @return the {@link Soma} location as a neo4j {@link Point}
     */
    public Point getSomaLocation() {
        if (soma != null) {
            return soma.getLocationAsPoint();
        } else {
            return null;
        }
    }

    /**
     * @return radius of {@link Soma}
     */
    public Double getSomaRadius() {
        if (soma != null) {
            return soma.getRadius();
        } else {
            return null;
        }
    }

    /**
     * @return set of synaptic locations on this neuron
     */
    public Set<Location> getSynapseLocationSet() {
        return synapseLocationSet;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Neuron) {
            final Neuron that = (Neuron) o;
            isEqual = this.id.equals(that.id);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Neuron { " + "bodyId=" + id +
                ", status=" + status +
                ", name=" + name +
                ", type=" + type +
                ", instance=" + instance +
                ", size=" + size +
                ", rois=" + rois +
                ", soma=" + soma +
                ", synapseSet=" + synapseLocationSet
                + " }";
    }

    /**
     * Returns a list of Neurons deserialized from a neuron JSON string.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON format</a>.
     *
     * @param jsonString string containing neuron JSON
     * @return list of Neurons
     */
    public static List<Neuron> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, NEURON_LIST_TYPE);
    }

    /**
     * Returns a list of Neurons deserialized from a {@link BufferedReader} reading from a neuron JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON format</a>.
     *
     * @param reader {@link BufferedReader}
     * @return list of Neurons
     */
    public static List<Neuron> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader, NEURON_LIST_TYPE);
    }

    /**
     * Returns a Neuron deserialized from a single JSON object from a neuron JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON format</a>.
     *
     * @param reader {@link JsonReader}
     * @return Neuron
     */
    public static Neuron fromJsonSingleObject(final JsonReader reader) {
        return JsonUtils.GSON.fromJson(reader, Neuron.class);
    }

    private static Type NEURON_LIST_TYPE = new TypeToken<List<Neuron>>() {
    }.getType();

}
