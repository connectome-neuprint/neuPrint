package org.janelia.flyem.neuprinter.model;

import com.google.common.base.CharMatcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A class representing a skeleton read from an swc file. A skeleton contains
 * a list of {@link SkelNode} instances and the bodyId of the neuron associated
 * with this skeleton.
 */
public class Skeleton {

    private List<SkelNode> skelNodeList;
    private Long associatedBodyId;
    private Optional<Long> mutationId = Optional.empty();
    private Optional<String> mutationUuid = Optional.empty();

    /**
     * Class constructor.
     */
    public Skeleton() {
    }

    /**
     *
     * @return list of {@link SkelNode} objects making up this skeleton
     */
    public List<SkelNode> getSkelNodeList() {
        return this.skelNodeList;
    }

    /**
     *
     * @return bodyId of neuron associated with this skeleton
     */
    public Long getAssociatedBodyId() {
        return this.associatedBodyId;
    }

    public Optional<Long> getMutationId() {
        return mutationId;
    }

    public Optional<String> getMutationUuid() {
        return mutationUuid;
    }

    @Override
    public String toString() {
        return "Skeleton{ " + "associatedBodyId = " + associatedBodyId +
                " skelTree = " + skelNodeList +
                "}";

    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Skeleton) {
            final Skeleton that = (Skeleton) o;
            isEqual = this.associatedBodyId.equals(that.associatedBodyId); //should be only one skeleton per body
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + associatedBodyId.hashCode();
        return result;
    }

    /**
     * Acquires a list of SkelNodes from a {@link BufferedReader} reading from an swc file.
     * The SkelNodes and bodyId of the neuron are added to the Skeleton object.
     *
     * @param reader {@link BufferedReader}
     * @param associatedBodyId bodyId of neuron
     * @throws IOException when swc file is not readable
     */
    public void fromSwc(final BufferedReader reader, final Long associatedBodyId) throws IOException {
        fromSwc(reader, associatedBodyId, null);
    }

    /**
     * Acquires a list of SkelNodes from a {@link BufferedReader} reading from an swc file.
     * The SkelNodes and bodyId of the neuron are added to the Skeleton object.
     *
     * @param reader {@link BufferedReader}
     * @param associatedBodyId bodyId of neuron
     * @param uuid dvid uuid associated with skeleton
     * @throws IOException when swc file is not readable
     */
    public void fromSwc(final BufferedReader reader, final Long associatedBodyId, final String uuid) throws IOException {
        String swcLine;
        List<SkelNode> skelNodeList = new ArrayList<>();
        List<Integer> location;
        float radius;
        int type;
        SkelNode parent;

        while ((swcLine = reader.readLine()) != null) {

            if (!swcLine.startsWith("#")) {

                String[] lineComponents = swcLine.split(" ");

                location = new ArrayList<>();
                for (int i = 2; i < 5; i++) {
                    int coordinate;
                    try {
                        coordinate = Integer.parseInt(lineComponents[i]);
                    } catch (NumberFormatException nfe) {
                        coordinate = Math.round(Float.parseFloat(lineComponents[i]));
                    }
                    location.add(coordinate);
                }

                radius = Float.parseFloat(lineComponents[5]);

                type = Integer.parseInt(lineComponents[1]);

                int parentIndex = Integer.parseInt(lineComponents[6]);

                int rowNumber = Integer.parseInt(lineComponents[0]);

                SkelNode skelNode;
                if (parentIndex != -1) {
                    parent = skelNodeList.get(parentIndex - 1);
                    skelNode = new SkelNode(associatedBodyId, location, radius, type, parent, rowNumber);
                    parent.addChild(skelNode);

                } else {
                    skelNode = new SkelNode(associatedBodyId, location, radius, type, null, rowNumber);
                }

                skelNodeList.add(skelNode);

            } else if (swcLine.startsWith("#${\"mutation id\"")) {
                this.mutationId = Optional.of(Long.parseLong(swcLine.replaceAll("[^0-9]", "")));
            }

        }

        this.skelNodeList = skelNodeList;
        this.associatedBodyId = associatedBodyId;
        this.mutationUuid = Optional.ofNullable(uuid);

    }

}
