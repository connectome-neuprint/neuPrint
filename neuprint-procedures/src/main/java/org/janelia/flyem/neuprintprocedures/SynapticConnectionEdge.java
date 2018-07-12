package org.janelia.flyem.neuprintprocedures;

public class SynapticConnectionEdge {

    private String sourceName;
    private String targetName;
    private Long distance;
    private transient SynapticConnectionNode source;
    private transient SynapticConnectionNode target;

    public SynapticConnectionEdge(SynapticConnectionNode source, SynapticConnectionNode target) {
        this.source = source;
        this.target = target;
        this.sourceName = source.getConnectionDescription();
        this.targetName = target.getConnectionDescription();
        setDistance();
    }

    public Long getDistance() {
        return distance;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getTargetName() {
        return targetName;
    }

    private void setDistance() {
        Long[] startCentroid = this.source.setAndGetCentroid();
        Long[] endCentroid = this.target.setAndGetCentroid();

        Long dx = (startCentroid[0] - endCentroid[0]);
        Long dy = (startCentroid[1] - endCentroid[1]);
        Long dz = (startCentroid[2] - endCentroid[2]);

        this.distance = Math.round(Math.sqrt(dx*dx + dy*dy + dz*dz));

    }


    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof SynapticConnectionEdge) {
            final SynapticConnectionEdge that = (SynapticConnectionEdge) o;
            isEqual = (this.source.equals(that.source)&&this.target.equals(that.target)) || (this.target.equals(that.target)&&this.source.equals(that.source));

        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.source.hashCode();
        result = 31 * result + this.target.hashCode();
        return result;
    }

}
