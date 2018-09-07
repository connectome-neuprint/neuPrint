package org.janelia.flyem.neuprintprocedures.analysis;

public class RoiSynapseCount {

    private String roiName;
    private Long inputCount;
    private Long outputCount;

    public RoiSynapseCount(String roiName, Long inputCount, Long outputCount) {
        this.roiName = roiName;
        this.inputCount = inputCount;
        this.outputCount = outputCount;
    }

    public String getRoiName() {
        return roiName;
    }

    public Long getInputCount() {
        return inputCount;
    }

    public Long getOutputCount() {
        return outputCount;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof RoiSynapseCount) {
            final RoiSynapseCount that = (RoiSynapseCount) o;
            isEqual = this.roiName.equals(that.roiName);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.roiName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return this.roiName + " inputs:" + this.inputCount + ",outputs:" + this.outputCount;
    }
}
