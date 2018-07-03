package org.janelia.flyem.neuprinter.model;

public class Roi {

    private String roiName;
    private long preCount;
    private long postCount;

    public Roi(String roiName, long preCount, long postCount) {
        this.roiName = roiName;
        this.preCount = preCount;
        this.postCount = postCount;
    }

    public void setPreCount(long preCount) {
        this.preCount = preCount;
    }

    public void setPostCount(long postCount) {
        this.postCount = postCount;
    }

    public long getPreCount() {
        return preCount;
    }

    public long getPostCount() {
        return postCount;
    }

    public String getRoiName() {
        return roiName.replaceAll("[^A-Za-z0-9]","");
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Roi) {
            final Roi that = (Roi) o;
            isEqual = this.roiName.equals(that.roiName);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + roiName.hashCode();
        return result;
    }
}
