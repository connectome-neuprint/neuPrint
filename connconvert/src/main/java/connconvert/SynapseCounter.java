package connconvert;

public class SynapseCounter {
    private int preCount;
    private int postCount;

    public SynapseCounter(){
        this.preCount = 0;
        this.postCount = 0;
    }

    public int getPreCount() {
        return this.preCount;
    }

    public int getPostCount() {
        return this.postCount;
    }

    public void incrementPreCount() {
        this.preCount++;
    }

    public void incrementPostCount() {
        this.postCount++;
    }

    @Override
    public String toString() {
        return "pre: " + preCount + " post: " + postCount;
    }

}
