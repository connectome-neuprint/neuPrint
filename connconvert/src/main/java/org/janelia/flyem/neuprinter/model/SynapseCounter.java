package org.janelia.flyem.neuprinter.model;

public class SynapseCounter {

    private int pre;
    private int post;

    public SynapseCounter() {
        this.pre = 0;
        this.post = 0;
    }

    public SynapseCounter(int pre, int post) {
        this.pre = pre;
        this.post = post;
    }

    public int getPre() {
        return this.pre;
    }

    public int getPost() {
        return this.post;
    }

    public void incrementPre() {
        this.pre++;
    }

    public void incrementPost() {
        this.post++;
    }

    @Override
    public String toString() {
        return "pre: " + this.pre + " post: " + this.post ;
    }

}
