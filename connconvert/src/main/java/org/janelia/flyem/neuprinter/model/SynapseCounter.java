package org.janelia.flyem.neuprinter.model;

public class SynapseCounter {

    private int pre;
    private int post;
    private int total;

    public SynapseCounter() {
        this.pre = 0;
        this.post = 0;
        this.total = 0;
    }

    public SynapseCounter(int pre, int post) {
        this.pre = pre;
        this.post = post;
        this.total = pre+post;
    }

    public int getPre() {
        return this.pre;
    }

    public int getPost() {
        return this.post;
    }

    public int getTotal() {
        return this.total;
    }

    public void incrementPre() {
        this.pre++;
        this.total++;
    }

    public void incrementPost() {
        this.post++;
        this.total++;
    }

    @Override
    public String toString() {
        return "pre: " + this.pre + " post: " + this.post + " total: " + this.total;
    }

}
