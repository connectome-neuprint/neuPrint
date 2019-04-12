package org.janelia.flyem.neuprintloadprocedures.model;

/**
 * A class that extends org.janelia.flyem.neuprintloadprocedures.procedures.model.SynapseCounter to store the number of high-precision pre- and postsynaptic densities on a neuron.
 */
public class SynapseCounterWithHighPrecisionCounts extends SynapseCounter {

    private long preHP;
    private long postHP;

    /**
     * Class constructor. Counter starts at 0 for all values.
     */
    public SynapseCounterWithHighPrecisionCounts() {
        super();
        this.preHP = 0;
        this.postHP = 0;
    }

    /**
     * Class constructor. Counter starts at specified values for
     * pre, post, preHP, and postHP.
     *
     * @param pre    presynaptic density count
     * @param post   postsynaptic density count
     * @param preHP  high-precision presynaptic density count
     * @param postHP high-precision postsynaptic density count
     */
    public SynapseCounterWithHighPrecisionCounts(long pre, long post, long preHP, long postHP) {
        super(pre, post);
        this.preHP = preHP;
        this.postHP = postHP;
    }

    /**
     * @return high-precision presynaptic density count
     */
    public long getPreHP() {
        return this.preHP;
    }

    /**
     * @return high-precision postsynaptic density count
     */
    public long getPostHP() {
        return this.postHP;
    }

    /**
     * Increments the high-precision presynaptic density count.
     */
    public void incrementPreHP() {
        this.preHP++;
    }

    /**
     * Increments the high-precision postsynaptic density count.
     */
    public void incrementPostHP() {
        this.postHP++;
    }

    /**
     * Decrements the high-precision presynaptic density count.
     */
    public void decrementPreHP() {
        if (this.preHP > 0) {
            this.preHP--;
        }
    }

    /**
     * Increments the high-precision presynaptic density count.
     */
    public void decrementPostHP() {
        if (this.postHP > 0) {
            this.postHP--;
        }
    }

    @Override
    public String toString() {
        return "{pre: " + this.getPre() + ", post: " + this.getPost() + ", preHP: " + this.preHP + ", postHP: " + this.postHP + "}";
    }
}
