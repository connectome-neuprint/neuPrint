package org.janelia.flyem.neuprinter.model;

import java.util.Objects;


public class NeuronType {


    private String cellType;
    private String cellDescription;
    private String putativeTransmitter;


    public NeuronType(String cellType, String cellDescription, String putativeTransmitter) {
        this.cellType = cellType;
        this.cellDescription = cellDescription;
        this.putativeTransmitter = putativeTransmitter;
    }


    public String getCellType() {
        return cellType;
    }

    public String getCellDescription() {
        return cellDescription;
    }

    public String getPutativeTransmitter() {
        return putativeTransmitter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NeuronType that = (NeuronType) o;
        return Objects.equals(cellType, that.cellType);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + cellType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "NeuronType{" +
                "cellType='" + cellType + '\'' +
                ", cellDescription='" + cellDescription + '\'' +
                ", putativeTransmitter='" + putativeTransmitter + '\'' +
                '}';
    }
}
