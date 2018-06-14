package org.janelia.flyem.neuprinter.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import com.opencsv.CSVReader;

public class NeuronTypeTree {

    private String cellClass;
    private List<NeuronType> neuronTypeList;
    private static HashMap<String,NeuronTypeTree> NEURON_TYPE_TREE_MAP = new HashMap<>();


    private NeuronTypeTree(String cellClass, List<NeuronType> neuronTypeList) {
        this.cellClass = cellClass;
        this.neuronTypeList = neuronTypeList;
    }

    public static NeuronTypeTree getInstanceWithClass(String cellClass) {
        NeuronTypeTree existingNeuronTypeTree = NEURON_TYPE_TREE_MAP.get(cellClass);
        if (existingNeuronTypeTree!=null) {
            return existingNeuronTypeTree;
        } else {
            NeuronTypeTree newNeuronTypeTree = new NeuronTypeTree(cellClass, new ArrayList<>());
            NEURON_TYPE_TREE_MAP.put(cellClass, newNeuronTypeTree);
            return newNeuronTypeTree;
        }

    }


    public String getCellClass() {
        return cellClass;
    }

    public List<NeuronType> getNeuronTypeList() {
        return neuronTypeList;
    }

    private void addNeuronType(NeuronType neuronType) {
        this.neuronTypeList.add(neuronType);
    }

    @Override
    public String toString() {
        return "NeuronTypeTree{" +
                "cellClass='" + cellClass + '\'' +
                ", neuronTypeList=" + neuronTypeList +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NeuronTypeTree that = (NeuronTypeTree) o;
        return Objects.equals(cellClass, that.cellClass);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + cellClass.hashCode();
        return result;
    }


    public static HashMap<String,NeuronTypeTree> readTypeTree(String filePath) {
        CSVReader csvReader = null;

        List<NeuronType> neuronTypeList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            csvReader = new CSVReader(reader);
            String[] nextLine;
            int lineCount=0;
            while((nextLine = csvReader.readNext()) != null) {
                if (lineCount!=0) {
                    neuronTypeList.add(new NeuronType(nextLine[0], nextLine[1], nextLine[2]));
                    System.out.println(neuronTypeList.get(lineCount-1));
                }

                lineCount++;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        for (NeuronType neuronType : neuronTypeList) {

            String cellClass = neuronType.getCellType().split("-")[0];
            NeuronTypeTree neuronTypeTree = NeuronTypeTree.getInstanceWithClass(cellClass);
            neuronTypeTree.addNeuronType(neuronType);


        }

        return NEURON_TYPE_TREE_MAP;

    }



}
