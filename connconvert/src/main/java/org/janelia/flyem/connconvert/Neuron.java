package org.janelia.flyem.connconvert;

public class Neuron {
    // for each neuron, create a node with properties:
    // bodyId (int)
    // name (string)
    // type (string)
    // status (string)
    // user (string)
    // size (int)

    // neuron also has labels: "Neuron" and string indicating dataset

    private final Integer id;
    private String status;
    private String name;
    private String neuronType;
    private int size;


    //private SynapseSet synapseSet;


    //private List<Label> labels = new ArrayList<>();


    public Neuron(int id, String name, String neuronType, String status, int size) {
        this.id = id;
        this.name = name;
        this.neuronType = neuronType;
        this.status = status;
        this.size = size;
        //this.labels.add(Label.label(dataset));
        //this.labels.add(Label.label("Neuron"));
    }



    //setters
//    public void setName(String name) {
//        this.name = name;
//    }
//
//    public void setType(String type) {
//        this.type = type;
//    }
//
//    public void setStatus(String status) {
//        this.status = status;
//    }
//
//    public void setUser(String user) {
//        this.user = user;
//    }
//
//    public void setSize(int size) {
//        this.size = size;
//    }
//
//    public void setPre(int pre) {
//        this.pre = pre;
//    }
//
//    public void setPost(int post) {
//        this.post = post;
//    }
//
//    public void addLabel(String label) {
//        this.labels.add(Label.label(label));
//    }
//
//    //public void removeLabel(String label) {
//    //
//    //}
//
//
//    //getters
    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getNeuronType() {
        return neuronType;
    }

    public String getStatus() {
        return status;
    }
    //
//    public String getUser() {
//        return user;
//    }
//
    public int getSize() {
        return size;
    }
//
//    public int getPre() {
//        return pre;
//    }
//
//    public int getPost() {
//        return post;
//    }
//
//    public List<Label> getLabels() {
//        return labels;
//    }


    @Override
    public String toString() {
        return "Neuron{" + "bodyid= " + id +
                ", status= " + status +
                ", name= " + name +
                ", neuronType= " + neuronType +
                ", size= " + size
                + "}";
    }


}
