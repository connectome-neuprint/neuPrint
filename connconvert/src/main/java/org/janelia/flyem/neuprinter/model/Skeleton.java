package org.janelia.flyem.neuprinter.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Skeleton {

    //entry point for skeleton

    private List<SkelNode> skelNodeList;
    private Long associatedBodyId;


    public Skeleton(final List<SkelNode> skelNodeList, final Long associatedBodyId) {
        this.skelNodeList = skelNodeList;
        this.associatedBodyId = associatedBodyId;
    }

    public Skeleton() {
    }

    public List<SkelNode> getSkelNodeList() { return this.skelNodeList; }

    public Long getAssociatedBodyId() { return this.associatedBodyId;  }


    @Override
    public String toString() {
        return "Skeleton{ " + "associatedBodyId = " + associatedBodyId +
                " skelTree = " + skelNodeList +
                "}";

    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Skeleton) {
            final Skeleton that = (Skeleton) o;
            isEqual = this.associatedBodyId.equals(that.associatedBodyId); //should be only one skeleton per body
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + associatedBodyId.hashCode();
        return result;
    }

    public void fromSwc(final BufferedReader reader, final Long associatedBodyId) throws IOException {
        String swcLine;
        List<SkelNode> skelNodeList = new ArrayList<>();
        List<Integer> location = null;
        float radius = 0.0f;
        int type = 0;
        SkelNode parent = null;

        while((swcLine = reader.readLine()) != null) {

            if (swcLine.startsWith("#")) {
                //System.out.println("Skipping header");
            } else {

                String[] lineComponents = swcLine.split(" ");

                location = new ArrayList<>();
                for(int i=2; i<5; i++) {
                    Integer coordinate = null;
                    try{
                        coordinate = Integer.parseInt(lineComponents[i]);
                    } catch (NumberFormatException nfe) {
                        coordinate = Math.round(Float.parseFloat(lineComponents[i]));
                    }
                    location.add(coordinate);
                }


                radius = Float.parseFloat(lineComponents[5]);

                type = Integer.parseInt(lineComponents[1]);

                Integer parentIndex = Integer.parseInt(lineComponents[6]);

                int rowNumber = Integer.parseInt(lineComponents[0]);

                SkelNode skelNode = null;
                if (parentIndex!=-1) {
                    parent = skelNodeList.get(parentIndex-1);
                    skelNode = new SkelNode(associatedBodyId, location, radius, type, parent, rowNumber);
                    parent.addChild(skelNode);

                } else {
                    skelNode = new SkelNode(associatedBodyId, location, radius, type, null, rowNumber);
                }

                skelNodeList.add(skelNode);



            }

        }

        this.skelNodeList = skelNodeList;
        this.associatedBodyId = associatedBodyId;

    }




}
