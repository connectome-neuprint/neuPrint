package org.janelia.flyem.connconvert.model;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SkeletonTest {

    @Test
    public void testGetSwc() throws IOException {
        String filepath = "src/test/resources/exampleSwc.swc";

        File file = new File(filepath);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        Skeleton skeleton = new Skeleton();

        skeleton.fromSwc(bufferedReader,new Long(10));

        List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

        Assert.assertEquals(new Long(10), skeleton.getAssociatedBodyId());

        Assert.assertEquals(29, skelNodeList.size());

        Assert.assertEquals(new Integer(3117), skelNodeList.get(5).getLocation().get(0));

        Assert.assertEquals(22.5f, skelNodeList.get(21).getRadius(),.00001);

        Assert.assertEquals(skelNodeList.get(9),skelNodeList.get(10).getParent());

        Assert.assertEquals(null, skelNodeList.get(0).getParent());

        Assert.assertEquals(skelNodeList.get(8), skelNodeList.get(26).getParent());

        List<SkelNode> childList = new ArrayList<>();
        childList.add(skelNodeList.get(9));
        childList.add(skelNodeList.get(26));
        Assert.assertEquals(childList, skelNodeList.get(8).getChildren());
        Assert.assertEquals( new ArrayList<>(), skelNodeList.get(28).getChildren());

    }


    @Test
    public void testEqualsAndHashCode() {

        List<Integer> location1 = new ArrayList<>();
        location1.add(0);
        location1.add(1);
        location1.add(5);
        SkelNode skelNode1 = new SkelNode(new Long(10), location1 , 3.0f, 2, new SkelNode());
        List<Integer> location2 = new ArrayList<>();
        location2.add(0);
        location2.add(1);
        location2.add(5);
        SkelNode skelNode2 = new SkelNode(new Long(13), location2 , 34.0f, 1, new SkelNode());
        List<Integer> location3 = new ArrayList<>();
        location3.add(0);
        location3.add(1);
        location3.add(12);
        SkelNode skelNode3 = new SkelNode(new Long(13), location2 , 34.0f, 1, new SkelNode());

        //reflexive
        Assert.assertTrue(skelNode1.equals(skelNode1));
        //symmetric
        Assert.assertTrue(skelNode1.equals(skelNode2) && skelNode2.equals(skelNode1));
        //transitive
        Assert.assertTrue(skelNode1.equals(skelNode2)  && skelNode2.equals(skelNode3) && skelNode3.equals(skelNode1));
        //consistent
        Assert.assertTrue(skelNode2.equals(skelNode1) && skelNode2.equals(skelNode1));
        //not equal to null
        Assert.assertTrue(!location2.equals(null));

        Assert.assertNotSame(skelNode1, skelNode2);
        Assert.assertTrue(skelNode1.hashCode() == skelNode2.hashCode());


    }

}
