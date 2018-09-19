package org.ngseq.metagenomics;

import java.io.Serializable;

/**
 * Created by zurbzh on 2018-09-11.
 */
public class SparcRecord implements Serializable {

    private String id;
    private int clusterNumber;
    private String sequence;

    public String getId() {return id;}

    public void setId(String id) {
        this.id = id;
    }

    public int getClusterNumber() {
        return clusterNumber;
    }

    public void setClusterNumber(int clusterNumber) {
        this.clusterNumber = clusterNumber;
    }

    public String getSequence() {return sequence;}

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }
}
