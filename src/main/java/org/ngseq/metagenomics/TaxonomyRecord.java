package org.ngseq.metagenomics;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.Serializable;




    public class TaxonomyRecord implements Serializable {

    //qseqid    sseqid     pident length mismatch gapopen qstart    qend sstart     send        evalue bitscore
    //k141_53	CM000261.1	91.429	35	    1	    1	    238	    270	21459480	21459446	0.001	47.3
    private String acc;
    private String acc1;
    private String taxid;
    private String gi;
    private String species;


    public String getAcc() {
        return acc;
    }

    public void setAcc(String acc) {
        this.acc = acc;
    }

    public String getAcc1() {
        return acc1;
    }

    public void setAcc1(String acc1) {
        this.acc1 = acc1;
    }

    public String getTaxid() {return taxid;}

    public void setTaxid(String taxid) {this.taxid = taxid;}

    public String getGi() {
        return gi;
    }

    public void setGi(String gi) {
        this.gi = gi;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

}

