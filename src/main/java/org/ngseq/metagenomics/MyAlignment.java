package org.ngseq.metagenomics;

import java.io.Serializable;

/**
 * Created by ilamaa on 11/3/16.
 */


    public class MyAlignment implements Serializable {

    private boolean duplicateRead;
    private String readName;
        private Integer start;
        private String referenceName;
        private Integer length;
        private String bases;
        private String cigar;
        private boolean readUnmapped;
        private String qualityBase;
        private boolean readPairedFlag;
        private boolean firstOfPairFlag;
        private boolean secondOfPairFlag;
    public MyAlignment(){
        
    }

    public MyAlignment(String readName, Integer start, String referenceName, Integer length, String bases, String cigar, boolean readUnmapped) {
        this.readName = readName;
        this.start = start;
        this.referenceName = referenceName;
        this.length = length;
        this.bases = bases;
        this.cigar = cigar;
        this.readUnmapped = readUnmapped;
    }

    public MyAlignment(String readName, Integer start, String referenceName, Integer length, String bases, String cigar, boolean readUnmapped, boolean duplicateRead) {
        this.readName = readName;
        this.start = start;
        this.referenceName = referenceName;
        this.length = length;
        this.bases = bases;
        this.cigar = cigar;
        this.readUnmapped = readUnmapped;
        this.duplicateRead = duplicateRead;
    }

    public MyAlignment(String readName, Integer start, String referenceName, Integer length, String bases, String cigar, boolean readUnmapped, boolean duplicateRead, String qualityBase, boolean readPairedFlag, boolean firstOfPairFlag, boolean secondOfPairFlag) {
        this.readName = readName;
        this.start = start;
        this.referenceName = referenceName;
        this.length = length;
        this.bases = bases;
        this.cigar = cigar;
        this.readUnmapped = readUnmapped;
        this.duplicateRead = duplicateRead;
        this.qualityBase = qualityBase;
        this.readPairedFlag = readPairedFlag;
        this.firstOfPairFlag = firstOfPairFlag;
        this.secondOfPairFlag = secondOfPairFlag;
    }

    public boolean isReadUnmapped() {
        return readUnmapped;
    }

    public boolean isReadPairedFlag() {return readPairedFlag;}
    public boolean isFirstOfPairFlag() {return firstOfPairFlag;}
    public boolean isSecondOfPairFlag() {return secondOfPairFlag;}
    public String getReadName() {
        return readName;
    }

    public String getReferenceName() {
        return referenceName;
    }


    public Integer getStart() {
            return start;
        }

    public void setStart(Integer start) {
            this.start = start;
        }

    public Integer getLength() {
            return length;
        }

    public void setLength(Integer length) {
            this.length = length;
        }

    public String getBases() {
            return bases;
        }

    public void setBases(String bases) {
            this.bases = bases;
        }

    public String getCigar() {
            return cigar;
        }

    public void setCigar(String cigar) {
            this.cigar = cigar;
        }

    public boolean isDuplicateRead() {
        return duplicateRead;
    }

    public void setDuplicateRead(boolean duplicateRead) {
        this.duplicateRead = duplicateRead;
    }

    public void setReadName(String readName) {
        this.readName = readName;
    }

    public void setReferenceName(String referenceName) {
        this.referenceName = referenceName;
    }

    public void setReadUnmapped(boolean readUnmapped) {
        this.readUnmapped = readUnmapped;
    }

    public String getQualityBase() {return qualityBase;}
    public void setQualityBase (String qualityBase) {this.qualityBase = qualityBase;}
}

