package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.In;
import org.datanucleus.store.types.backed.Collection;
import scala.Int;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Created by zurbzh on 2018-09-11.
 */
public class SparcOutput {

    private static String output = "records";


    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("SQLQueryBAM");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Options options = new Options();

        Option inputClusters = new Option("clusters", true, "");
        Option inputSeq = new Option("seq", true, "");
        Option outputdir  = new Option("out", true, "");


        options.addOption(inputClusters);
        options.addOption(inputSeq);
        options.addOption(outputdir);
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }

        String inClusters = (cmd.hasOption("clusters")==true)? cmd.getOptionValue("clusters"):null;
        String inSeq = (cmd.hasOption("seq")==true)? cmd.getOptionValue("seq"):null;
        String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;

        //JavaRDD<String> seqRDD = sc.textFile(inSeq);
//

        JavaPairRDD<String, String> sparcClusRDD = sc.textFile(inClusters).mapToPair(line -> {
            String[] s = line.trim().split("\t");
            return new Tuple2<String, String>( s[0], s[1]);
        });



        JavaPairRDD<String, String> sparcSeq = sc.textFile(inSeq).mapToPair(line -> {
            String[] s = line.trim().split("\t");
            return new Tuple2<String, String>( s[1], s[2]);
        });


        JavaPairRDD<String, Tuple2<String, String>> ss = sparcClusRDD.join(sparcSeq);


        JavaRDD<SparcRecord> rdd = ss.map(line -> {
            SparcRecord record = new SparcRecord();
            record.setId(line._1);
            record.setClusterNumber(Integer.parseInt(line._2()._1));
            record.setSequence(line._2()._2);
            return record;

        });


        Dataset df = sqlContext.createDataFrame(rdd, SparcRecord.class);




        Dataset sortedDF = df.sort("clusterNumber");
        sortedDF.show(1000);
        sortedDF.registerTempTable(output);
        String maxQuery = "SELECT MAX(clusterNumber) FROM records";

        int firstRow = 0;
        int lastRow = 10000;
        String maxCluster = sqlContext.sql(maxQuery).first().get(0).toString();
        System.out.println("max number of cluster " + maxCluster);
        while (lastRow <= Integer.parseInt(maxCluster)) {

            String query = "SELECT * FROM records where clusterNumber >= '"+ firstRow +"' AND clusterNumber < '"+ lastRow +"'";
            Dataset<Row> resultDF = sqlContext.sql(query);
            if(outDir!=null){
                JavaRDD<String> resultRDD = dfToFasta(resultDF).coalesce(1);
                String name = Integer.toString(lastRow);
                resultRDD.saveAsTextFile(outDir + "/" + name);
            }
            firstRow += 10000;
            lastRow += 10000;
        }

        FileSystem fs = FileSystem.get(new Configuration());




        FileStatus[] dirs = fs.listStatus(new Path(outDir));
        for (FileStatus dir : dirs) {
            FileStatus[] st = fs.listStatus(dir.getPath());
            for (int i = 0; i < st.length; i++) {
                String fn = st[i].getPath().getName().toString();
                if (!fn.equalsIgnoreCase("_SUCCESS")) {
                    String folder = dir.getPath().toUri().getRawPath().toString();
                    String fileName = folder.substring(folder.lastIndexOf("/")+1) + ".fa";
                    String newPath = dir.getPath().toUri().getRawPath().toString() + "/" + fileName;
                    System.out.println("new name " + newPath);
                    Path srcPath = new Path(st[i].getPath().toUri().getRawPath().toString());
                    Path dstPath = new Path(newPath);
                    fs.rename(srcPath, dstPath);
                    System.out.println("*" + st[i].getPath().toUri().getRawPath().toString());
                }
            }
        }

        sc.stop();
    }

    private static JavaRDD<String> dfToFasta(Dataset<Row> df) {
        return df.toJavaRDD().map(row ->  {

            String output = ">" + row.getAs("id")+"_"+row.getAs("clusterNumber")+"\n"+row.getAs("sequence");

            return output;
        });
    }

}
