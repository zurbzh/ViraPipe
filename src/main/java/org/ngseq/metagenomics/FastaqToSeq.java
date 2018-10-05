package org.ngseq.metagenomics;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.hadoop.io.Text;

import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by zurbzh on 2018-09-14.
 */
public class FastaqToSeq {

    private static final Logger LOG = Logger.getLogger(FastaqToSeq.class.getName());
    private static String tablename = "records";


    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("FastaqToSeq");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);



        Options options = new Options();
        Option pathOpt = new Option("in", true, "Path to fastq file in hdfs.");

        Option outOpt = new Option("out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");


        options.addOption( pathOpt );
        options.addOption( outOpt );


        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "spark-submit <spark specific args>", options, true );

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
        String input = cmd.getOptionValue("in");
        String seqOutDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;




        JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(input, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());


        JavaRDD<MyRead> rdd = fastqRDD.map(record -> {
                        MyRead read = new MyRead();
                        read.setKey(record._1.toString().split("/")[0]);
                        read.setRead(Integer.parseInt(record._1.toString().split("/")[1]));
                        read.setSequence(record._2.getSequence().toString());
                        read.setQuality(record._2.getQuality().toString());
                        return read;
        });

        Dataset df = sqlContext.createDataFrame(rdd, MyRead.class);
        df.registerTempTable(tablename);


                    // find pair ends
        Dataset pairEndKeys = df.groupBy("key").agg(count("*").as("count")).where("count > 1");

        Dataset<Row> pairDF = pairEndKeys.join(df, pairEndKeys.col("key").equalTo(df.col("key"))).drop(pairEndKeys.col("key"));


                   // separate forward and reverse reads
//        Dataset<Row> forward = pairDF.filter(pairDF.col("read").equalTo(1)).sort("key");
//        Dataset<Row> reverse = pairDF.filter(pairDF.col("read").equalTo(2)).sort("key");
//
//
//        dfToFastq(forward).coalesce(1).saveAsNewAPIHadoopFile(seqOutDir + "/forward", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
//        dfToFastq(reverse).coalesce(1).saveAsNewAPIHadoopFile(seqOutDir + "/reverse", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


                    // concatinate pair reads and write seq files
        pairDF.registerTempTable("pairs");
        String query = "SELECT key, concat_ws('',collect_list(sequence)) AS sequence FROM pairs GROUP BY key";
        Dataset seq = sqlContext.sql(query);
        seq.show(100);
        JavaRDD<String> sortedRDD = dfToRDD(seq);


         JavaRDD<String> indexRDD = sortedRDD.zipWithIndex().mapPartitions(line -> {
             ArrayList<String> index = new ArrayList<String>();
             while (line.hasNext()) {
                 Tuple2<String, Long> record = line.next();
                 String[] read = record._1.split("\t");
                 String readName = read[0];
                 String sequence = read[1];

                 Long indexNum = record._2 + 1;

                 index.add(indexNum.toString() + "\t" + readName + "\t" + sequence);
             }
                        return index.iterator();

         });
        indexRDD.coalesce(100).saveAsTextFile(seqOutDir);



//        FileStatus[] dirs = fs.listStatus(new Path(seqOutDir));
//        for (FileStatus dir : dirs) {
//            FileStatus[] st = fs.listStatus(dir.getPath());
//            for (int i = 0; i < st.length; i++) {
//                String fn = st[i].getPath().getName().toString();
//                if (!fn.equalsIgnoreCase("_SUCCESS")) {
//                    String folder = dir.getPath().toUri().getRawPath().toString();
//                    String fileName = folder.substring(folder.lastIndexOf("/")+1) + ".seq";
//                    String newPath = dir.getPath().getParent().toUri().getRawPath().toString() + "/" + fileName;
//
//                    Path srcPath = new Path(st[i].getPath().toString());
//
//                    FileUtil.copy(fs, srcPath, fs, new Path(newPath),true, new Configuration());
//                    fs.delete(new Path(dir.getPath().toUri().getRawPath()));
//                }
//            }
//        }
        sc.stop();

    }



    private static JavaRDD<String> dfToRDD (Dataset<Row> df) {
        return df.toJavaRDD().map(row ->  {

            String output = row.getAs("key")+"\t"+row.getAs("sequence");

            return output;
        });
    }


    private static JavaPairRDD<Text, SequencedFragment> dfToFastq(Dataset<Row> df) {

        return df.toJavaRDD().mapToPair(row -> {



            String name = row.getAs("key");


            //TODO: check values
            Text t = new Text(name);
            SequencedFragment sf = new SequencedFragment();
            sf.setSequence(new Text(row.getAs("sequence").toString()));
            sf.setQuality(new Text(row.getAs("quality").toString()));

            return new Tuple2<Text, SequencedFragment>(t, sf);
        });

    }






}
