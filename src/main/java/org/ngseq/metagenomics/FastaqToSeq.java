package org.ngseq.metagenomics;

import com.github.lindenb.jbwa.jni.ShortRead;
import io.hops.VirapipeHopsPipeline;
import org.antlr.v4.runtime.misc.Triple;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Predef;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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



        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] files = fs.listStatus(new Path(input));

        Arrays.asList(files).forEach(file -> {

            String output = file.getPath().getName().split("\\.")[0];
            JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(file.getPath().toString(), FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());


            JavaRDD<MyRead> rdd = fastqRDD.map(record -> {
                        MyRead read = new MyRead();
                        read.setKey(record._1.toString().split("/")[0]);
                        read.setSequence(record._2.getSequence().toString());
                        return read;
            });

            Dataset df = sqlContext.createDataFrame(rdd, MyRead.class);
            df.registerTempTable(tablename);



            String query = "SELECT key, concat_ws('',collect_list(sequence)) AS sequence FROM records GROUP BY key";
            Dataset seq = sqlContext.sql(query);
            seq.show(100);
            JavaRDD<String> sortedRDD = dfToRDD(seq);






            JavaRDD<String> indexRDD = sortedRDD.zipWithIndex().mapPartitions(line ->{
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
            indexRDD.coalesce(1).saveAsTextFile(seqOutDir + "/" + output);

        });

        FileStatus[] dirs = fs.listStatus(new Path(seqOutDir));
        for (FileStatus dir : dirs) {
            FileStatus[] st = fs.listStatus(dir.getPath());
            for (int i = 0; i < st.length; i++) {
                String fn = st[i].getPath().getName().toString();
                if (!fn.equalsIgnoreCase("_SUCCESS")) {
                    String folder = dir.getPath().toUri().getRawPath().toString();
                    String fileName = folder.substring(folder.lastIndexOf("/")+1) + ".seq";
                    String newPath = dir.getPath().getParent().toUri().getRawPath().toString() + "/" + fileName;

                    Path srcPath = new Path(st[i].getPath().toString());

                    FileUtil.copy(fs, srcPath, fs, new Path(newPath),true, new Configuration());
                    fs.delete(new Path(dir.getPath().toUri().getRawPath()));
                }
            }
        }
        sc.stop();

    }



    private static JavaRDD<String> dfToRDD (Dataset<Row> df) {
        return df.toJavaRDD().map(row ->  {

            String output = row.getAs("key")+"\t"+row.getAs("sequence");

            return output;
        });
    }



}
