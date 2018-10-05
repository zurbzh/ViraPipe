package org.ngseq.metagenomics;

import com.github.lindenb.jbwa.jni.ShortRead;
import htsjdk.samtools.SAMRecord;
import io.hops.VirapipeHopsPipeline;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.seqdoop.hadoop_bam.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Usage
 spark-submit --master local[40] --driver-memory 4g --executor-memory 4g --class org.ngseq.metagenomics.SQLQueryBAM target/metagenomics-0.9-jar-with-dependencies.jar -in aligned -out unmapped -query "SELECT * from records WHERE readUnmapped = TRUE"

 **/


public class SQLQueryBAMTCGA {

  private static final Logger LOG = Logger.getLogger(SQLQueryBAMTCGA.class.getName());

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("SQLQueryBAM");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    Options options = new Options();
    Option opOpt = new Option("mapped", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");
    Option out = new Option("unmapped", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");
    Option virOut = new Option("virDir", true, "HDFS path for output files. If not present, the output files are not moved to HDFS.");
    Option queryOpt = new Option("query", true, "SQL query string.");
    Option baminOpt = new Option("in", true, "");

    options.addOption(opOpt);
    options.addOption(queryOpt);
    options.addOption(baminOpt);
    options.addOption(out);
    options.addOption(virOut);
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);

    } catch (ParseException exp) {
      System.err.println("Parsing failed.  Reason: " + exp.getMessage());
    }


    String mappedDir = (cmd.hasOption("mapped") == true) ? cmd.getOptionValue("mapped") : null;
    String unmappedDir =  (cmd.hasOption("unmapped") == true) ? cmd.getOptionValue("unmapped") : null;
    String virDir =  (cmd.hasOption("virOut") == true) ? cmd.getOptionValue("virOut") : null;
    String in = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] st = fs.listStatus(new Path(in));
    String query1 = "SELECT * from records WHERE readUnmapped = TRUE";
    String query2 = "SELECT * from records WHERE readUnmapped = FALSE";


    ArrayList<String> bamToFastaq = new ArrayList<>();


    for (FileStatus f : Arrays.asList(st)) {
      FileStatus[] b = fs.listStatus(new Path(f.getPath().toUri()));
      for (FileStatus bam : Arrays.asList(b)) {
        if (bam.getPath().getName().endsWith(".bam"))
          bamToFastaq.add(bam.getPath().toUri().getRawPath().toString());
      }
    }


    for (String s : bamToFastaq) {
      JavaPairRDD<LongWritable, SAMRecordWritable> bamPairRDD = sc.newAPIHadoopFile(s, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());
      //Map to SAMRecord RDD
      JavaRDD<SAMRecord> samRDD = bamPairRDD.map(v1 -> v1._2().get());
      JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag(),bam.getBaseQualityString(), bam.getReadPairedFlag(), bam.getFirstOfPairFlag(), bam.getSecondOfPairFlag()));


      Dataset<Row> samDF = sqlContext.createDataFrame(rdd, MyAlignment.class);
      samDF.registerTempTable("records");


      if (query1 != null) {

        List<String> items = Arrays.asList(s.split("\\s*/\\s*"));
        String name = items.get(items.size() - 1);

        Dataset df = sqlContext.sql(query1);
        df.show(100);

        df.groupBy("referenceName").count().show(100);
        df.groupBy("length").count().show(100);
        df.groupBy("cigar").count().show(100);
        //mappedreads.coalesce(1).write().csv(mappedDir + "/" + name);


        //df.drop(df.col("referenceName").startsWith("chr")).show();
        //Dataset mappedViralReads = df.drop(df.col("referenceName").startsWith("chr"));

        //mappedViralReads.write().csv(virDir + "/" + name);


        Dataset df2 = sqlContext.sql(query2);

        df2.groupBy("referenceName").count().show(100);
        df2.groupBy("length").count().show(100);
        df2.groupBy("cigar").count().show(100);

        //df2.show(100, false);

        //metadata.coalesce(1).write().csv(bwaOutDir + "/" + name);
        JavaRDD<String> tabDelRDD = dfToRDD(df2);

        JavaPairRDD<Text, SequencedFragment> fastqRDD = dfToFastq(df2);
        //tabDelRDD.saveAsTextFile(bwaOutDir+ "/" + name);
        //fastqRDD.saveAsNewAPIHadoopFile(bwaOutDir+ "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());




        fastqRDD.saveAsNewAPIHadoopFile(unmappedDir+"/"+name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


      }

   }
    sc.stop();

  }


  private static JavaRDD<String> dfToRDD (Dataset<Row> df) {
    return df.toJavaRDD().map(row ->  {

      String output = row.getAs("bases")+"\t"+row.getAs("cigar")+"\t"+row.getAs("duplicateRead")+"\t"
              +row.getAs("firstOfPairFlag")+"\t"+row.getAs("length")+"\t"+row.getAs("qualityBase")+"\t"+row.getAs("readName")+"\t"
              +row.getAs("readPairedFlag")+"\t"+row.getAs("readUnmapped") +"\t"+row.getAs("referenceName")+"\t"
              +row.getAs("secondOfPairFlag")+"\t"+row.getAs("start");

      return output;
    });
  }


  private static JavaPairRDD<Text, SequencedFragment> dfToFastq(Dataset<Row> df) {

    return df.toJavaRDD().mapToPair(row -> {



      String name = row.getAs("readName");


      //TODO: check values
      Text t = new Text(name);
      SequencedFragment sf = new SequencedFragment();
      sf.setSequence(new Text(row.getAs("bases").toString()));
      sf.setQuality(new Text(row.getAs("qualityBase").toString()));

      return new Tuple2<Text, SequencedFragment>(t, sf);
    });

  }


}