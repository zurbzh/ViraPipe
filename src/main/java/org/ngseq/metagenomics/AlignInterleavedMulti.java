package org.ngseq.metagenomics;


import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import io.hops.VirapipeHopsPipeline;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**Usage:
 //ON SINGLE NODE
 spark-submit --master local[${NUM_EXECUTORS}] --executor-memory 30g --class org.ngseq.metagenomics.AlignInterleavedMulti metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -out ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -ref ${REF_INDEX_IN_LOCAL_FS}
 //ON YARN CLUSTER
 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 30g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.AlignInterleavedMulti virapipe-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -out ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -ref ${REF_INDEX_IN_LOCAL_FS}
 **/


public class AlignInterleavedMulti {

  private static final Logger LOG = Logger.getLogger(AlignInterleavedMulti.class.getName());


  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("AlignInterleaved");
    JavaSparkContext sc = new JavaSparkContext(conf);

    Options options = new Options();
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );
    Option fqoutOpt = new Option( "out", true, "" );
    options.addOption( new Option( "in", true, "Path to fastq file in hdfs." ));
    options.addOption(new Option( "partitions", true, "number of file partitions to save"));
    options.addOption( refOpt );
    options.addOption( fqoutOpt );


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
    String in = cmd.getOptionValue("in");
    String ref = (cmd.hasOption("ref")==true)? cmd.getOptionValue("ref"):null;
    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    int numpartitions = (cmd.hasOption("partitions")==true)? Integer.valueOf(cmd.getOptionValue("partitions")):0;

    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] dirs = fs.listStatus(new Path(in));

    Arrays.asList(dirs).forEach(dir ->{

      String output = dir.getPath().getName();

      JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(dir.getPath().toString(), FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

      JavaPairRDD<Text, SequencedFragment> alignmentRDD = fastqRDD.mapPartitionsToPair(split -> {
        //THIS MUST BE LOADED HERE FOR YARN
        System.loadLibrary("bwajni");
        //TODO: Modify JBWA to use SAMRecord class, this would radically reduce map operations
        BwaIndex index = new BwaIndex(new File(ref));
        BwaMem mem = new BwaMem(index);

        List<ShortRead> L1 = new ArrayList<ShortRead>();
        List<ShortRead> L2 = new ArrayList<ShortRead>();


        while (split.hasNext()) {
          Tuple2<Text, SequencedFragment> next = split.next();
          String key = next._1.toString();
          String[] keysplit = key.split(" ");
          key = keysplit[0];

          SequencedFragment sf = new SequencedFragment();
          sf.setQuality(new Text(next._2.getQuality().toString()));
          sf.setSequence(new Text(next._2.getSequence().toString()));

          if (split.hasNext()) {

            Tuple2<Text, SequencedFragment> next2 = split.next();
            String key2 = next2._1.toString();
            String[] keysplit2 = key2.split(" ");
            key2 = keysplit2[0];

            SequencedFragment sf2 = new SequencedFragment();
            sf2.setQuality(new Text(next2._2.getQuality().toString()));
            sf2.setSequence(new Text(next2._2.getSequence().toString()));

            if(key.equalsIgnoreCase(key2)){
              L1.add(new ShortRead(key, sf.getSequence().toString().getBytes(), sf.getQuality().toString().getBytes()));
              L2.add(new ShortRead(key2, sf2.getSequence().toString().getBytes(), sf2.getQuality().toString().getBytes()));
            }else
              split.next();
          }
        }

        String[] aligns = mem.align(L1, L2);

        if (aligns != null) {

          ArrayList<Tuple2<Text, SequencedFragment>> filtered = new ArrayList<Tuple2<Text, SequencedFragment>>();
          Arrays.asList(aligns).forEach(aln -> {
            String[] fields = aln.split("\\t");
            int flag = Integer.parseInt(fields[1]);


            if (flag == 77) {
              String name = fields[0] + "/1";
              String bases = fields[9];
              String quality = fields[10];

              Text t = new Text(name);
              SequencedFragment sf = new SequencedFragment();
              sf.setSequence(new Text(bases));
              sf.setQuality(new Text(quality));
              filtered.add(new Tuple2<Text, SequencedFragment>(t, sf));
            } else if (flag == 141) {
              String name = fields[0] + "/2";
              String bases = fields[9];
              String quality = fields[10];

              Text t = new Text(name);
              SequencedFragment sf = new SequencedFragment();
              sf.setSequence(new Text(bases));
              sf.setQuality(new Text(quality));
              filtered.add(new Tuple2<Text, SequencedFragment>(t, sf));

            }

          });
          return filtered.iterator();
        } else
          return new ArrayList<Tuple2<Text, SequencedFragment>>().iterator(); //NULL ALIGNMENTS

      });

      if(numpartitions!=0){
        alignmentRDD.count();
        alignmentRDD.coalesce(numpartitions).saveAsNewAPIHadoopFile(outDir+"/"+output, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
      }
      else{
        alignmentRDD.saveAsNewAPIHadoopFile(outDir+"/"+output, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
      }
    });
    sc.stop();

  }

}
