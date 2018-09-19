package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;

/**Usage
 spark-submit --master local[${NUM_EXECUTORS}] --executor-memory 20g --class org.ngseq.metagenomics.Assemble metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_groupped -out ${OUTPUT_PATH}/${PROJECT_NAME}_assembled -localdir ${LOCAL_TEMP_PATH} -merge -t ${ASSEMBLER_THREADS}

 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 20g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.Assemble metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_groupped -out ${OUTPUT_PATH}/${PROJECT_NAME}_assembled -localdir ${LOCAL_TEMP_PATH} -merge -t ${ASSEMBLER_THREADS}

 **/


public class Assemble {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("Assemble");
    JavaSparkContext sc = new JavaSparkContext(conf);

    Options options = new Options();

    Option splitOpt = new Option( "in", true, "" );
    Option cOpt = new Option( "t", true, "Threads" );
    Option kOpt = new Option( "m", true, "fraction of memory to be used per process" );
    Option ouOpt = new Option( "out", true, "" );

    options.addOption(new Option( "localdir", true, "Absolute path to local temp dir ( YARN must have write permissions if YARN used)"));
    options.addOption(new Option( "merge", "Merge output"));
    options.addOption(  new Option( "subdirs", "Read from subdirectories" ) );
    options.addOption(  new Option( "debug", "saves error log" ) );
    options.addOption(  new Option( "bin", true,"Path to megahit binary, defaults calls 'megahit'" ) );
    options.addOption(  new Option( "single", "Single reads option, default is interleaved paired-end" ) );
    options.addOption( splitOpt );
    options.addOption( cOpt );
    options.addOption( kOpt );
    options.addOption( ouOpt );

    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "spark-submit <spark specific args>", options, true );

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse( options, args );
    }
    catch( ParseException exp ) {
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
      System.exit(1);
    }
    String inputPath = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    String localdir = cmd.getOptionValue("localdir");
    boolean subdirs = cmd.hasOption("subdirs");
    boolean debug = cmd.hasOption("debug");
    String readstype = (cmd.hasOption("single")==true)? "-r":"--12";
    String bin = (cmd.hasOption("bin")==true)? cmd.getOptionValue("bin"):"megahit";

    int t = (cmd.hasOption("t")==true)? Integer.valueOf(cmd.getOptionValue("t")):1;
    double m = (cmd.hasOption("m")==true)? Double.valueOf(cmd.getOptionValue("m")):0.9;
    boolean mergeout = cmd.hasOption("merge");

    FileSystem fs = FileSystem.get(new Configuration());
    fs.mkdirs(fs,new Path(outDir),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));



    ArrayList<String> splitFileList = new ArrayList<>();
    if(subdirs){
      FileStatus[] dirs = fs.listStatus(new Path(inputPath));
      for (FileStatus dir : dirs){
        FileStatus[] st = fs.listStatus(dir.getPath());
        for (int i=0;i<st.length;i++){
          String fn = st[i].getPath().getName().toString();
          if(!fn.equalsIgnoreCase("_SUCCESS")){
            splitFileList.add(st[i].getPath().toUri().getRawPath().toString());
            System.out.println(st[i].getPath().toUri().getRawPath().toString());
          }
        }
      }
    }else{
      FileStatus[] st = fs.listStatus(new Path(inputPath));
      for (int i=0;i<st.length;i++){
        String fn = st[i].getPath().getName().toString();
        if(!fn.equalsIgnoreCase("_SUCCESS"))
          splitFileList.add(st[i].getPath().toUri().getRawPath().toString());
      }
    }

    JavaRDD<String> splitFilesRDD = sc.parallelize(splitFileList, splitFileList.size());
    Broadcast<String> bs = sc.broadcast(fs.getUri().toString());
    JavaRDD<String> outRDD = splitFilesRDD.mapPartitions(f -> {
      String path = f.next();
      System.out.println(path);
      String fname;
      if(path.lastIndexOf(".")<path.lastIndexOf("/"))
        fname = path.substring(path.lastIndexOf("/")+1);
      else fname = path.substring(path.lastIndexOf("/")+1, path.lastIndexOf("."));

      String tempName = String.valueOf((new Date()).getTime());




      DFSClient client = new DFSClient(URI.create(bs.getValue()), new Configuration());
      DFSInputStream hdfsstream = client.open(path);

      String premission_cmd = System.getenv("HADOOP_HOME")+"/bin/hdfs dfs -chmod -R 775 " + outDir;

      ProcessBuilder pc = new ProcessBuilder("/bin/sh", "-c", premission_cmd);
      Process perm_process = pc.start();

      BufferedReader err4 = new BufferedReader(new InputStreamReader(perm_process.getErrorStream()));
      String e4;
      while ((e4 = err4.readLine()) != null) {
        System.out.println(e4);
      }
      perm_process.waitFor();

      String ass_cmd = bin+" -t" + t + " -m" + m + " "+readstype+" /dev/stdin -o "+localdir+"/"+tempName; //YARN must have write permission to localdir
      System.out.println(ass_cmd);

      ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", ass_cmd);
      Process process = pb.start();

      BufferedReader hdfsinput = new BufferedReader(new InputStreamReader(hdfsstream));
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
      String line;
      while ((line = hdfsinput.readLine()) != null) {
        writer.write(line);
        writer.newLine();
      }
      writer.close();

      ArrayList<String> out = new ArrayList<String>();

      BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String e;
      while ((e = err.readLine()) != null) {
        System.out.println(e);
        out.add(e);
      }

      String copy_cmd = System.getenv("HADOOP_HOME")+"/bin/hdfs dfs -put "+localdir+"/"+tempName+" "+outDir+"/megahit_"+fname; //YARN should have write permissions to /tmp in HDFS
      ProcessBuilder pb2 = new ProcessBuilder("/bin/sh", "-c", copy_cmd);
      Process process2 = pb2.start();

      BufferedReader err2 = new BufferedReader(new InputStreamReader(process2.getErrorStream()));
      String e2;
      while ((e2 = err2.readLine()) != null) {
        System.out.println(e2);
        out.add(e2);
      }
      process2.waitFor();

      String delete_cmd = "rm -rf "+localdir+"/"+tempName;

      ProcessBuilder pb3 = new ProcessBuilder("/bin/sh", "-c", delete_cmd);
      Process process3 = pb3.start();
      BufferedReader err3 = new BufferedReader(new InputStreamReader(process3.getErrorStream()));
      String e3;
      while ((e3 = err3.readLine()) != null) {
        System.out.println(e3);
        out.add(e3);
      }
      process3.waitFor();

      out.add(ass_cmd);
      out.add(copy_cmd);
      out.add(delete_cmd);

      return out.iterator();
    });
    if(debug) outRDD.saveAsTextFile("pipe_errorlog/"+String.valueOf(new Date().getTime()));
    else outRDD.foreach(err -> System.out.println(err));


    if(mergeout){

      FileStatus[] dirs = fs.listStatus(new Path(outDir));
      for (FileStatus dir : dirs){
        if(dir.getPath().getName().toString().startsWith("megahit")){
          FileStatus[] st = fs.listStatus(dir.getPath());
          for (int i=0;i<st.length;i++){
            String fn = st[i].getPath().getName().toString();
            if(fn.endsWith(".fasta") || fn.endsWith(".fa")){
              String dst = outDir+"/"+dir.getPath().getName()+"_"+st[i].getPath().getName();
              FileUtil.copy(fs, st[i].getPath(), fs, new Path(dst),true, new Configuration());
            }
          }
          //fs.delete(dir.getPath(), true);
        }
      }
    }

    sc.stop();

  }

}
