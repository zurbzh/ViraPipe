package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

/**
 Usage:

 spark-submit --master local[${NUM_EXECUTORS}] --executor-memory 10g --class org.ngseq.metagenomics.BlastNFilter metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_contigs -out ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -db ${BLAST_HUMAN_DATABASE} -task megablast -outfmt 6 -threshold 70 -num_threads ${BLAST_THREADS}

 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.BlastNFilter metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_contigs -out ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -db ${BLAST_HUMAN_DATABASE} -task megablast -outfmt 6 -threshold 70 -num_threads ${BLAST_THREADS}
 */
public class BlastNFilter {

    public static void main(String[] args) throws IOException {

        Options options = new Options();
        options.addOption(new Option( "temp", "Temporary output"));
        options.addOption(new Option( "out", true, "" ));
        options.addOption(new Option( "in", true, "" ));
        options.addOption(new Option( "word_size", ""));
        options.addOption(new Option( "gapopen", true, "" ));
        options.addOption(new Option( "gapextend", true, "" ));
        options.addOption(new Option( "penalty", true, "" ));
        options.addOption(new Option( "reward", true, "" ));
        options.addOption(new Option( "max_target_seqs", true, "" ));
        options.addOption(new Option( "evalue", true, "" ));
        options.addOption(new Option( "show_gis", "" ));
        options.addOption(new Option( "outfmt", true, "" ));
        options.addOption(new Option( "num_threads", true, "" ));
        options.addOption(new Option( "db", true, "Path to local Blast human genome database (database must be available on every node under the same path)"));
        options.addOption(new Option( "task", true, "" ));
        options.addOption(new Option( "threshold", true, "" ));
        options.addOption(new Option( "fa", true, "Include only files with extension given " ));
        options.addOption(  new Option( "bin", true,"Path to blastn binary, defaults calls 'blastn'" ) );

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

        String input = cmd.getOptionValue("in");
        String output = cmd.getOptionValue("out");
        int word_size = (cmd.hasOption("word_size")==true)? Integer.valueOf(cmd.getOptionValue("word_size")):11;
        int gapopen = (cmd.hasOption("gapopen")==true)? Integer.valueOf(cmd.getOptionValue("gapopen")):0;
        int gapextend = (cmd.hasOption("gapextend")==true)? Integer.valueOf(cmd.getOptionValue("gapextend")):2;
        int penalty = (cmd.hasOption("penalty")==true)? Integer.valueOf(cmd.getOptionValue("penalty")):-1;
        int reward = (cmd.hasOption("reward")==true)? Integer.valueOf(cmd.getOptionValue("reward")):1;
        int max_target_seqs = (cmd.hasOption("max_target_seqs")==true)? Integer.valueOf(cmd.getOptionValue("max_target_seqs")):10;
        double evalue = (cmd.hasOption("evalue")==true)? Double.valueOf(cmd.getOptionValue("evalue")):0.001;
        boolean show_gis = cmd.hasOption("show_gis");
        String outfmt = (cmd.hasOption("outfmt")==true)? cmd.getOptionValue("outfmt"):"6";
        int num_threads = (cmd.hasOption("num_threads")==true)? Integer.valueOf(cmd.getOptionValue("num_threads")):1;
        String bin = (cmd.hasOption("bin")==true)? cmd.getOptionValue("bin"):"blastn";

        String db = cmd.getOptionValue("db"); //We want to filter out human matches, so use human db as default
        String task = (cmd.hasOption("task")==true)? cmd.getOptionValue("task"):"blastn";
        int threshold = (cmd.hasOption("threshold")==true)? Integer.valueOf(cmd.getOptionValue("threshold")):0;
        String fastaonly = (cmd.hasOption("fa")==true)? cmd.getOptionValue("fa"):null;
        SparkConf conf = new SparkConf().setAppName("BlastNFilter");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", ">");

        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] st = fs.listStatus(new Path(input));
        ArrayList<String> splitFileList = new ArrayList<>();
        for (int i=0;i<st.length;i++){
            if(!st[i].isDirectory() && st[i].getLen()>1){
                    if(fastaonly!=null){
                        if(st[i].getPath().getName().endsWith(fastaonly)){
                            splitFileList.add(st[i].getPath().toUri().getRawPath().toString());
                            System.out.println(st[i].getPath().toUri().getRawPath().toString());
                        }
                    }else{
                        splitFileList.add(st[i].getPath().toUri().getRawPath().toString());
                        System.out.println(st[i].getPath().toUri().getRawPath().toString());
                    }
            }
        }

        JavaRDD<String> fastaFilesRDD = sc.parallelize(splitFileList, splitFileList.size());
        Broadcast<String> bcast = sc.broadcast(fs.getUri().toString());
        JavaPairRDD<String, String> outRDD = fastaFilesRDD.mapPartitionsToPair(f -> {
            Process process;
            String fname = f.next();

            DFSClient client = new DFSClient(URI.create(bcast.getValue()), new Configuration());
            DFSInputStream hdfsstream = client.open(fname);
            String blastn_cmd;

//	    Path srcInHdfs = new Path(fname);
//	    Path destInTmp = new Path("file:///tmp/" + srcInHdfs.getName());
//	    fs.copyToLocalFile(false, srcInHdfs, destInTmp);

//                blastn_cmd = "cat /tmp/" + srcInHdfs.getName() + " | blastn -db " + db + " -num_threads "+num_threads+" -task megablast -word_size " + word_size + " -max_target_seqs " + max_target_seqs + " -evalue " + evalue + " " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt " + outfmt;
//            else
//                blastn_cmd = "cat /tmp/" + srcInHdfs.getName() + " | blastn -db " + db + " -num_threads "+num_threads+" -word_size " + word_size + " -gapopen " + gapopen + " -gapextend " + gapextend + " -penalty " + penalty + " -reward " + reward + " -max_target_seqs " + max_target_seqs + " -evalue " + evalue + " " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt " + outfmt;

            if (task.equalsIgnoreCase("megablast"))
                blastn_cmd = bin+" -db " + db + " -num_threads "+num_threads+" -task megablast -word_size " + word_size + " -max_target_seqs " + max_target_seqs + " -evalue " + evalue + " " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt " + outfmt;
            else
                blastn_cmd = bin+" -db " + db + " -num_threads "+num_threads+" -word_size " + word_size + " -gapopen " + gapopen + " -gapextend " + gapextend + " -penalty " + penalty + " -reward " + reward + " -max_target_seqs " + max_target_seqs + " -evalue " + evalue + " " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt " + outfmt;

            System.out.println(blastn_cmd);

            ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", blastn_cmd);
            process = pb.start();

            BufferedReader hdfsinput = new BufferedReader(new InputStreamReader(hdfsstream));
            BufferedWriter blastinputwriter = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            String l;
            while ((l = hdfsinput.readLine()) != null) {
                System.out.println(l);
                blastinputwriter.write(l);
                blastinputwriter.newLine();
            }
            blastinputwriter.close();

            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String bline;
            ArrayList<Tuple2<String, String>> out = new ArrayList<>();
            while ((bline = in.readLine()) != null) {
                String[] s = bline.trim().split("\t");
                out.add(new Tuple2(s[0], bline));
            }

            /*
            BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String e;
            while ((e = err.readLine()) != null) {
                out.add(e);
            }
            */
            in.close();
            return out.iterator();
        });

        JavaRDD<String> srdd;
        if(fastaonly!=null)
            srdd = sc.textFile(input+"/*."+fastaonly);
        else
            srdd = sc.textFile(input); //take whole directory as input

        JavaPairRDD<String, String> fastaRDD = srdd.mapToPair(fasta -> {
            String[] fseq = fasta.trim().split("\n");
            String seq_id = fseq[0].split(" ")[0];
            String seq = Arrays.toString(Arrays.copyOfRange(fseq, 1, fseq.length)).replace(", ","").replace("[","").replace("]","");

            return new Tuple2<String, String>(seq_id.replace(">",""), seq);

        });

        JavaPairRDD<String, Tuple2<String, Optional<String>>> filtered = fastaRDD.leftOuterJoin(outRDD).filter(record -> {

            try{
                if (record._2()._2().isPresent()) {
                    String bmatch = record._2()._2().get();
                    if(!bmatch.isEmpty()) {
                        //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore
                        String[] bs = bmatch.split("\t");

                        //Select sequences with more that 70% identity over 70% overlap
                        double overlap = ((Integer.valueOf(bs[7]) - Integer.valueOf(bs[6]) + 1) / record._2()._1().length()) * 100;
                        //keep the fasta sequence if overlap and pident values are over threshold
                        return (overlap > threshold && Double.valueOf(bs[2]) > threshold);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            //no blast match, we keep the fasta sequence
            return true;
        });

        filtered.map(z-> ">"+z._1+"\n"+z._2()._1()).saveAsTextFile(output);
        sc.stop();
    }
}
