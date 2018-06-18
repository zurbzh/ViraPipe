package org.ngseq.metagenomics;

import breeze.linalg.rank;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.util.Arrays;

/**
 * Usage
 spark-submit  --master local[40] --driver-memory 4g --executor-memory 4g --class org.ngseq.metagenomics.SQLQueryBlast target/metagenomics-0.9-jar-with-dependencies.jar -in normrdd.fq -out blast_result -query "SELECT * from records ORDER BY sseqid ASC"

 **/


public class SQLQueryBlast {

    private static String blasttable = "records";
    private static String taxatable = "taxa";

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("SQLQueryBlast");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //String query = args[2];


        Options options = new Options();
        options.addOption(new Option( "temp", "Temporary output"));
        options.addOption(new Option( "out", true, "" ));
        options.addOption(new Option( "in", true, "" ));
        options.addOption(new Option( "partitions", true, "Number of partitions" ));

        options.addOption(new Option( "word_size", ""));
        options.addOption(new Option( "gapopen", true, "" ));
        options.addOption(new Option( "gapextend", true, "" ));
        options.addOption(new Option( "penalty", true, "" ));
        options.addOption(new Option( "reward", true, "" ));
        options.addOption(new Option( "max_target_seqs", true, "" ));
        options.addOption(new Option( "evalue", true, "" ));
        options.addOption(new Option( "show_gis", "" ));
        options.addOption(new Option( "outfmt", true, "" ));
        options.addOption(new Option( "db", true, "" ));
        options.addOption(new Option( "format", true, "parquet or fastq" ));
        options.addOption(new Option( "lines", true, "" ));

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

        String input = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
        String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
        String format = (cmd.hasOption("format")==true)? cmd.getOptionValue("format"):"fastq";
        int lines = (cmd.hasOption("lines")==true)? Integer.valueOf(cmd.getOptionValue("lines")):100;

        JavaRDD<String> blastrdd = sc.textFile(input);
        JavaRDD<String> taxardd = sc.textFile("hdfs:///Projects/indexes/Resources/taxonomy/acc_taxid_gi_species.txt");

        JavaRDD<BlastRecord> blastRDD = blastrdd.map(f -> {

            //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore
            String[] fields = f.split("\\t");

            BlastRecord record = new BlastRecord();
            record.setQseqid(fields[0]!=null?fields[0]:null);
            String[] taxfields = fields[1].split("\\|");
            System.out.println(Arrays.toString(taxfields));
            String[] accfields = taxfields[3].split("\\.");
            record.setAcc(accfields[0]!=null?accfields[0]:null);
            record.setPident(fields[2]!=null?Double.valueOf(fields[2]):null);
            record.setLength(fields[3]!=null?Integer.valueOf(fields[3]):null);
            record.setMismatch(fields[4]!=null?Integer.valueOf(fields[4]):null);
            record.setGapopen(fields[5]!=null?Integer.valueOf(fields[5]):null);
            record.setQstart(fields[6]!=null?Long.valueOf(fields[6]):null);
            record.setQend(fields[7]!=null?Long.valueOf(fields[7]):null);
            record.setSstart(fields[8]!=null?Long.valueOf(fields[8]):null);
            record.setSend(fields[9]!=null?Long.valueOf(fields[9]):null);
            record.setEvalue(fields[10]!=null?Double.valueOf(fields[10]):null);
            record.setBitscore(fields[11]!=null?Double.valueOf(fields[11]):null);
            record.setQlen(fields[12]!=null?Long.valueOf(fields[12]):null);
            record.setSlen(fields[13]!=null?Long.valueOf(fields[13]):null);

            System.out.println(f);

            return record;
        });

        JavaRDD<TaxonomyRecord> taxaRDD = taxardd.map(f -> {

            String[] fields = f.split("\\t");
            TaxonomyRecord record = new TaxonomyRecord();
            record.setAcc(fields[0]!=null?fields[0]:null);
            record.setAcc1(fields[1]!=null?fields[1]:null);
            record.setTaxid(fields[2]!=null?fields[2]:null);
            record.setGi(fields[3]!=null?fields[3]:null);
            record.setSpecies(fields[4]!=null?fields[4]:null);

            System.out.println(f);

            return record;
        });



        Dataset blastDF = sqlContext.createDataFrame(blastRDD, BlastRecord.class);
        blastDF.registerTempTable(blasttable);

        Dataset taxaDF = sqlContext.createDataFrame(taxaRDD, TaxonomyRecord.class);
        taxaDF.registerTempTable(taxatable);
        //eq. count duplicates "SELECT count(DISTINCT(sequence)) FROM reads"
        //"SELECT key,LEN(sequence) as l FROM reads where l<100;"


        String coverage = "SELECT *, Round((((qend - qstart)+1)/qlen)*100, 2) as coverage from records where Round((((qend - qstart)+1)/qlen)*100, 2) > 70 AND pident > 90";
        Dataset<Row> parsedDF = sqlContext.sql(coverage);


        Dataset<Row> parsedDFSPecies = parsedDF.join(taxaDF, parsedDF.col("acc").equalTo(taxaDF.col("acc")));
        //resultDF.show(lines, false);


        WindowSpec windowSpecPrev = Window.partitionBy(parsedDFSPecies.col("qseqid")).orderBy("coverage", "pident");
        Dataset<Row> resultDF = parsedDFSPecies.withColumn("order", functions.row_number().over(windowSpecPrev).equalTo(1));


        if(outDir!=null){
            JavaRDD<String> resultRDD = dfToTabDelimited(resultDF).coalesce(1);
            resultRDD.saveAsTextFile(outDir);
        }
        sc.stop();

    }

    private static JavaRDD<String> dfToTabDelimited(Dataset<Row> df) {
        return df.toJavaRDD().map(row ->  {
            //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore

            String output = row.getAs("qseqid")+"\t"+row.getAs("acc")+"\t"+row.getAs("pident")+"\t"
                    +row.getAs("qlen")+"\t"+row.getAs("coverage")+"\t"+row.getAs("evalue")+"\t"
                    +row.getAs("bitscore")+"\t"+row.getAs("species");

            return output;
        });
    }



}