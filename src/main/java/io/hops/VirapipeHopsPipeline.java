/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops;

import java.util.logging.Logger;

public class VirapipeHopsPipeline {

  private static final Logger LOG = Logger.getLogger(VirapipeHopsPipeline.class.getName());

  public static void main(String[] args) {

//    SparkSession spark = SparkSession
//        .builder()
//        .appName(HopsUtil.getJobName())
//        .getOrCreate();

//    try {
//
//      String[] jobsNames = {"DecompressInterleave", "AlignInterleavedMulti", "NormalizeRDD",
//        "FastqGroupper", "Assemble", "RenameContigsUniq", "BlastNFilter", "BlastN", "HMMSearch"};
//      //Start job with ID: 6145 that prepares data for job with id 6146
//
//      for (String jobName : jobsNames) {
//        Integer jobId = Integer.parseInt(jobName);
//        Response r = HopsUtil.startJobs(jobId);
//        if (r.getStatus() == Response.Status.OK.getStatusCode()) {
//          if (HopsUtil.waitForJobs(jobId, 10000l, TimeUnit.SECONDS)) {
//            LOG.severe("Problem executing job: " + jobName);
//            break;
//          } else {
//            LOG.info("Successfully executed job: " + jobName);
//
//          }
//        }
//      }
//
//    } catch (CredentialsNotFoundException ex) {
//      Logger.getLogger(VirapipeHopsPipeline.class.getName()).log(Level.SEVERE, null, ex);
//    } finally {
//      //Stop spark session
//      spark.stop();
//    }
  }

}
