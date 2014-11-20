package com.ikaver.aagarwal.hw3.mrmaster.jobmanager.jobvalidator;

import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.ikaver.aagarwal.hw3.common.config.JobConfig;
import com.ikaver.aagarwal.hw3.common.dfs.IDFS;

public class JobValidatorImpl implements IJobValidator {
  
  private IDFS dfs;
  private static final Logger LOG = Logger.getLogger(JobValidatorImpl.class);
  
  @Inject
  public JobValidatorImpl(IDFS dfs) {
    this.dfs = dfs;
  }
  

  public boolean isJobValid(JobConfig job) {
    boolean validJob = false;
    try {
      validJob = job != null 
          && dfs.containsFile(job.getInputFilePath())
          && job.getJarFile() != null
          && job.getJarFile().length > 0
          && job.getNumReducers() > 0
          && job.getOutputFilePath() != null
          && job.getMapperClass() != null
          && job.getReducerClass() != null;
    } catch (RemoteException e) {
      LOG.warn("Remote exception calling dfs", e);
    }
    LOG.info(job.getJobName() + " isvalid ?" + validJob);
    return validJob;
  }

}
