package com.ikaver.aagarwal.hw3.mrclient.joblauncher;

import com.ikaver.aagarwal.hw3.common.config.Job;
import com.ikaver.aagarwal.hw3.common.master.IJobManager;

/**
 * Interface implemented by entities that know how to find a JobManager given
 * a Job.
 */
public interface IJobManagerFactory {

  public IJobManager jobManagerFromJob(Job job);
  
}
