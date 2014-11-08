package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.ikaver.aagarwal.hw3.common.master.IJobManager;
import com.ikaver.aagarwal.hw3.common.util.SocketAddress;

/**
 * Interface implemented by entities that know how to find a JobManager given
 * a Job.
 */
public interface IJobManagerFactory {

  public IJobManager getJobManager(SocketAddress addr);
  
}
