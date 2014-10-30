package com.ikaver.aagarwal.hw3.common.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.json.JSONObject;

public class JobFromJSONCreator {
  
  private static final String CONFIG_OBJECT = "config";
  private static final String JOB_NAME = "job-name";
  private static final String MASTER_IP = "master-ip";
  private static final String MASTER_PORT = "master-port";
  private static final String BUNDLE_PATH = "bundle-path";
  private static final String INPUT_FILE_PATH = "input-file-path";
  private static final String OUTPUT_FILE_PATH = "output-file-path";
  private static final String NUM_MAPPERS = "num-mappers";
  private static final String NUM_REDUCERS = "num-reducers";
  private static final String RECORD_SIZE = "record-size";

  public static Job createJobFromJSONFile(File jsonFile) throws IOException, UnsupportedEncodingException {
    FileInputStream fis = new FileInputStream(jsonFile);
    byte[] data = new byte[(int)jsonFile.length()];
    fis.read(data);
    fis.close();
    String json = new String(data, "UTF-8");
    return createJobFromJSON(json);
  }
  
  public static Job createJobFromJSON(String json) throws FileNotFoundException, IOException {
    if(json == null) return null;
    JSONObject obj = new JSONObject(json);
    JSONObject config = obj.getJSONObject(CONFIG_OBJECT);
    String jobName = config.getString(JOB_NAME);
    String masterIP = config.getString(MASTER_IP);
    int masterPort = config.getInt(MASTER_PORT);
    String bundlePath = config.getString(BUNDLE_PATH);
    String inputFilePath = config.getString(INPUT_FILE_PATH);
    String outputFilePath = config.getString(OUTPUT_FILE_PATH);
    int numMappers = config.getInt(NUM_MAPPERS);
    int numReducers = config.getInt(NUM_REDUCERS);
    int recordSize = config.getInt(RECORD_SIZE);
    
    Job job = new Job();
    
    File bundleFile = new File(bundlePath);
    FileInputStream fis = new FileInputStream(bundlePath);
    byte[] data = new byte[(int)bundleFile.length()];
    fis.read(data);
    fis.close();
    
    //TODO: how to get combiner/reducer/mapper class from jar file?
    
    job.setJobName(jobName);
    job.setMasterIP(masterIP);
    job.setMasterPort(masterPort);
    job.setJarFile(data);
    job.setInputFilePath(inputFilePath);
    job.setOutputFilePath(outputFilePath);
    job.setNumMappers(numMappers);
    job.setNumReducers(numReducers);
    job.setRecordSize(recordSize);
    
    
    return job;
  }
}
