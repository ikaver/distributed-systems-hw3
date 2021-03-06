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
  private static final String BUNDLE_PATH = "bundle-path";
  private static final String MAP_CLASS_NAME = "map-class";
  private static final String REDUCE_CLASS_NAME = "reduce-class";
  private static final String INPUT_FILE_PATH = "input-file-path";
  private static final String OUTPUT_FILE_PATH = "output-file-path";
  private static final String NUM_REDUCERS = "num-reducers";
  private static final String RECORD_SIZE = "record-size";

  public static JobConfig createJobFromJSONFile(File jsonFile) throws IOException, UnsupportedEncodingException {
    FileInputStream fis = new FileInputStream(jsonFile);
    byte[] data = new byte[(int)jsonFile.length()];
    fis.read(data);
    fis.close();
    String json = new String(data, "UTF-8");
    return createJobFromJSON(json);
  }
  
  public static JobConfig createJobFromJSON(String json) throws FileNotFoundException, IOException {
    if(json == null) return null;
    JSONObject obj = new JSONObject(json);
    JSONObject config = obj.getJSONObject(CONFIG_OBJECT);
    String jobName = config.getString(JOB_NAME);
    String mapperClass = config.getString(MAP_CLASS_NAME);
    String reducerClass = config.getString(REDUCE_CLASS_NAME);
    String bundlePath = config.getString(BUNDLE_PATH);
    String inputFilePath = config.getString(INPUT_FILE_PATH);
    String outputFilePath = config.getString(OUTPUT_FILE_PATH);
    int numReducers = config.getInt(NUM_REDUCERS);
    int recordSize = config.getInt(RECORD_SIZE);
    
    File jarFile = new File(bundlePath);
    FileInputStream fis = new FileInputStream(jarFile);
    byte[] data = new byte[(int)jarFile.length()];
    fis.read(data);
    fis.close();
        
    JobConfig job = new JobConfig();
        
    job.setJobName(jobName);
    job.setJarFile(data);
    job.setMapperClass(mapperClass);
    job.setReducerClass(reducerClass);
    job.setInputFilePath(inputFilePath);
    job.setOutputFilePath(outputFilePath);
    job.setNumReducers(numReducers);
    job.setRecordSize(recordSize);
    
    return job;
  }
}
