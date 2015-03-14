package edu.cmu.sep.FeatureGenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;


/**
 * Created by Jacob on 3/9/15.
 */
public class OneFeatureGenerator {

  public static void main(String[] argv) throws Exception {
//    if (argv.length != 3) {
//      System.out.println("Usage: java -classpath DatabasePatch.jar OneFeatureGenerator [task_usage] [2] [3]");
//      return;
//    }

    //String output = argv[1];

    //String inputFilePath = setWorkingInputFile(argv[0]);
    String inputFilePath = setWorkingInputFile("task_usage");
    processGoogleDatasetFiles(inputFilePath);
  }

  private static String setWorkingInputFile(String inputFile) {
    return "inputData/" + inputFile;
  }

  private static void processGoogleDatasetFiles(String inputFilePath) throws Exception {
    try {
      File inputDir = new File(inputFilePath);
      if (!inputDir.exists()) {
        System.out.println("InputFile directory does not exist.");
        return;
      }
      File fileList[] = inputDir.listFiles();
      for (int i = 0; i < fileList.length; i++) {
        if (fileList[i].isFile() && fileList[i].getName().endsWith("gz")) {
          GzipFile gzipFile = new GzipFile(inputFilePath + "/" + fileList[i].getName());
          String flatFile = gzipFile.gunZipFile();
          if (fileList[i].canRead()) {
            if (fileList[i].getName().contains("usages")) {
              processGoogleDatasetUsageFile(flatFile);
              //processGoogleDatasetJobEventsFile(flatFile);
            }
          }

          else
            System.out.println("Process Google Dataset file: " + fileList[i].getName() +
                " failed; Cannot Read.");
        }
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  private static void processGoogleDatasetUsageFile(String file) throws Exception {
    FlatFileReader reader = new FlatFileReader(file, ',');

    try {
      File outputDir = new File("outputData/task_usage");
      if (!outputDir.exists()) {
        outputDir.mkdirs();
      }

      String output = file.replaceFirst("inputData", "outputData");
      FileOutputStream out = new FileOutputStream(output);
      Map<String, Long> keyMap = new HashMap<String, Long>();

      String[] fields = reader.readRecord();
      while (fields != null) {
        // We assume jobId is a required field
        if (fields[3] == null)
          break;

        generateNewIdenticalFieldForUsage(fields, keyMap);
        fields = reader.readRecord();
      }
      generateOutputFile(out, keyMap);
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private static void processGoogleDatasetJobEventsFile(String file) throws Exception {
    FlatFileReader reader = new FlatFileReader(file, ',');

    try {
      File outputDir = new File("outputData/task_events");
      if (!outputDir.exists()) {
        outputDir.mkdirs();
      }

      String output = file.replaceFirst("inputData", "outputData");
      FileOutputStream out = new FileOutputStream(output);
      Map<String, String> keyMap = new HashMap<String, String>();

      String[] fields = reader.readRecord();
      while (fields != null) {
        // We assume jobId is a required field
        if (fields[3] == null)
          break;

        generateNewIdenticalFieldForJobEvent(fields, keyMap);
        fields = reader.readRecord();
      }
      generateOutputFile(out, keyMap);
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void generateNewIdenticalFieldForUsage(String[] fields,  Map<String, Long> keyMap) {
    String startTime = fields[0], endTime = fields[1], jobId = fields[2], taskIndex = fields[3], machineId = fields[4],
        meanCPUusageRate = fields[5], canonicalMemUsage = fields[6], assignedMemUsage = fields[7], unmapPageCacheMemUsage = fields[8], totalPageCacheMemUsage = fields[9],
        maxMemUsage = fields[10], meanDiskIoTime = fields[11], meanLocalDiskSpaceUsed = fields[12], maxCPUUsage = fields[13], MaxDiskIoTime = fields[14], cyclesPerInst = fields[15],
        memAccessPerInst = fields[16], samplePortion = fields[17], aggregationType = fields[18], sampleCpuUsage = fields[19];

    Long jobDuration = keyMap.get(jobId);
    if (jobDuration == null) {
      keyMap.put(jobId, Long.parseLong(endTime) - Long.parseLong(startTime));
    } else {
      jobDuration = jobDuration + Long.parseLong(endTime) - Long.parseLong(startTime);
      keyMap.put(jobId,jobDuration);
    }

    System.out.println("Check!");
  }
  
  private static void generateNewIdenticalFieldForJobEvent(String[] fields,  Map<String, String> keyMap) {
    String time = fields[0], missingInfo = fields[1], jobId = fields[2], 
            eventType = fields[3], user = fields[4], 
            schedulingClass = fields[5], jobName = fields[6], 
            logicalJobName = fields[7];
  
    
    final int JOB_FAIL_CODE = 3;
    String jobStatus;
    if (Integer.parseInt(eventType) == JOB_FAIL_CODE) {
        jobStatus = "FAIL";
    }
    else {
        jobStatus = "NO_FAIL";
    }
    
    keyMap.put(jobId, jobStatus);
  }

  private static void generateOutputFile (FileOutputStream out,  Map keyMap) {

    String dilimit = ", ";
    String endLine = "\n";
    Iterator mapIt = keyMap.entrySet().iterator();

    try {
      while (mapIt.hasNext()) {
        Map.Entry item = (Map.Entry) mapIt.next();
        String entry = item.getKey() + dilimit + item.getValue().toString() + endLine;
        out.write(entry.getBytes());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
