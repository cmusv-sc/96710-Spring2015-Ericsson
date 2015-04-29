package edu.cmu.sep.FeatureGenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Jacob on 3/9/15.
 */
public class OneFeatureGenerator {

  private static final int NUM_FEATURES = 5;
  
  public static void main(String[] argv) throws Exception {
//    if (argv.length != 3) {
//      System.out.println("Usage: java -classpath FeatureGenerator.jar OneFeatureGenerator [task_usage] [2] [3]");
//      return;
//    }

      
      /*
    String inputData = "inputData/";
    String config = "src/main/resources/config/feature_construction.ini";
    
    if (argv.length > 0) {
        inputData = argv[0];
    }
    
    if (argv.length > 1) {
        config = argv[1];
    }
    
    File inputDataFile = new File(inputData);
    
    if (!inputDataFile.exists()) {
        System.out.println("InputFile directory does not exist.");
        System.exit(5);
    }
    
    File configFile = new File(config);
    
    if (!configFile.exists()) {
        System.out.println("feature configuration file does not exist");
        System.exit(5);
    }
     */ 
    //String output = argv[1];

    //String inputFilePath = setWorkingInputFile(argv[0]);
    //String inputFilePath = setWorkingInputFile("task_usage");
    // processGoogleDatasetFiles();
      
      
      int[] fileCountArray = {1, 2, 5, 10, 20, 50, 100, 200, 500};
      
      for(int fileCount : fileCountArray) {
          
          recordTimeStamp("(Start) Number of Files: " + Integer.toString(fileCount));
          FeatureConstructorSingleton.getInstance().Initialize(fileCount);

          // Create job fail feature
          JobEventsFeature jobEventsFeature = new JobEventsFeature();
          jobEventsFeature.generateFeatureAllRows();

          //Create job duration feature
          TaskUsageFeature taskUsageFeature = new TaskUsageFeature();
          taskUsageFeature.generateFeatureAllRows();

          //CombinedFeature combinedFeature = new CombinedFeature();
          //combinedFeature.generateFeatureAllRows();
          //recordTimeStamp("(End) Number of Files: " + Integer.toString(fileCount));
      }
      

//      JobDurationFeature jobDurationFeature = new JobDurationFeature();
//      jobDurationFeature.generateFeatureAllRows();

    // Create job fail feature
//      JobFailFeature jobFailFeature = new JobFailFeature();
//      jobFailFeature.generateFeatureAllRows();

//    CpuRateFeatures cpuRateFeatures = new CpuRateFeatures();
//    cpuRateFeatures.generateFeatureAllRows();
//
//    CanonicalMemUsage canonicalMemUsage = new CanonicalMemUsage();
//    canonicalMemUsage.generateFeatureAllRows();
//
//    AssignedMemUsage assignedMemUsage = new AssignedMemUsage();
//    assignedMemUsage.generateFeatureAllRows();
//
//    UnmappedPageCache unmappedPageCache = new UnmappedPageCache();
//    unmappedPageCache.generateFeatureAllRows();
//
//    TotalPageCache totalPageCache = new TotalPageCache();
//    totalPageCache.generateFeatureAllRows();
//
//    MaxMemUsage maxMemUsage = new MaxMemUsage();
//    maxMemUsage.generateFeatureAllRows();
//
//    DiskIOTime diskIOTime = new DiskIOTime();
//    diskIOTime.generateFeatureAllRows();
//
//    LocalDiskSpaceUsage localDiskSpaceUsage = new LocalDiskSpaceUsage();
//    localDiskSpaceUsage.generateFeatureAllRows();
//
//    MaxCPURate maxCPURate = new MaxCPURate();
//    maxCPURate.generateFeatureAllRows();
//
//    MaxDiskIOTime maxDiskIOTime = new MaxDiskIOTime();
//    maxDiskIOTime.generateFeatureAllRows();
    
  }

  
    private static void recordTimeStamp(String text) {
        BufferedWriter writer = null;
        try {
            //create a temporary file
            String timeLog = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            File logFile = new File(timeLog);

            // This will output the full path where the file will be written to...
            System.out.println(logFile.getCanonicalPath());

            writer = new BufferedWriter(new FileWriter(logFile));
            writer.write(timeLog + "\n");
            writer.write(text + "\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            }
        }
    }
  
  private static void processGoogleDatasetFiles() throws Exception {
      File inputDir = new File("inputData/");
      if (!inputDir.exists()) {
        System.out.println("InputFile directory does not exist.");
        return;
      }
      
      File topLevelFiles[] = inputDir.listFiles();
      ArrayList tableArrayList = new ArrayList<File>();
      for (int i = 0; i < topLevelFiles.length; i++) {
          if (topLevelFiles[i].isDirectory()) {
              tableArrayList.add(topLevelFiles[i]);
          }
      }
 
      File[] tableList = new File[tableArrayList.size()];
      
      for (int i = 0; i < tableArrayList.size(); i++) {
          tableList[i] = (File)tableArrayList.get(i);
      }

      tableArrayList = null;
      
      // Unzip all files
      for (int i = 0; i < tableList.length; i++) {
        File tableFiles[] = tableList[i].listFiles();
        for (int j = 0; j < tableFiles.length; j++) {
            if (tableFiles[j].isFile() && tableFiles[j].getName().endsWith("gz")) {
                GzipFile gzipFile = new GzipFile(tableFiles[j].getAbsolutePath());
                gzipFile.gunZipFile(); 
                tableFiles[j].delete();
            }
        }
      }
      
      File[] job_events_files = tableList[0].listFiles();
      File[] machine_attributes_files = tableList[1].listFiles();
      File[] machine_events_files = tableList[2].listFiles();
      File[] task_constraints_files = tableList[3].listFiles();
      File[] task_events_files = tableList[4].listFiles();
      File[] task_usage_files = tableList[5].listFiles();
      
      processGoogleDatasetJobs(job_events_files, machine_attributes_files, machine_events_files, task_constraints_files, task_events_files, task_usage_files);
      
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
            if (fileList[i].getName().contains("task_usage")) {
              //processGoogleDatasetUsageFile(flatFile);
            }
            if (fileList[i].getName().contains("job_events")) {
              //processGoogleDatasetUsageFile(flatFile);
              processGoogleDatasetJobEventsFile(flatFile);
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
  
  private static void processGoogleDatasetJobs(File[] job_events_files, File[] machine_attributes_files, File[] machine_events_files, File[] task_constraints_files, File[] task_events_files, File[] task_usage_files) throws Exception {
    
      int fileCount = job_events_files.length;
      
      FlatFileReader job_events_reader;
      FlatFileReader machine_attributes_reader;
      FlatFileReader machine_events_reader;
      FlatFileReader task_constraints_reader;
      FlatFileReader task_events_reader;
      FlatFileReader task_usage_reader;
      
      for (int i = 0; i < fileCount; i++) {
          job_events_reader = new FlatFileReader(job_events_files[i].getAbsolutePath(), ',');
          //machine_attributes_reader = new FlatFileReader(machine_attributes_files[i].getAbsolutePath(), ',');
          //machine_events_reader = new FlatFileReader(machine_events_files[i].getAbsolutePath(), ',');
          //task_constraints_reader = new FlatFileReader(task_constraints_files[i].getAbsolutePath(), ',');
          task_events_reader = new FlatFileReader(task_events_files[i].getAbsolutePath(), ',');
          task_usage_reader = new FlatFileReader(task_usage_files[i].getAbsolutePath(), ',');      
      

            try {
              File outputDir = new File("outputData/job_features");
              if (!outputDir.exists()) {
                outputDir.mkdirs();
              }

              String output = outputDir.getAbsolutePath() + "/job_features.csv";
              FileOutputStream out = new FileOutputStream(output);
              Map<String, Object[]> keyMap = new LinkedHashMap<String, Object[]>();

              String[] job_events_fields = job_events_reader.readRecord();
              //String[] machine_attributes_fields = machine_attributes_reader.readRecord();
              //String[] machine_events_fields = machine_events_reader.readRecord();
              //String[] task_constraints_fields = task_constraints_reader.readRecord();
              String[] task_events_fields = task_events_reader.readRecord();
              String[] task_usage_fields = task_usage_reader.readRecord();
              
              // Generate Features for Task Usage table
              while (task_usage_fields != null) {
                // We assume jobId is a required field
                if (task_usage_fields[3] == null)
                  break;

                generateTaskUsageFeature(task_usage_fields, keyMap);
                task_usage_fields = task_usage_reader.readRecord();
              }
              
              // Generate Features for Job Events table
              while (job_events_fields != null) {
                // We assume jobId is a required field
                if (job_events_fields[3] == null)
                  break;

                generateJobEventFeature(job_events_fields, keyMap);
                job_events_fields = job_events_reader.readRecord();
              }
              
              
              // Generate Features for Task Events table
              while (task_events_fields != null) {
                // We assume jobId is a required field
                if (task_events_fields[3] == null)
                  break;

                generateTaskEventFeature(task_events_fields, keyMap);
                task_events_fields = task_events_reader.readRecord();
              }
              generateOutputFile(out, keyMap);
              out.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
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
  
  private static void generateTaskUsageFeature(String[] fields,  Map<String, Object[]> keyMap) {
    String startTime = fields[0], endTime = fields[1], jobId = fields[2], taskIndex = fields[3], machineId = fields[4],
        meanCPUusageRate = fields[5], canonicalMemUsage = fields[6], assignedMemUsage = fields[7], unmapPageCacheMemUsage = fields[8], totalPageCacheMemUsage = fields[9],
        maxMemUsage = fields[10], meanDiskIoTime = fields[11], meanLocalDiskSpaceUsed = fields[12], maxCPUUsage = fields[13], MaxDiskIoTime = fields[14], cyclesPerInst = fields[15],
        memAccessPerInst = fields[16], samplePortion = fields[17], aggregationType = fields[18], sampleCpuUsage = fields[19];

    Object[] values = keyMap.get(jobId);
    Long jobDuration;
    if (values == null) {
        values = new Object[NUM_FEATURES];
        jobDuration = Long.parseLong(endTime) - Long.parseLong(startTime);
        values[0] = jobDuration;
        keyMap.put(jobId, values);
    } else if (values[0] == null) {
        values[0] = Long.parseLong(endTime) - Long.parseLong(startTime);
        keyMap.put(jobId, values);
    } else {
        jobDuration = (Long)values[0] + Long.parseLong(endTime) - Long.parseLong(startTime);
        values[0] = jobDuration;
        keyMap.put(jobId, values);
    }
  }
  
  private static void generateJobEventFeature(String[] fields,  Map<String, Object[]> keyMap) {
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
    
    Object[] values = keyMap.get(jobId);
    
    if (values == null) {
        values = new Object[NUM_FEATURES];
        values[1] = jobStatus;
        keyMap.put(jobId, values);
    } else {
        values[1] = jobStatus;
        keyMap.put(jobId, values);  
    }    
  }
  
    private static void generateTaskEventFeature(String[] fields,  Map<String, Object[]> keyMap) {
    String time = fields[0], missingInfo = fields[1], jobId = fields[2], 
            taskIndex = fields[3], machineId = fields[4], eventType = fields[5],
            user = fields[6], schedulingClass = fields[7], priority = fields[8],
            cpuRequest = fields[9], memoryRequest = fields[10],
            diskSpaceRequest = fields[11], 
            differentMachinesRestriction = fields[12];
    
    final int TASK_FAIL_CODE = 3;
    final int TASK_EVICT_CODE = 2;
    final int TASK_SCHEDULE_CODE = 1;
    boolean taskFailStatus = false;
    Object[] values = keyMap.get(jobId);
    
    if (Integer.parseInt(eventType) == TASK_FAIL_CODE) {
        taskFailStatus = true;
    }
    
    if (values == null) {
        values = new Object[NUM_FEATURES];
        values[2] = 0;
        values[3] = taskFailStatus;
        if(Integer.parseInt(eventType) == TASK_EVICT_CODE) {
            values[4] = 1;
        }
        else {
            values[4] = 0;
        }
        keyMap.put(jobId, values);
    } else if (values[2] == null) {
        values[2] = 0;
        values[3] = taskFailStatus;
        if(Integer.parseInt(eventType) == TASK_EVICT_CODE) {
            values[4] = 1;
        }
        else {
            values[4] = 0;
        }
        keyMap.put(jobId, values);
    } else {
        
        if ((Boolean)values[3] == true) {
            if (Integer.parseInt(eventType) == TASK_SCHEDULE_CODE) {
                int numCrashLoops = (Integer)values[2];
                numCrashLoops++;
                values[2] = numCrashLoops;
                values[3] = false;
            }
        }
        else {
            if (Integer.parseInt(eventType) == TASK_FAIL_CODE) {
                values[3] = true;
            }
        }
        if(Integer.parseInt(eventType) == TASK_EVICT_CODE) {
            values[4] = (Integer)values[4] + 1;
        }
        keyMap.put(jobId, values);  
    }    
  }
  
  private static void generateOutputFile (FileOutputStream out,  Map keyMap) {

    String dilimit = ", ";
    String endLine = "\n";
    Iterator mapIt = keyMap.entrySet().iterator();

    try {
      while (mapIt.hasNext()) {
        Map.Entry item = (Map.Entry) mapIt.next();
        Object[] value = (Object[])item.getValue();
        String entry = (String)item.getKey();
        
        for (int i = 0; i < value.length; i++) {
            String valueString = "";
            if (value[i] != null) {
                valueString = value[i].toString();
            }
            entry += dilimit + valueString;
        }
        entry += endLine;
        
        out.write(entry.getBytes());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }    
  }
  
  private static int getMinValue(int[] array) {
      int minValue = array[0];
      for (int i = 1; i < array.length; i++) {
          if (array[i] < minValue) {
              minValue = array[i];
          }
      }
      return minValue;
  }
}
