/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.nio.file.Files;

/**
 *
 * @author rohit_000
 */
public class FeatureConstructorSingleton {
    
    private static FeatureConstructorSingleton mInstance;
    private String[] mTableArray;
    private HashMap<String, ArrayList<String>> mSchemaHash;
    private HashMap<String, ArrayList<String>> mFileListHash;
    private LinkedHashMap<String, ArrayList<String>> mJobHash;
    private ArrayList<String> mGeneratedFeaturesSchema = new ArrayList<String>();
    
    private FeatureConstructorSingleton() {}
    
    public static FeatureConstructorSingleton getInstance() {
        if (mInstance == null) {
            mInstance = new FeatureConstructorSingleton();
        }
        return mInstance;
    }
    
    public void Initialize() {
        generateTableList();
        generateSchemaHash();
        generateFileListHash();
        generateJobHash(); // Jacob: Not quite understand what is this for?
        initializeOutputFile();
        
        addFeatureToSchema("job ID");
    }
    
    private String getDatasetRoot() {
        return "inputData/";
    }
    
    private String getOutputDir() {
        return "outputData/";
    }
    
    private String getSchemaFile() {
        return getDatasetRoot() + "schema.csv";
    }
    
    private ArrayList<String> generateSchema(String table) throws IOException {
      ArrayList<String> schemaList = new ArrayList<String>();

      String schemaFileName = getSchemaFile();
      File inputDir = new File(schemaFileName);
      if (!inputDir.exists()) {
        System.out.println("InputFile file does not exist.");
        return schemaList;
      }

        FlatFileReader schemaReader = new FlatFileReader(schemaFileName, ',');
        
        String[] schemaFields = schemaReader.readRecord();
        
        while (schemaFields != null) {
            
            if (schemaFields[0] == null) break;
            
            if(schemaFields[0].startsWith(table)) {
                schemaList.add(schemaFields[2]);
            }
            schemaFields = schemaReader.readRecord();
        }
                
        return schemaList;
    }
    
    private void generateSchemaHash() {
        
        mSchemaHash = new HashMap<String, ArrayList<String>>();
        for(String table : mTableArray) {
            try {
                mSchemaHash.put(table, generateSchema(table));
            } catch(IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
    
    private void generateTableList() {
        mTableArray = new String[]{"job_events", "machine_attributes", "machine_events", "task_constraints", "task_events", "task_usage"};
    }
    
    public final ArrayList<String> getSchema(String table) {
        return mSchemaHash.get(table);
    }
    
    public String getOutputFeaturesFile() {
        return getOutputDir() + "job_features.csv";
    }
    
    private ArrayList<String> generateFileList(String table) {
        
        ArrayList<String> tableList = new ArrayList<String>();
        File tableFolder = new File(getDatasetRoot() + table);

      if (!tableFolder.exists()) {
        System.out.println("InputFile file does not exist.");
        return tableList;
      }

        File[] tableFiles = tableFolder.listFiles();

        for (File tableFile : tableFiles) {
            if(tableFile.isFile() && tableFile.getName().endsWith(".csv")) {
                tableList.add(tableFile.getAbsolutePath());
            }

        }
        
        return tableList;
    }
    
    private void generateFileListHash() {
        mFileListHash = new HashMap<String, ArrayList<String>>();
        for(String table : mTableArray) {
            mFileListHash.put(table, generateFileList(table));
        }
    }
    
    public final ArrayList<String> getFileList(String table) {
        if(mFileListHash.containsKey(table))
            return mFileListHash.get(table);
        else {
            ArrayList<String> retVal = new ArrayList<String>();
            retVal.add(getOutputFeaturesFile());
            return retVal;
        }
        
    }
    
    private void generateJobHash() {
        mJobHash = new LinkedHashMap<String, ArrayList<String>>();
        ArrayList<String> fileList = getFileList("job_events");
        ArrayList<String> schema = getSchema("job_events");
        int jobIdIndex = schema.indexOf("job ID");
        String[] tableRowArray;
        for(String file : fileList) {
            try {
                FlatFileReader reader = new FlatFileReader(file, ',');
                do {
                    tableRowArray = reader.readRecord();
                    if(tableRowArray[jobIdIndex] == null) break;
                    mJobHash.put(tableRowArray[jobIdIndex], null);
                } while (tableRowArray != null);
            } catch(IOException e) {
                e.printStackTrace();
                System.exit(1);
            }

        }
    }
    
    public LinkedHashMap<String, ArrayList<String>> getJobHash() {
        return mJobHash;
    }
    
    public void clearJobHash() {
        for (Map.Entry<String, ArrayList<String>> entry : mJobHash.entrySet()) {
            entry.setValue(null);
        }
    }
  
    private void initializeOutputFile () {
        File outputDir = new File(getOutputDir());
        if (!outputDir.exists()) {
          outputDir.mkdirs();
        }
        try {
            FileOutputStream outputStream = new FileOutputStream(getOutputFeaturesFile());
            String endLine = "\n";
            for (Map.Entry<String, ArrayList<String>> entry : mJobHash.entrySet()) {
                String lineToWrite = entry.getKey() + endLine;
                outputStream.write(lineToWrite.getBytes());
            }
            outputStream.close();
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public void updateOutputFile() {
        File outputFeaturesFile = new File(getOutputFeaturesFile());
        if(!outputFeaturesFile.isFile()) {
            System.exit(1);
        }
        File tempFile = new File(getOutputDir() + "temp.csv");
        if(tempFile.isFile()) {
            try {
                Files.delete(tempFile.toPath());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        
        try {
            FileOutputStream outputStream = new FileOutputStream(tempFile);
            FlatFileReader inputReader = new FlatFileReader(outputFeaturesFile.getAbsolutePath(), ',');
            String endLine = "\n";
            for (Map.Entry<String, ArrayList<String>> entry : mJobHash.entrySet()) {
                String lineToWrite = "";
                for(String feature : inputReader.readRecord()) {
                    if(feature == null) break;
                    lineToWrite += feature + ", ";
                }
              ArrayList<String> values = entry.getValue();
              if (values != null) {
                for (int i = 0; i < values.size(); i++) {
                /*
                1. Job Duration
                 2. Job failed
                 3. Max, min, avg CPU rate
                 */
                  String value = values.get(i);
                  if (value.contains("/")) {
                    String[] numbers = value.split("/");

                    Integer num = Integer.parseInt(numbers[1]);
                    Float average = Float.parseFloat(numbers[0])/num;
                    value = Float.toString(average);
                  }
                  lineToWrite += value + ", ";
                }
                lineToWrite = lineToWrite.substring(0, lineToWrite.length() - 2) + endLine;
              } else {
                lineToWrite = lineToWrite + endLine;
              }

                outputStream.write(lineToWrite.getBytes());
            }
            inputReader.close();
            outputStream.close();
            Files.delete(outputFeaturesFile.toPath());
            Files.copy(tempFile.toPath(), outputFeaturesFile.toPath());
            Files.delete(tempFile.toPath());
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public void addFeatureToSchema(String feature) {
        mGeneratedFeaturesSchema.add(feature);
    }
    
    public ArrayList<String> getGeneratedFeaturesSchema() {
        return mGeneratedFeaturesSchema;
    }
}
