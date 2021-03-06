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
import java.lang.Thread;

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
    private String mDatasetRoot;
    private String mOutputFile;
    private int mFileCount;
    
    private FeatureConstructorSingleton() {}
    
    public static FeatureConstructorSingleton getInstance() {
        if (mInstance == null) {
            mInstance = new FeatureConstructorSingleton();
        }
        return mInstance;
    }
    
    public void Initialize(String datasetRoot, String outputFile, int fileCount) {

        mDatasetRoot = datasetRoot;
        mOutputFile = outputFile;
        mFileCount = fileCount;

        generateTableList();
        generateSchemaHash();
        generateFileListHash(mFileCount);
        generateJobHash();
        initializeOutputFile();
        
        addFeatureToSchema("job ID");
    }
    
    private String getSchemaFile() {
        return mDatasetRoot + "/schema.csv";
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

    private ArrayList<String> generateFileList(String table, int fileCount) {
        
        ArrayList<String> tableList = new ArrayList<String>();
        File tableFolder = new File(mDatasetRoot + "/" + table);

        if (!tableFolder.exists()) {
            System.out.println("InputFile file does not exist.");
            return tableList;
        }
        int numTableFiles = tableFolder.listFiles().length;
        System.out.println(table + ": " + numTableFiles);
        for(int i = 0; i < fileCount && i < numTableFiles; i++) {
            File tableFile = new File(mDatasetRoot + "/" + table
                    + String.format("/part-%05d-of-%05d.csv.gz", i, numTableFiles));
            if(tableFile.isFile()) {
                tableList.add(tableFile.getAbsolutePath());
            }
            else {
                System.out.println(tableFile.getAbsolutePath() + " is not a file");
            }
        }

        return tableList;
    }
    
    private void generateFileListHash(int fileCount) {
        mFileListHash = new HashMap<String, ArrayList<String>>();
        for(String table : mTableArray) {
            ArrayList<String> fileList = generateFileList(table, fileCount);
            mFileListHash.put(table, fileList);
        }
    }
    
    public final ArrayList<String> getFileList(String table) {
        if(mFileListHash.containsKey(table))
            return mFileListHash.get(table);
        else {
            ArrayList<String> retVal = new ArrayList<String>();
            retVal.add(mOutputFile);
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
                GzipFile gzipFile = new GzipFile(file);
                String flatFile = gzipFile.gunZipFile();

                FlatFileReader reader = new FlatFileReader(flatFile, ',');
                do {
                    tableRowArray = reader.readRecord();
                    if(tableRowArray[jobIdIndex] == null) break;
                    mJobHash.put(tableRowArray[jobIdIndex], null);
                } while (tableRowArray != null);
                File deleteFile = new File(flatFile);
                Files.deleteIfExists(deleteFile.toPath());
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
        try {
            FileOutputStream outputStream = new FileOutputStream(mOutputFile);
            String endLine = "\n";
            String header = "job_id" + endLine;
            outputStream.write(header.getBytes());
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
    
    public void updateOutputFile(ArrayList<String> featureSchema) {
        File outputFeaturesFile = new File(mOutputFile);
        if(!outputFeaturesFile.isFile()) {
            System.exit(1);
        }
        File tempFile = new File("temp.csv");
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
            String headers = "";
            String[] inputHeaders = inputReader.readRecord();
            for(String header : inputHeaders) {
                if(header == null) break;
                headers += header + ",";
            }
             for(String header : featureSchema) {
                headers +=  header + ",";
            }
            headers += endLine;
            outputStream.write(headers.getBytes());
            for (Map.Entry<String, ArrayList<String>> entry : mJobHash.entrySet()) {
                String lineToWrite = "";
                for(String feature : inputReader.readRecord()) {
                    if(feature == null) break;
                    lineToWrite += feature + ",";
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
                        lineToWrite += value + ",";
                    }
                    lineToWrite += endLine;
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
