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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.nio.file.Files;
import java.util.Arrays;

/**
 *
 * @author rohit_000
 */
public class FeatureConstructorSingleton {
    
    private static FeatureConstructorSingleton mInstance;
    private String[] mTableArray;
    private HashMap<String, ArrayList<String>> mSchemaHash;
    private HashMap<String, ArrayList<String>> mFileListHash;
    private LinkedHashMap<String, String> mJobHash;
    
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
        generateJobHash();
        
        initializeOutputFile();
    }
    
    private String getDatasetRoot() {
        return "inputData";
    }
    
    private String getOutputDir() {
        return "outputData";
    }
    
    private String getSchemaFile() {
        return getDatasetRoot() + "/schema.csv";
    }
    
    private ArrayList<String> generateSchema(String table) throws IOException {
        
        FlatFileReader schemaReader = new FlatFileReader(getSchemaFile(), ',');
        
        ArrayList<String> schemaList = new ArrayList<String>();
        
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
        return getOutputDir() + "/job_features.csv";
    }
    
    private ArrayList<String> generateFileList(String table) {
        
        ArrayList<String> tableList = new ArrayList<String>();
        File tableFolder = new File(getDatasetRoot() + "/" + table);
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
        return mFileListHash.get(table);
    }
    
    private void generateJobHash() {
        mJobHash = new LinkedHashMap<String, String>();
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
    
    public LinkedHashMap<String, String> getJobHash() {
        return mJobHash;
    }
    
    public void clearJobHash() {
        for (Map.Entry<String, String> entry : mJobHash.entrySet()) {
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
            for (Map.Entry<String, String> entry : mJobHash.entrySet()) {
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
        File tempFile = new File(getOutputDir() + "/temp.csv");
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
            for (Map.Entry<String, String> entry : mJobHash.entrySet()) {
                String lineToWrite = "";
                for(String feature : inputReader.readRecord()) {
                    if(feature == null) break;
                    lineToWrite += feature + ',';
                }
                lineToWrite += entry.getValue() + endLine;
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
}
