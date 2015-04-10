/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 *
 * @author rohit_000
 */
public abstract class Feature {
    
    protected final ArrayList<String> mSchema = FeatureConstructorSingleton.getInstance().getSchema(getTableName());
    protected final ArrayList<String> mFileList = FeatureConstructorSingleton.getInstance().getFileList(getTableName());
    protected final LinkedHashMap<String, ArrayList<String>> mJobHash = FeatureConstructorSingleton.getInstance().getJobHash();
       
    public abstract String getTableName();
    
    public abstract void generateFeatureSingleValue(String[] tableRowArray);
    
    public void generateFeatureAllRows() throws IOException {
        // Add feature to schema
        FeatureConstructorSingleton.getInstance().addFeatureToSchema(this.getClass().getSimpleName());
        
        for(String file : mFileList) {
            FlatFileReader reader = new FlatFileReader(file, ',');
            String[] tableRowArray;
            do {
                tableRowArray = reader.readRecord();
                if(tableRowArray[0] == null) break;
                generateFeatureSingleValue(tableRowArray);
            } while (tableRowArray != null);
        }
        FeatureConstructorSingleton.getInstance().updateOutputFile();
        FeatureConstructorSingleton.getInstance().clearJobHash();
    }
}
