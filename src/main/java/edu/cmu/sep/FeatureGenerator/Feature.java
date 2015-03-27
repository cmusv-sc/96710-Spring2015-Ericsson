/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 *
 * @author rohit_000
 */
public abstract class Feature {
    
    protected final ArrayList<String> mSchema = FeatureConstructorSingleton.getInstance().getSchema(getTableName());
    protected final ArrayList<String> mFileList = FeatureConstructorSingleton.getInstance().getFileList(getTableName());
    protected final LinkedHashMap<String, String> mJobHash = FeatureConstructorSingleton.getInstance().getJobHash();
       
    public abstract String getTableName();
    
    public abstract void generateFeatureSingleValue(String[] tableRowArray);
    
    public void generateFeatureAllRows() throws IOException {
        for(String file : mFileList) {
            FlatFileReader reader = new FlatFileReader(file, ',');
            String[] tableRowArray;
            do {
                tableRowArray = reader.readRecord();
                generateFeatureSingleValue(tableRowArray);
            } while (tableRowArray != null);
        }
        FeatureConstructorSingleton.getInstance().clearJobHash();
    }
}
