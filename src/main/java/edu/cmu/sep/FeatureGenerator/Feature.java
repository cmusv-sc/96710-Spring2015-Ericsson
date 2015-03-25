/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author rohit_000
 */
public abstract class Feature {
    
    //public abstract void generateFeature();
    
    public abstract String[] getSchema() throws IOException;
    
    public String getSchemaFile() {
        return "inputData/schema.csv";
    }
    
    public String getOutputFeaturesFile() {
        return "outputData/job_features.csv";
    }
    
    public String[] getSchema(String table) throws IOException {
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
        
        String[] schemaArray = new String[schemaList.size()];
        schemaArray = schemaList.toArray(schemaArray);
        
        return schemaArray;
    }
}
