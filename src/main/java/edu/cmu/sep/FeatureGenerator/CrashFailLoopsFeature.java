/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author rohit_000
 */
public class CrashFailLoopsFeature extends TaskEventsFeature {
    
  public CrashFailLoopsFeature() {
            
  }  
    
  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String eventType = tableRowArray[mSchema.indexOf("event type")];
    if (eventType == null || !mJobHash.containsKey(jobId)) {
      return;
    }
        boolean taskFailStatus = false;
        if (Integer.parseInt(eventType) == 3) {
            taskFailStatus = true;
        }
      ArrayList<String> value = mJobHash.get(jobId);
      if (value == null) {
        value = new ArrayList<String>();
        value.add(Integer.toString(0));
        value.add(Boolean.toString(taskFailStatus));
      } else {
          if( Boolean.parseBoolean(value.get(1)) ) {
            if(Integer.parseInt(eventType) == 1) {
                int numCrashLoops = Integer.parseInt(value.get(0));
                numCrashLoops++;
                value.clear();
                value.add(Integer.toString(numCrashLoops));
                value.add(Boolean.toString(false));
            }
          }
          else {
              if( Integer.parseInt(eventType) == 3) {
                value.remove(1);
                value.add(Boolean.toString(true));
              }
          }
      }
        mJobHash.put(jobId, value);
  }
  
  public void generateFeatureAllRows() throws IOException {
    for(String file : mFileList) {
        FlatFileReader reader = new FlatFileReader(file, ',');
        String[] tableRowArray;
        do {
            tableRowArray = reader.readRecord();
            if(tableRowArray[0] == null) break;
            generateFeatureSingleValue(tableRowArray);
        } while (tableRowArray != null);
    }
    
    // Clear out second value in arraylist
    for (Map.Entry<String, ArrayList<String>> entry : mJobHash.entrySet()) {
        String key = entry.getKey();
        ArrayList<String> values = entry.getValue();
        if(values != null) {
            values.remove(1);
            mJobHash.put(key, values);  
        }

    }
    
    FeatureConstructorSingleton.getInstance().updateOutputFile();
    FeatureConstructorSingleton.getInstance().clearJobHash();
  }
}
