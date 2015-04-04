/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 *
 * @author rohit_000
 */
public class CrashFailLoopsFeature extends TaskEventsFeature {
    
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
          System.out.println(jobId);
          System.out.println(Boolean.toString(taskFailStatus));
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
              if( Integer.parseInt(eventType) == 1) {
                value.remove(1);
                value.add(Boolean.toString(true));
              }
          }
      }
        mJobHash.put(jobId, value);
  }
}
