/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author rohit_000
 */
public class JobDurationFeature extends TaskUsageFeature {
    
    public void generateFeatureSingleValue(String[] tableRowArray) {
        
        String jobId = tableRowArray[mJobIdIndex];
        String startTime = tableRowArray[mTableSchema.indexOf("start time")];
        String endTime = tableRowArray[mTableSchema.indexOf("end time")];
        if(!mJobHash.containsKey(jobId)) {
            return;
        }
        ArrayList<String> value = mJobHash.get(jobId);
        if (value == null) {
          value = new ArrayList<String>();
          String jobDuaration = Long.toString(Long.parseLong(endTime) - Long.parseLong(startTime));
          value.add(jobDuaration);
          mJobHash.put(jobId, value);
        } else {
          String jobDuaration = Long.toString(Long.parseLong(value.get(0)) + Long.parseLong(endTime) - Long.parseLong(startTime));
          value.remove(0);
          value.add(jobDuaration);
          mJobHash.put(jobId, value);
        }
    }
    
}
