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
        String startTime = tableRowArray[mSchema.indexOf("start time")];
        String endTime = tableRowArray[mSchema.indexOf("end time")];
        String value = mJobHash.get(jobId);
        if (value == null) {
            mJobHash.put(jobId, Long.toString(Long.parseLong(endTime) - Long.parseLong(startTime)));
        } else {
            mJobHash.put(jobId, Long.toString(Long.parseLong(value) + Long.parseLong(endTime) - Long.parseLong(startTime)));
        }
    }
    
}
