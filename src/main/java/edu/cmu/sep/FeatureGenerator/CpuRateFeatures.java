package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class CpuRateFeatures extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String cpuRate = tableRowArray[mSchema.indexOf("CPU rate")];
    if( cpuRate == null || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg CPU rate
    if (values == null) {
      values = new ArrayList<String>();
      values. add(cpuRate);
      values. add(cpuRate);
      values. add(cpuRate + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (cpuRate.compareTo(min) < 0)
        min = cpuRate;
      if (cpuRate.compareTo(max) > 0)
        max = cpuRate;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(cpuRate)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }

}
