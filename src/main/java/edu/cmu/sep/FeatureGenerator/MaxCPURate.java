package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class MaxCPURate extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String maxCPURate = tableRowArray[mTableSchema.indexOf("maximum CPU rate")];
    if (maxCPURate == null || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg maximum CPU rate
    if (values == null) {
      values = new ArrayList<String>();
      values.add(maxCPURate);
      values.add(maxCPURate);
      values.add(maxCPURate + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (maxCPURate.compareTo(min) < 0)
        min = maxCPURate;
      if (maxCPURate.compareTo(max) > 0)
        max = maxCPURate;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(maxCPURate)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }
}
