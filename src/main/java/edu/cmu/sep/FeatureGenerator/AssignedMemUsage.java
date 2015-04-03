package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class AssignedMemUsage extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String assignedMemUsage = tableRowArray[mSchema.indexOf("assigned memory usage")];
    if (assignedMemUsage == null || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg assigned memory usage
    if (values == null) {
      values = new ArrayList<String>();
      values.add(assignedMemUsage);
      values.add(assignedMemUsage);
      values.add(assignedMemUsage + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (assignedMemUsage.compareTo(min) < 0)
        min = assignedMemUsage;
      if (assignedMemUsage.compareTo(max) > 0)
        max = assignedMemUsage;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(assignedMemUsage)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }
}