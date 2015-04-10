package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class UnmappedPageCache extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String unmappedMemUsage = tableRowArray[mSchema.indexOf("unmapped page cache")];
    if (unmappedMemUsage == null || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg unmapped page cache
    if (values == null) {
      values = new ArrayList<String>();
      values.add(unmappedMemUsage);
      values.add(unmappedMemUsage);
      values.add(unmappedMemUsage + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (unmappedMemUsage.compareTo(min) < 0)
        min = unmappedMemUsage;
      if (unmappedMemUsage.compareTo(max) > 0)
        max = unmappedMemUsage;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(unmappedMemUsage)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }
}