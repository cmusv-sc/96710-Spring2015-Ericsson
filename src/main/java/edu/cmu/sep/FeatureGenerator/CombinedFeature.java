/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 *
 * @author rkabadi
 */
public class CombinedFeature extends Feature {
    
    private static final String TABLE_NAME = "job_features";
    protected final ArrayList<String> mSchema = FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema();
    protected final int mJobIdIndex = mSchema.indexOf("job ID");

    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void generateFeatureSingleValue(String[] tableRowArray) {
    
        String jobId = tableRowArray[mJobIdIndex];
        Float unmappedPageCache = 0f;
        Float totalPageCache = 0f;
        Float assignedMemoryUsage = 0f;
        Float canonicalMemoryUsage = 0f;
        Float maximumMemoryUsage = 0f;
        Float cpuRate = 0f;
        Float diskIOTime = 0f;
        try {
            unmappedPageCache = Float.parseFloat(tableRowArray[FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema().indexOf("avg_unmapped_page_cache")]);
            totalPageCache = Float.parseFloat(tableRowArray[FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema().indexOf("avg_total_page_cache")]);
            assignedMemoryUsage = Float.parseFloat(tableRowArray[FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema().indexOf("avg_assigned_memory_usage")]);
            canonicalMemoryUsage = Float.parseFloat(tableRowArray[FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema().indexOf("avg_canonical_memory_usage")]);
            maximumMemoryUsage = Float.parseFloat(tableRowArray[FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema().indexOf("avg_maximum_memory_usage")]);
            cpuRate = Float.parseFloat(tableRowArray[FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema().indexOf("avg_cpu_rate")]);
            diskIOTime = Float.parseFloat(tableRowArray[FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema().indexOf("avg_disk_io_time")]);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(!mJobHash.containsKey(jobId)) {
          return;
        }
        ArrayList<String> values = mJobHash.get(jobId);

        if (values == null) {
            values = new ArrayList<String>();
            Float assignedVsMaximumMemoryUsage = maximumMemoryUsage == 0? 0 : (assignedMemoryUsage) / (maximumMemoryUsage);
            Float canonicalVsMaximumMemoryUsage = maximumMemoryUsage == 0? 0 : (canonicalMemoryUsage) / (maximumMemoryUsage);
            Float unmappedVsTotalPageCacheRatio = maximumMemoryUsage == 0? 0 : (unmappedPageCache) / (totalPageCache);
            Float cpuRateAndAssignedMemoryUsage = assignedMemoryUsage == 0? 0 : (cpuRate) * (assignedMemoryUsage);
            Float cpuRateAndCanonicalMemoryUsage = canonicalMemoryUsage == 0? 0 : (cpuRate) * (canonicalMemoryUsage);
            Float cpuRateAndMaximumMemoryUsage = (cpuRate) * (maximumMemoryUsage);
            Float totalPageCacheVsCanonicalMemoryUsage = canonicalMemoryUsage == 0? 0 : (totalPageCache) / (canonicalMemoryUsage);
            Float totalPageCacheVsAssignedMemoryUsage = assignedMemoryUsage == 0? 0 : (totalPageCache) / (assignedMemoryUsage);
            Float totalPageCacheVsMaximumMemoryUsage = maximumMemoryUsage == 0? 0 : (totalPageCache) / (maximumMemoryUsage);
            Float cpuRateAndDiskIOTime = (cpuRate) * (diskIOTime);
            Float assignedMemoryUsageVsDiskIOTime = diskIOTime == 0? 0 : (assignedMemoryUsage) / (diskIOTime);
            Float canonicalMemoryUsageVsDiskIOTime = diskIOTime == 0? 0 : (canonicalMemoryUsage) / (diskIOTime);
            Float maximumMemoryUsageVsDiskIOTime = diskIOTime == 0? 0 : (maximumMemoryUsage) / (diskIOTime);

            values.add(Float.toString(assignedVsMaximumMemoryUsage));
            values.add(Float.toString(canonicalVsMaximumMemoryUsage));
            values.add(Float.toString(unmappedVsTotalPageCacheRatio));
            values.add(Float.toString(cpuRateAndAssignedMemoryUsage));
            values.add(Float.toString(cpuRateAndCanonicalMemoryUsage));
            values.add(Float.toString(cpuRateAndMaximumMemoryUsage));
            values.add(Float.toString(totalPageCacheVsCanonicalMemoryUsage));
            values.add(Float.toString(totalPageCacheVsAssignedMemoryUsage));
            values.add(Float.toString(totalPageCacheVsMaximumMemoryUsage));
            values.add(Float.toString(cpuRateAndDiskIOTime));
            values.add(Float.toString(assignedMemoryUsageVsDiskIOTime));
            values.add(Float.toString(canonicalMemoryUsageVsDiskIOTime));
            values.add(Float.toString(maximumMemoryUsageVsDiskIOTime));
            mJobHash.put(jobId, values);
        }
  }
  
  protected void addFeatureToSchema() {
    addFeatureToSchema("assigned_vs_max_memory_usage");  
    addFeatureToSchema("canonical_vs_max_memory_usage");
    addFeatureToSchema("unmapped_vs_total_page_cache");
    addFeatureToSchema("cpu_rate * canonical_memory_usage");
    addFeatureToSchema("cpu_rate * assigned_memory_usage");
    addFeatureToSchema("cpu_rate * max_memory_usage");
    addFeatureToSchema("total_page_cache_vs_canonical_memory_usage");
    addFeatureToSchema("total_page_cache_vs_assigned_memory_usage");
    addFeatureToSchema("total_page_cache_vs_maximum_memory_usage");
    addFeatureToSchema("cpu_rate_and_disk_io_time");
    addFeatureToSchema("assigned_memory_usage_vs_disk_io_time");
    addFeatureToSchema("canonical_memory_usage_vs_disk_io_time");
    addFeatureToSchema("maximum_memory_usage_vs_disk_io_time");
  }
}
