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
public abstract class CombinedFeature extends Feature {
    
    private static final String TABLE_NAME = "job_features";
    protected final ArrayList<String> mSchema = FeatureConstructorSingleton.getInstance().getGeneratedFeaturesSchema();
    protected final int mJobIdIndex = mSchema.indexOf("job ID");

    public String getTableName() {
        return TABLE_NAME;
    }
    
    public abstract void generateFeatureSingleValue(String[] tableRowArray);
}
