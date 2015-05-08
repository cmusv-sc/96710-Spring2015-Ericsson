package edu.cmu.sep.FeatureGenerator;

/**
 * Created by Jacob on 3/9/15.
 */
public class OneFeatureGenerator {

    public static void main(String[] argv) throws Exception {

        if (argv.length < 2) {
            System.out.println("Usage: java -classpath FeatureGenerator.jar OneFeatureGenerator [Google Dataset Directory] [Output CSV File] [Number of files out of 500 (optional)]");
            return;
        }

        String dataDirectory = argv[0];
        String outputFile = argv[1];
        int numFilesToProcess = 500;

        if (argv.length > 2) {
            numFilesToProcess = Integer.parseInt(argv[2]);
        }

        FeatureConstructorSingleton.getInstance().Initialize(dataDirectory, outputFile, numFilesToProcess);

        JobEventsFeature jobEventsFeature = new JobEventsFeature();
        jobEventsFeature.generateFeatureAllRows();

        TaskUsageFeature taskUsageFeature = new TaskUsageFeature();
        taskUsageFeature.generateFeatureAllRows();
    }

}