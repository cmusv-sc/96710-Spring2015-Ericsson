# Big Data Feature Construction & Selection
Software Engineering Practicum - CMU S15 - Ericsson Research Team

- **Developers:** Rohit Kabadi, Khoa DoBa, Jacob Wu, Joe Mirizio
- **Advisor:** Ole J. Mengshoel
- **Client:** Ericsson Research


## Feature Construction

### Sequential Implementation (Java)

#### Build
For sequential feature construction, users should first compile the code to generate the JAR file:

1. Use the command line to generate JAR file using Maven. 
   - ```mvn package```
2. Importing project from IDE, and generate JAR file from IDE.

**Software Requirements**:

 - Java JRE 1.7
 - Maven (Optional)
 - IDE (Optional)

#### Preparation
Users need to do following preparation work:

1. Create an ```inputData``` directory under the same directory with JAR file
2. Download the [Google ```ClusterData2011_2 ```](https://code.google.com/p/googleclusterdata/) into the ```inputData``` directory.
    - **NOTE**: Under ```inputData``` should be subdirectories of the Google dataset, such as ```job_events```, ```task_events```, and ```task_usage```
3. The ```Schema.csv``` file should also be under the ```inputData``` directory


#### Run
After completing the preparation steps mentioned above, users can run the sequential feature generator with the following command:

```sh
$ java –classpath FeatureGenerator.jar OneFeatureGenerator
```

#### Extend
The sequential feature construction process:

1. Initialize all Google dataset tables
2. Get all table schemas as the key name with table name
3. Initialize input and output files directories, and load all input ```.gzip``` files under each subdirectory
4. Initialize the generated feature structure
5. Load different tables to generate corresponding features
    - **NOTE**: All processed CSV files are uncompressed then analyzed. After processing, the uncompressed file is deleted
6. Generate the output result file

To add or modify features, the respective table class should be edited with the aggregation algorithm and derived fields.

### Spark Implementation (Python)

#### Build
The feature construction Spark implementation is written in Python and therefore does not require compilation. 

**Software Requirements**:

 - Python 2.7
 - [Spark 1.2+](https://spark.apache.org/downloads.html)

#### Preparation
1. Download all or a subset of the [Google ```ClusterData2011_2 ```](https://code.google.com/p/googleclusterdata/)
2. Optional: Copy and modify the default config file (```/src/main/resources/config/feature_construction.ini```)

#### Run
    # SPARK_DIR       - Spark installation directory
    # SPARK_MASTER    - Master URI of the running spark instance
    # DATA_DIR        - Root directory of all data
    # OUTPUT_DIR      - Output directory
    # CONFIG_FILE     - Configuration file
    # SCHEMA_FILE     - Input data schema  
    # OUTPUT_HEADERS  - A plaintext file containing the constructed feature headers

    $ ${SPARK_DIR}/bin/spark-submit --master ${SPARK_MASTER} \
                                    --py-files rdd.py,feature.py \
                                    feature_construction.py \
                                    ${DATA_DIR} \
                                    ${OUTPUT_DIR} \
                                    --config $CONFIG_FILE \
                                    --schema $SCHEMA_FILE \
                                    --output_headers $OUPUT_HEADERS

In some cases the output data will need to be cleaned. Python likes to put exponentials in the output.
If this happens you can run clean.py on the CSV files to remove this formatting.
    $ clean.py ${OUTPUT_DIR}/part*

To combine all part files, you can use a file editor or unix utility like ```cat```.
    $ cat ${OUTPUT_DIR}/part* > features.csv


#### Extend
The flexible configuration file allows users to easily construct new features. To add a new feature:

1. Add the feature name to the ```features ``` list in the  ```[main]``` section
2. Add a section with the name of the feature containing:
    - ```table```: Table containing the data
    - ```fields```: Fields to derive from
    - ```functions```: List of aggregation functions to apply
3. Implement the aggregation functions in ```feature.py```

---
    [main]
    tables   = task_events,job_events,...
    key      = job ID
    features = job status,CPU rate,...
    ...
    
    [table_files]
    task_events = part-00000-of-00500.csv.gz
    job_events  = part-00000-of-00500.csv.gz
    ...
    
    ; FEATURES
    [job status]
    table     = job_events
    fields    = event type
    functions = job_status
    
    [CPU rate]
    table     = task_usage
    functions = max,min,average
    ...

## Feature Selection

### Requirements
The following software is required to build and run the Feature Selection package in this repo:

1. Weka 3.7.12
2. Java JRE 1.7
3. Eclipse Java IDE
4. Spark 1.2.1 with Hadoop 2.4

### Build and Installation Instruction
1. Install Weka
2. In Weka's Package Manager, install distributedWekaBase and distributedWekaSpark
3. Load the Feature Selection project on Eclipse and compile the code to a jar file
4. Overwrite the new jar file with this file: wekafiles/packages/distributedWekaPark/distributedWekaSpark.jar
5. Relaunch Weka and start Knowledge Flow

### Run Instruction Video
https://www.youtube.com/watch?v=AyzMb9Khi7E
