# Big Data Feature Construction & Selection
Software Engineering Practicum - CMU S15 - Ericsson Team

- **Developers:** Rohit Kabadi, Khoa DoBa, Jacob Wu, Joe Mirizio
- **Advisor:** Ole J. Mengshoel
- **Client:** Ericsson Research

## Feature Construction
### Spark Implementation

#### Build
The feature construction Spark implementation is written in Python and therefore does not require compilation. 

**Software Requirements**:

 - Python 2.7
 - Spark 1.2

#### Run
    # SPARK_DIR       - Spark installation directory
    # SPARK_MASTER    - Master URI of the running spark instance
    # DATA_DIR        - Root directory of all data
    # OUTPUT_DIR      - Output directory
    # CONFIG_FILE     - Configuration file
    # SCHEMA_FILE     - Input data schema  
    # OUTPUT_HEADERS  - A plaintext file containing the constructed feature headers

    ${SPARK_DIR}/bin/spark-submit   --master ${SPARK_MASTER} \
                                    --py-files rdd.py,feature.py \
                                    feature_construction.py \
                                    ${DATA_DIR} \
                                    ${OUTPUT_DIR} \
                                    --config $CONFIG_FILE \
                                    --schema $SCHEMA_FILE \
                                    --output_headers $OUPUT_HEADERS
    
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
    task_events        = part-00000-of-00500.csv.gz
    job_events         = part-00000-of-00500.csv.gz
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

### Java sequential Implementation
#### Build
For sequential feature construction, Users should compile the code to generate the jar file from source code first. 

**Software Requirements**:

 - Java jre 1.7

#### Preparation
Users could do this step from command or IDE. Then, Users need to do following preparation work:
1.     Create “inputData” directory under the same directory with Jar file.
2.     Place all Google dataset under “inputData” directory.
3.     Users should noted that, under “inputData” should be subdirectories of Google dataset, such as “job_events”, “task_events”, and “task_usage”.
4.     Schema.cvs file should also under “inputData” directory.

#### Run
After all steps mentioned above, users could run the sequential feature generator with the following command:
java –classpath FeatureGenerator.jar OneFeatureGenerator

#### Extend
The processes of the sequential feature construction code are:
1.     Initialize the all Google dataset tables.
2.     Get all table schemas as the key name with table name.
3.     Initialize input and output files directories, and load all input gzip files under each subdirectories.
4.     Initialize the generated feature structure.
5.     Loading different tables to generate corresponding features.
6.     Generate output result file.       	
 
In step five; features are generated based on types of table. Therefore, each type of table to generate the feature, all gzip csv files will be decompressed first, analyzed, and deleted the decompressed files.