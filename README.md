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

