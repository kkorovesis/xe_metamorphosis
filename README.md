## Solution for Data Engineer Code Challenge

In order to consume message from kafka stream and process them separately, I build two processes. Process `get_process` consumes the payload from messages and puts them temporally it in a queue, while `insert_process` pulls data from the queue and executes SQL insert to store them in MYSQL db ( see `utils.py`). I created methods for connecting and inserting data to MYSQL db. In addition I created a unit test and a integration test (see `unit_tests.py`). To run this code in any system, I setup an image, which you need to build just by executing I single command with `Docker`, and run the image in your workstation as described. There is also a logging system for debugging. 
 
####  Prepare Database

##### Create Schema (database) , it is already created
```bash
Create Schema kkdataengineer;
```

##### Change database
```bash
use kkdataengineer;
```

##### Create table as described
```bash
CREATE TABLE IF NOT EXISTS Classifieds (
    id VARCHAR(32) PRIMARY KEY NOT NULL,
    customerId VARCHAR(32) NOT NULL,
    createdAt DATETIME,
    text TEXT,
    adType VARCHAR(12),
    price FLOAT,
    currency VARCHAR(4),
    paymentType VARCHAR(12),
    paymentCost FLOAT);

```

#### Building image
```bash
docker build -t xe_dec .
```

#### Run image (provide db information and credentials)
```bash
docker run --entrypoint ./execute.sh -t xe_dec <SERVER HOST> <SCHEMA> <USERNAME> <PASSWORD>
```


