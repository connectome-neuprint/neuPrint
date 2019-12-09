# Scripts to update neo4j database incrementally through neuprint HTTP

## Before Running

* install neuprint-python
* set the environment variable NEUPRINT_APPLICATION_CREDENTIALS=YOURTOKEN using your neuprint token
* ensure that neuprint HTTP is running and that you have admin privileges

## Incrementally updating neo4j

merge and split segment operations have been implemented.  A NeuPrintUpdater object first
needs to be created with the location of neuprint http server and the datset.
Multiple transactions can be executed for the same updater.  For all functions,
an optional debug variable, when true, will prevent the database from being written.

### Split

Splitting a segment requires an array of synapse points to be split from the body.  The user
needs to specify the new body id and can specify additional properties for both resulting segments.

```python
import incremental_update as up
import time

updater = up.NeuPrintUpdater("localhost:13000", "hemibrain", verify=False)

start = time.time()
updater.split_segment(5901220939, [(10023, 21022, 15694)], {"bodyId": 926345671224568}, debug=False)
print(time.time()-start)
```

### Merge

Merging two segments together (undoing the previous operation)

```python
import incremental_update as up
import time

updater = up.NeuPrintUpdater("localhost:13000", "hemibrain", verify=False)

start = time.time()
#updater.merge_segments([5901232424,5813024885], debug=True)
updater.merge_segments([5901220939,926345671224568], debug=False)
print(time.time()-start)
```
