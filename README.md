# pydisq 🦦
 *Disk assisted queue implemented in python*

----

### Features

* Queue content is persisted on disk after memory cache size is crossed.
* Thread safe, multiple threads can work on queue data structure.
* Ability to explicitly sync memory buffers to disk when required.
* Recovers from last check points in case of program crash.



⚠️ NOTE:  ***Work in progress***

```python
import random

diskQ = DiskQueue(path='./', queue_name='elastic-insert-miss', cache_size=10)

dummy_data = []

# Add random 50 objects to the queue.

for i in range(50):
    diskQ.put({'a':random.randint(1,2000)})

# Pull objects from the queue

for i in range(50):
    obj = diskQ.get()
    print(obj)
```


#### Multiple workers using threads.
```python
from DiskQueue import DiskQueue
import threading
import time
import random

# define the queue
diskq = DiskQueue(path='./', queue_name='es-miss', cache_size=4)

# Insert some objects into the queue
for i in range(20):
    diskq.put({'data':i})


# define worker method
def consumer(worker_id):
    
    obj = diskq.get()
    while obj:
        print(f'Thread => {worker_id} : {obj}')
        time.sleep(1)
        obj = diskq.get()

consumers = []


# Create 2 worker threads to consume from the the queue
for i in range(2):
    T = threading.Thread(target=consumer, args=(random.randint(1,10),))
    consumers.append(T)
    T.start()


# Wait for the threads to join
for worker in consumers:
    worker.join()
```

### Tests
Run test by using this commands.
```bash
$ cd src/tests && pytest -vvv

```
