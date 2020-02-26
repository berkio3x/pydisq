![](https://img.shields.io/static/v1?label=made%20in&message=python&color=%3CCOLOR%3E)


# pydisq 🦦
 *Disk assisted queue implemented in python*

----

### Features

* Queue content is persisted on disk after memory cache size is crossed.
* Expose all apis similar to standard library `queue.Queue` object.
* Thread safe, multiple threads can work on queue data structure.
* Provides fast & effecient binary serialization via msgpack serialization format.
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


#### Multiple producer / consumer example (threads).
```python
from DiskQueue import DiskQueue
import threading
import time
import random

diskq = DiskQueue(path='./', queue_name='es-miss', cache_size=4)


def producer(producer_id):
    while True:
        obj = random.randint(1,50)
        print(f'[🤖 WORKER THREAD] : {producer_id}: {obj}')
        diskq.put(obj)
        time.sleep(random.randint(2,4))


def consumer(worker_id):
    
    obj = diskq.get()
    while obj:
        print(f'[🙋‍♂️ CONSUMER THREAD] => {worker_id} : {obj}')
        time.sleep(1)
        obj = diskq.get()



producer_thread1 = threading.Thread(target=producer, args=(1,))
producer_thread2 = threading.Thread(target=producer, args=(2,))


worker_thread1 = threading.Thread(target=consumer, args=(1,))
worker_thread2 = threading.Thread(target=consumer, args=(2,))

producer_thread1.start();
producer_thread2.start();

worker_thread1.start();
worker_thread2.start()


producer_thread1.join();
producer_thread2.join();
worker_thread1.join()
worker_thread2.join()
```

##### peek()
```python

# peek will not remove the objects from queue

cache_size = 2
queue = 'testq'
datadir = './'

diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

diskq.put(1)
diskq.put(2)
diskq.put(3)
diskq.put(4)
diskq.put(5)
diskq.put(6)
diskq.put(7)
diskq.put(8)

diskq.peek(4)  # will give [1,2,3,4]
```


### Tests
Run test by using this commands.
```bash
$ cd src/tests && pytest -vvv

```


#### Contributing
Want to add features? Improve existing code or fix bugs? Awesome!! Clone the repository and submit a pull request.


