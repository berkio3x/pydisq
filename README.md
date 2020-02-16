# python-disk-assisted-queue
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
