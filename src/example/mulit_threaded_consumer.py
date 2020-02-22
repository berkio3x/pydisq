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


