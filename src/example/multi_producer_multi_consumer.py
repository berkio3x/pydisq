
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

