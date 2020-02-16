from DiskQueue import DiskQueue
import os
import pytest
import shutil


def remove_queue(queue):
    shutil.rmtree(queue)



def test_queue_created():
    cache_size = 10
    objects = range(50)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)
    queue_dir = os.path.join(datadir, queue)
    assert os.path.exists(queue_dir) == 1
    remove_queue(queue)

def test_queue_chunking():
    cache_size = 10
    objects = range(50)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)
    
    for i in objects:
        diskq.put(i)
    
    for i in range(0, int(len(objects)/cache_size) - 1 ):
        filename = os.path.join(
                    os.path.join(datadir, queue),
                    str(i)
                )
        assert os.path.exists(filename)
    remove_queue(queue)

def test_get_put_when_buffer_size_equals_objects_count():
    cache_size = 20
    objects = range(20)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)
    
    for i in objects:
        diskq.put(i)

    for i in objects:
        assert i == diskq.get()
    remove_queue(queue)
   
def test_get_put_when_buffer_size_exceeds_objects_count():
    cache_size = 20
    objects = range(50)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        diskq.put(i)

    for i in objects:
        assert i == diskq.get()
    remove_queue(queue)
   

