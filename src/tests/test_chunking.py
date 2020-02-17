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


def test_queue_chunk_removed_after_loading_into_buffer():
    cache_size = 2
    objects = range(10)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        diskq.put(i)

    for i in objects:
        diskq.get()

    queue_dir_content = os.listdir(os.path.join(datadir, queue))
    
    assert len(queue_dir_content) == 1 
    assert queue_dir_content[0] == '000'
    remove_queue(queue)


def test_get_queue_length_when_memory_buffer_size_equal_obects_count():
    cache_size = 10
    objects = range(10)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        diskq.put(i)

    assert len(diskq) == len(objects)
    remove_queue(queue)


def test_get_queue_length_when_obects_count_exceed_memory_buffer_size():
    cache_size = 10
    objects = range(50)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        diskq.put(i)

    assert len(diskq) == len(objects)
    remove_queue(queue)

def test_get_queue_length_when_obects_count_is_less_than_memory_buffer_size():
    cache_size = 50
    objects = range(10)
    queue = 'testq'
    datadir = './'
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        diskq.put(i)

    assert len(diskq) == len(objects)
    remove_queue(queue)


def test_explicit_sync_creates_another_chunk():

    cache_size = 10
    objects = range(20)
    queue = 'testq'
    datadir = './'
    
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        diskq.put(i)


    # Remove index file '000' before counting queue chunks
    os.remove(os.path.join(datadir, os.path.join(queue, '000')))
    assert int(len(objects)/cache_size) - 1 == len(os.listdir(os.path.join(datadir, queue)))
    diskq.sync()
    assert int(len(objects)/cache_size) == len(os.listdir(os.path.join(datadir, queue))) 
    
    remove_queue(queue)



def test_index_pointers_are_0_0_on_new_queue():

    cache_size = 2
    objects = range(10)
    queue = 'testq'
    datadir = './'

    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)
    assert diskq.head == 0
    assert diskq.tail == 0

    remove_queue(queue)


def test_tail_pointer_increments_correctly():

    cache_size = 5
    objects = range(10)
    queue = 'testq'
    datadir = './'

    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)
    
    assert diskq.tail == 0 
    prev_tail = diskq.tail

    # Tail pointer is incremented when no. of objects equal to memory buffer is ,
    # crossed & they are flushed to disk

    for i in objects:
        diskq.put(i)

    assert prev_tail + 1  == diskq.tail 
    remove_queue(queue)


def test_tail_pointer_sync_to_disk_on_increment():

    cache_size = 5
    objects = range(10)
    queue = 'testq'
    datadir = './'

    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)
    
    assert diskq.tail == 0 
    prev_tail = diskq.tail

    # Tail pointer is incremented when no. of objects equal to memory buffer is ,
    # crossed & they are flushed to disk

    for i in objects:
        diskq.put(i)

    index_file = os.path.join(datadir, os.path.join(queue,'000'))
    
    with open(index_file) as f:
        head , tail = [int(x) for x in f.read().split(',')]
   
    assert tail  == diskq.tail 
    remove_queue(queue)


def tesxt_queue_recover_with_last_working_breakpoints():
    cache_size = 2
    objects = range(10)
    queue = 'testq'
    datadir = './'

    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        diskq.put(i)

    diskq.sync()
    diskq.close()

    # Test recovery of queue
    diskq = DiskQueue(path=datadir, queue_name=queue, cache_size=cache_size)

    for i in objects:
        obj = diskq.get()
        assert i == obj
        print (obj)
    #remove_queue(queue)

