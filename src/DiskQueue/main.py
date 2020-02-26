import os, sys
import marshal
import random 
import msgpack


from threading import Lock
import threading

from .exceptions import Full, Empty
class DiskQueue:

    def __init__(self, path, queue_name, cache_size, memory_safe=False, max_size=None):

        self.queue_name = queue_name
        self.cache_size = cache_size

        self.queue_dir = os.path.join(path, self.queue_name) 
        self.index_file = os.path.join(self.queue_dir,'000')
        
        self.get_memory_buffer = []
        self.put_memory_buffer = []

        self.max_size = max_size

        self._thread_lock = threading.Lock()
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full  = threading.Condition(self.mutex)
        
        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0


        self._init_queue()


    def _init_queue(self):

        """ If thus Queue was being operated upon previously , recover checkpoints """

        if os.path.exists(self.queue_dir):
            with open(self.index_file) as f:
                self.head , self.tail = [int(x) for x in f.read().split(',')]

            self._sync_from_fs_to_memory_buffer()
            # TODO: this bug was not captured in the tests.
            self.head += 1
            self._sync_index_pointers(self.head, self.tail)

        else:
            self.head, self.tail = 0,0
            os.mkdir(self.queue_dir)
            with open(self.index_file, 'w') as f:
                f.write(f"{self.head},{self.tail}")
                f.flush()
                os.fsync(f.fileno())

        """ Initialize get & put memory buffers   """
        

    def _sync_index_pointers(self, head, tail):
        """
        Sync the index of  head & tail pointers
        """

        with open(self.index_file, 'r+') as f:
            f.read()
            f.seek(0)
            f.write(f"{head},{tail}")

            f.flush()
            os.fsync(f.fileno())

    def sync(self):
        """
        Explicitly sync the memory buffer to disk.
        """

        ''' 
        H(1)        T(3)
                                            
        FILEINDEX    :     0     1     2     3     
        QUEUECONTENT :    <0 1> [2 3] [4 5] <6 7>    
        POINTERS     :     ^ ^               ^ ^ 
                           G G               P P  
        
        H(1)        T(4)
                                            
        FILEINDEX    :     0     1     2     3     
        QUEUECONTENT :    [0 1] [2 3] [4 5] [6 7]    
        POINTERS     :                    
                           G=<>               P=<>
        '''


        with self._thread_lock:
            self._sync_memory_buffer_to_fs('put_buffer')
            self._sync_memory_buffer_to_fs('get_buffer')
            self.put_memory_buffer = []
            self.get_memory_buffer = []
            self.tail += 1
            if self.head > 0:
                self.head -= 1
            self._sync_index_pointers(self.head, self.tail)

    

    def _sync_memory_buffer_to_fs(self, buffer_type):
        """
        Sync the cache in memory to filesystem
        """
        if buffer_type == 'put_buffer':
            mem_buffer = self.put_memory_buffer
            file_name  = os.path.join(self.queue_dir, str(self.tail))
        else:
            file_name  = os.path.join(self.queue_dir, str(self.head-1))
            mem_buffer = self.get_memory_buffer

        with open(file_name, 'wb+') as fp:
            fp.write(msgpack.packb(mem_buffer))
            fp.flush()
            os.fsync(fp.fileno())



    def _sync_from_fs_to_memory_buffer(self, readonly=False):
        """
        Sync data from fs to memory cache, when read only is False(default) the file is
        deleted after loading it into memory , otherwise do not delete the file(used for peek()).
        """
       
        file_name  = os.path.join(self.queue_dir, str(self.head))
        
        if os.path.exists(file_name):
            with open(file_name, 'rb') as fp:
                data = fp.read()
                data = msgpack.unpackb(data)
                self.get_memory_buffer = data
                if not readonly:
                    try:
                        os.remove(file_name)
                    except Exception as e:
                        print(e)
                        print("error removing queue file {file_name} from disk")

    def __len__(self):
        """ Return the length of the queue"""
        with self._thread_lock:
            return (self.tail - self.head)  * self.cache_size + (len(self.get_memory_buffer) \
                    + len(self.put_memory_buffer))

    def close(self):
        pass


    def _get(self):

        # Check if anything is present in the `get` memory buffer
        if self.get_memory_buffer:
            return self.get_memory_buffer.pop(0)

        else:
            # Check head & tail pointers are same
            if self.head == self.tail:
                self.get_memory_buffer = self.put_memory_buffer
                self.put_memory_buffer = []

            else:
                self._sync_from_fs_to_memory_buffer()
                self.head += 1 
                self._sync_index_pointers(self.head, self.tail)
    
        try: 
            obj = self.get_memory_buffer[0]
        except IndexError:
            obj = None
        else:
            del self.get_memory_buffer[0]
        
        return obj


    def get(self, block=True , timeout=None):
        
        """
        Provides an api similar to stdlib queue module. 
        Gets an object from the queue, block by default if there is no item available to process,
        and waits if timeout is false, otherwise if timeout is true , wait for the `timeout` seconds and raise
        Empty exception. If block is false timeout variable is ignored
        """

        with self.not_empty:
            if not block:
                if self._qsize() == 0:
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize():
                    time_left = endtime - time()
                    if time_left < 0.0:
                        raise Empty
                    self.not_empty.wait(time_left)
            obj = self._get()

            # Notify all consumer threads that a slot is empty 
            self.not_full.notify()

            return obj


    def _qsize(self):
        return self.__len__()


    def _put(self, obj):

        if len(self.put_memory_buffer) >= self.cache_size:
            self._sync_memory_buffer_to_fs('put_buffer')
            self.put_memory_buffer = []
            self.tail += 1
            self._sync_index_pointers(self.head, self.tail)
        self.put_memory_buffer.append(obj)


    def put(self, obj, block=True, timeout=None):
        """
        Puts an obj into the queue, if block is True & timeout is None (default),
        block if necessary until the next slot is available, if timeout is a non-negative 
        number , it block for at most 'timeout' seconds and raises the full exception if no 
        free slot was available within that time, otherwise ('block' is false), put an item
        on the queue if a free slot is immediately available, else raise the full exception,
        ('timeout' is ignored in this case
        """
        
        with self.not_full:
            if self.max_size:
                if not block:
                    if self._qsize() >= self.max_size:
                        raise Full("Max que limit reached")
                elif timeout is None:
                    while self._qsize() >= self.max_size:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError('timeout must be a non negative number')
                else:
                    endtime = time() + self.timeout
                    while self._qsize() >= self.max_size:
                        time_left = endtime - time()
                        if remaining <= 0.0:
                            raise Full
            self._put(obj)
            self.unfinished_tasks += 1
            # notify other threads waiting on `not_empty` condition variable
            self.not_empty.notify()




    def task_done(self):
        '''Indicate that a formerly enqueued task is complete.
        Used by Queue consumer threads.  For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.
        If a join() is currently blocking, it will resume when all items
        have been processed (meaning that a task_done() call was received
        for every item that had been put() into the queue).
        Raises a ValueError if called more times than there were items
        placed in the queue.
        '''
        
        with self.all_tasks_done:
           unfinished = self.unfinished_tasks - 1
           if unfinished <= 0:
              if unfinished < 0:
                  raise ValueError('task_done() called too many times')
              self.all_tasks_done.notify_all()
           self.unfinished_task = unfinished




    def join(self):
        '''Blocks until all items in the Queue have been gotten & processed.
           
        The count of unfinished task goes up  whenever an item is added to the queue.
        The count goes down whener a consumer thread  calls task_done() to indicate
        the item was retrieved and all work on it is complete.
        When the  count of unfinished  tasks drops to zero, join unblocks.
        '''

        with self.all_task_done:
            while self.unfinished_tasks:
                self.all_tasks_done.wait()

      
    def _read_file(self, index):
        '''Read a file with given `index` from disk'''
        
        file_name = os.path.join(self.queue_dir, str(index))
        if os.path.exists(file_name):
            with open(file_name, 'rb') as fp:
                data = fp.read()
                data = msgpack.unpackb(data)
                return data 
        else:
            raise KeyError('yo')

 
    def peek(self,count=1):
        '''Return the top items of queue without popping it, 
           the default no of items is 1 , otherwise  return `count` no of items.
           Non blocking by default.Multiple threads doinf a peek() will return the same value
        '''

        if count <= 0:
            raise ValueError('Argument to peek() must be a positive integer')
        else:
            c = 1
            get_buffer_index = 0
            file_index = 0
            objects = []
            data = self.get_memory_buffer

            while c <= count:
                if data  and (get_buffer_index < self.cache_size) :
                        obj = data[get_buffer_index]
                        objects.append(obj)
                        c += 1
                        get_buffer_index += 1
                else:
                    data = self._read_file(file_index)
                    file_index += 1
                    get_buffer_index = 0

            if len(objects) == 1 : 
                return objects[0]
            else:
                return objects            
 
            
    def put_nowait(self, item):
        '''Put an item into the queue without blocking.
        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        '''
        return self.put(item, block=False)

    def get_nowait(self):
        '''Remove and return an item from the queue without blocking.
        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        '''
        return self.get(block=False)



	
