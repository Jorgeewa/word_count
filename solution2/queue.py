from collections import deque



class Queue:
    '''
        This class method extends the functionality of python deque to retrieve, requeue and specify when work is complete

        Parameters
        ----------
        None
    '''
    
    def __init__(self):
        self.data = deque()
        self.is_finished = False
        self.counter = 0
        self.is_working = 0
    
    def add(self, data):
        '''
            This adds data to the queue and increments counter

            Parameters
            ----------
            data: data to be added

            Returns
            --------
            None
        '''
        self.data.append(data)
        self.counter += 1
    
    def requeue(self, data):
        '''
            This requeues data to the queue in a situation where a worker failed

            Parameters
            ----------
            data: data to be requeued

            Returns
            --------
            None
        '''
        self.data.appendleft(data)
        self.counter += 1
        self.is_working -= 1
    
    def retrieve(self):
        '''
            This retrieves data from the queue

            Parameters
            ----------
            None

            Returns
            --------
            Data according to FIFO
        '''
        if self.counter == 0:
            return
        self.counter -= 1
        self.is_working += 1
        return self.data.popleft()
    
    def finished_work(self):
        '''
            This decrements the number of workers and also updates the is finished parameters when there are no workers left

            Parameters
            ----------
            None

            Returns
            --------
            None
        '''
        if self.is_working == 0:
            return
        self.is_working -= 1
        
        if self.is_working == 0 and self.counter == 0:
            self.is_finished = True
        
    def is_finished_jobs(self):
        '''
            This returns a boolean if all jobs are finished

            Parameters
            ----------
            None

            Returns
            --------
            Boolean
        '''
        return self.is_finished
    
    def view(self):
        '''
            This returns a peep into all the data in the queue

            Parameters
            ----------
            None

            Returns
            --------
            All the data in the queue
        '''
        for data in self.data:
            print(data)
    
    
