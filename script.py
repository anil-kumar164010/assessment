
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

class Task:
    def __init__(self, task_id, duration, dependencies=None):
        self.task_id = task_id
        self.duration = duration
        self.dependencies = dependencies if dependencies else [] 
        self.completed = threading.Event()
    

    def execute(self):
        print("task execute")
        print(f'Task {self.task_id} started ..')
        print(self.duration)
        time.sleep(self.duration)
        print(f'Task {self.task_id} completed ...')
        self.completed.set()



class TaskSchedular:
    def __init__(self, max_parallel_task):
        self.max_parallel_task =max_parallel_task
        self.task_queue = Queue()
        self.task_map = {}
        self.lock = threading.Lock()
        
    def add_task(self,task):
        with self.lock:
            self.task_map[task.task_id]=task
            self.task_queue.put(task)

    def _process_task(self,task):
        for dep in task.dependencies:
            print("process task")
            print(self.task_map[dep].completed)
            # self.task_map[dep].completed.wait()  # NOTE I AM FACING LITTLE ISSUE WITH WAIT FUNCTION 
            print("task ap")
            task.execute()

    def schedule_tasks(self):
        with ThreadPoolExecutor(max_workers= self.max_parallel_task) as executor:
            while not self.task_queue.empty() or any(not task.completed.is_set() for task in self.task_map.values()):
                
                if not self.task_queue.empty():
                    task=self.task_queue.get()
                    print(task)
                    executor.submit(self._process_task,task)
    


if __name__=='__main__':
    task1 = Task(task_id="Task1", duration=2) # No dependencies
    task2 = Task(task_id="Task2", duration=3, dependencies=["Task1"]) #
    task3 = Task(task_id="Task3", duration=1, dependencies=["Task2"])
    schedular = TaskSchedular(max_parallel_task=3)
    schedular.add_task(task1)
    schedular.add_task(task2)
    schedular.add_task(task3)

    schedular.schedule_tasks()
