from abc import ABCMeta, abstractclassmethod

class JobExecutor(metaclass=ABCMeta):
    @abstractclassmethod
    def execute(self):
        raise NotImplementedError(
            "JobExecutorにはexecuteメソッドを実装してください!")
