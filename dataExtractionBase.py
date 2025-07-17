from abc import abstractmethod, ABC

class ExtractionBase(ABC):

    @abstractmethod
    def connectToDataSource(self, *args, **kwargs):
        pass

    @abstractmethod
    def getData(self):
        pass

    def run(self, *args, **kwargs):
        return self.getData(*args, **kwargs)