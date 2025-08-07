from abc import ABC, abstractmethod

class ChunkerBase(ABC):
    @abstractmethod
    def chunkData(self, *args, **kwargs):
        pass

    def run(self, *args, **kwargs):
        return self.chunkData(*args, **kwargs)






class ChunkerBase(ABC):
    @abstractmethod
    def chunkData(self, *args, **kwargs):
        pass

    def run(self, *args, **kwargs):
        return self.chunkData(*args, **kwargs)



class ChunkerBase(ABC):
    @abstractmethod
    def chunkData(self, *args, **kwargs):
        pass

    def run(self, *args, **kwargs):
        return self.chunkData(*args, **kwargs)
