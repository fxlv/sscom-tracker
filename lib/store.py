from abc import ABC, abstractmethod


class Store(ABC):
    """Abstract class that defines the interface for storage."""

    @abstractmethod
    def write_classified(self, data):
        pass
