from abc import ABC, abstractmethod
from lib.datastructures import Classified


class Store(ABC):
    """Abstract class that defines the interface for storage."""

    @abstractmethod
    def write_classified(self, data):
        pass


class ObjectStore(Store):
    @abstractmethod
    def get_classified_count(self, category) -> int:
        pass

    @abstractmethod
    def get_classified(self, data) -> Classified:
        pass

    @abstractmethod
    def get_classified_by_category_hash(self, category, hash_string) -> Classified:
        pass

    @abstractmethod
    def get_all_classifieds(self, data) -> list:
        pass

    @abstractmethod
    def write_classified(self, data):
        pass

    @abstractmethod
    def update_classified(self, data):
        pass
