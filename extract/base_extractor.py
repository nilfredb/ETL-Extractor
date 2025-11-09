from abc import ABC, abstractmethod
from typing import Any

class IExtractor(ABC):
    @abstractmethod
    def extract(self) -> Any:
        ...