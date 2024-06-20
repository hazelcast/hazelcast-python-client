import dataclasses
import enum
from typing import Any, Dict, List, Optional, Union


__all__ = "Document", "Vector", "IndexConfig", "SearchResult"


class Type(enum.IntEnum):
    DENSE = 0


VectorType = Type


@dataclasses.dataclass
class Vector:
    name: str
    type: Type
    vector: List[float]


VectorPair = Vector


class Document:
    def __init__(self, value: Any, vectors: Union[Vector, List[Vector]]) -> None:
        self.value = value
        if isinstance(vectors, Vector):
            self.vectors = [vectors]
        else:
            self.vectors = vectors

    @property
    def vector(self) -> Optional[Vector]:
        if len(self.vectors) == 0:
            return None
        return self.vectors[0]

    def __copy__(self):
        return Document(self.value, self.vectors)

    def __repr__(self):
        return f"Vector<value={self.value}, vectors=self.vectors}>"


VectorDocument = Document


@dataclasses.dataclass
class SearchResult:

    key: Any
    value: Any
    score: float
    vectors: List[VectorPair]


VectorSearchResult = SearchResult


class Metric(enum.IntEnum):

    EUCLIDEAN = 0
    COSINE = 1
    DOT = 2


@dataclasses.dataclass
class IndexConfig:

    name: str
    metric: Metric
    dimension: int
    max_degree: int = 200
    ef_construction: int = 300
    use_deduplication: bool = False


VectorIndexConfig = IndexConfig


@dataclasses.dataclass
class VectorSearchOptions:

    include_value: bool
    include_vectors: bool
    limit: int
    hints: Dict[str, str] = dataclasses.field(default_factory=lambda: {})
