import dataclasses
import enum
from typing import Any, Dict, List, Optional, Union


__all__ = "Document", "Vector", "IndexConfig", "SearchResult"


class Type(enum.IntEnum):
    DENSE = 0
    """
    Represents a vector defined by a list of floats.
    """


VectorType = Type


@dataclasses.dataclass
class Vector:
    """Vector represents a named vector of floats.

    Args:
        name: Name of the vector.
            To use the default vector index, name must be set
            to the blank string (``""``)
        type: Type of the vector.
            Currently only ``Type.DENSE`` is supported.
        vector: The vector of floats specified as a list of floats.
    """

    name: str
    type: Type
    vector: List[float]


VectorPair = Vector


class Document:
    """Document represents a value and associated vectors.

    Args:
        value: The value associated with this Document.
        vectors: Either one ``Vector`` instance or a list of ``Vector`` instances associated with this Document.
    """

    def __init__(self, value: Any, vectors: Union[Vector, List[Vector]]) -> None:
        self.value = value
        if isinstance(vectors, Vector):
            self.vectors = [vectors]
        else:
            self.vectors = vectors

    @property
    def vector(self) -> Optional[Vector]:
        """Returns the vector associated with this Document

        It returns ``None`` if no vector is associated.
        It returns the first vector if one or more vectors were associated.
        """
        if len(self.vectors) == 0:
            return None
        return self.vectors[0]

    def __copy__(self):
        return Document(self.value, self.vectors)

    def __repr__(self):
        return f"Document<value={self.value}, vectors={self.vectors}>"


VectorDocument = Document


@dataclasses.dataclass
class SearchResult:
    """SearchResult contains one of the results from a vector search.

    Args:
      key: The ``key`` set for the found Document.
      value: The value of the found Document.
      score: A numeric value that shows the similarity of the found Document
        to the reference vector. The score will be in the [0, 1] range.
        The score gets higher when the found Document is more similar to the
        reference vector.
    """

    key: Any
    value: Any
    score: float
    vectors: List[VectorPair]


VectorSearchResult = SearchResult


class Metric(enum.IntEnum):
    """Metric is the similarity metric to use for indexing a vector index."""

    EUCLIDEAN = 0
    COSINE = 1
    DOT = 2


@dataclasses.dataclass
class IndexConfig:
    """The IndexConfig contains configuration for a vector index.

    Args:
        name: Name of the vector index.
        metric: The metric to be used for the index configuration.
        dimension: The dimension of vectors to be used in the vector index.
            All vectors that use this index configuration must have
            the same dimension.
        max_degree: The maximum number of connections allowed per node.
        ef_construction: The size of the dynamic list for search.
        use_deduplication: Enable deduplication in the index.
    """

    name: str
    metric: Metric
    dimension: int
    max_degree: int = 16
    ef_construction: int = 100
    use_deduplication: bool = True


VectorIndexConfig = IndexConfig


@dataclasses.dataclass
class VectorSearchOptions:
    """VectorSearchOptions contains search configuration options.

    This class is not meant to be utilized by the user code.
    """

    include_value: bool
    include_vectors: bool
    limit: int
    hints: Dict[str, str] = dataclasses.field(default_factory=lambda: {})
