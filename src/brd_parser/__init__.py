"""BRD ingestion: read documents, extract requirements, validate the spec."""

from .document_reader import BrdDocument, BrdLine, read_document
from .requirement_extractor import RequirementExtractor, extract_pipeline_spec
from .spec_validator import validate_spec

__all__ = [
    "BrdDocument",
    "BrdLine",
    "read_document",
    "RequirementExtractor",
    "extract_pipeline_spec",
    "validate_spec",
]
