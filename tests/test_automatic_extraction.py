from pathlib import Path

import pytest
from pydantic import ValidationError

from src.brd_parser.document_reader import BrdDocument, BrdLine
from src.llm.provider import EchoProvider
from src.orchestrator.automatic_runner import AutomaticExtractionAgent
from src.orchestrator.automatic_runner import AutomaticProposal


def test_automatic_agent_rejects_invented_physical_tables(legacy_config):
    agent = AutomaticExtractionAgent(EchoProvider(), legacy_config)
    payload = {
        "dataset_00": "orders_00",
        "dataset_01": "orders_01",
        "tables": [{
            "source_name": "orders",
            "table_00": "00_orders",
            "table_01": "01_orders",
            "columns": [{"name": "order_id", "type": "STRING"}],
        }],
    }
    document = BrdDocument(Path("test.txt"), [BrdLine(0, "Orders order_id STRING")])
    with pytest.raises(ValueError, match="do not invent"):
        agent._compile(payload, document, "2026_10")


def test_automatic_agent_rejects_assumptions_and_context_truncation(legacy_config):
    agent = AutomaticExtractionAgent(EchoProvider(), legacy_config, max_document_chars=5)
    with pytest.raises(ValueError, match="unresolved"):
        agent._compile({"assumptions": [{"topic": "invented"}]}, BrdDocument(Path("test.txt")), "2026_10")
    with pytest.raises(ValueError, match="partial extraction"):
        agent._document_text(BrdDocument(Path("test.txt"), [BrdLine(0, "long requirements")]))


def test_automatic_agent_preserves_required_column_and_rejects_missing_type(legacy_config):
    agent = AutomaticExtractionAgent(EchoProvider(), legacy_config)
    column = agent._compile_column({"name": "order_id", "type": "STRING", "nullable": False})
    assert column.nullable is False
    with pytest.raises(ValueError, match="explicit BigQuery type"):
        agent._compile_column({"name": "order_id"})


def test_automatic_proposal_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        AutomaticProposal.model_validate({
            "tables": [{
                "source_name": "orders", "table_00": "00_orders", "table_01": "01_orders",
                "columns": [{"name": "id", "type": "STRING", "nullable": "false"}],
                "execute_sql": "unexpected",
            }],
        })
