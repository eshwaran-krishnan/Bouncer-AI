"""
Bouncer file-reading tools.

Each function returns a structured dict the LLM agent can reason over.
They are plain Python functions — wrap with @tool (LangChain) or
convert to Anthropic tool schemas at the node/graph layer.

Supported formats:
  Text  : CSV, TSV, JSON, PDF, YAML
  Binary: FCS (flow cytometry), EDS (QuantStudio qPCR)
  Meta  : peek_file (classification / preview)
"""

from bouncer.agent.tools.peek_file import peek_file
from bouncer.agent.tools.read_csv import read_csv
from bouncer.agent.tools.read_json import read_json
from bouncer.agent.tools.read_pdf import read_pdf
from bouncer.agent.tools.read_yaml import read_yaml
from bouncer.agent.tools.read_fcs import read_fcs
from bouncer.agent.tools.read_eds import read_eds

__all__ = [
    "peek_file",
    "read_csv",
    "read_json",
    "read_pdf",
    "read_yaml",
    "read_fcs",
    "read_eds",
]
