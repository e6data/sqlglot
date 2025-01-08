# rules/base_rule.py

from abc import ABC, abstractmethod
from typing import Dict, Any, List


class Rule(ABC):
    def __init__(self, rule_id: str, description: str, severity: str, conditions: Dict[str, Any]):
        self.rule_id = rule_id
        self.description = description
        self.severity = severity
        self.conditions = conditions

    @abstractmethod
    def validate(self, query: Dict[str, Any], table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass
