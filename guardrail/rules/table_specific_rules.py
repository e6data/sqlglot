# rules/table_specific_rules.py

from .base_rule import Rule
from typing import Dict, Any, List
import logging

class RequiredColumnRule(Rule):
    def __init__(self, rule_id: str, description: str, severity: str, applicable_tables: List[str], conditions: Dict[str, Any]):
        super().__init__(rule_id, description, severity, conditions)
        self.applicable_tables = applicable_tables
        self.required_columns = conditions.get('required_columns', [])

    def validate(self, query: Dict[str, Any], table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations = []
        table = query.get('table')
        if table in self.applicable_tables:
            columns = query.get('columns', [])
            # Handle wildcard '*' by assuming all columns are selected
            if '*' in columns:
                logging.debug(f"Wildcard '*' detected in Query for table '{table}'. No violation for required columns.")
                return violations
            missing_columns = [col for col in self.required_columns if col not in columns]
            # Debug logs
            logging.debug(f"Validating RequiredColumnRule for table '{table}':")
            logging.debug(f"  Required Columns: {self.required_columns}")
            logging.debug(f"  Query Columns: {columns}")
            logging.debug(f"  Missing Columns: {missing_columns}")
            if missing_columns:
                violations.append({
                    'rule_id': self.rule_id,
                    'description': self.description,
                    'severity': self.severity,
                    'details': f"Missing required columns: {', '.join(missing_columns)}."
                })
        return violations

class ForbiddenColumnRule(Rule):
    def __init__(self, rule_id: str, description: str, severity: str, applicable_tables: List[str], conditions: Dict[str, Any]):
        super().__init__(rule_id, description, severity, conditions)
        self.applicable_tables = applicable_tables
        self.forbidden_columns = conditions.get('forbidden_columns', [])

    def validate(self, query: Dict[str, Any], table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations = []
        table = query.get('table')
        if table in self.applicable_tables:
            columns = query.get('columns', [])
            # Handle wildcard '*' by assuming all columns are selected, including forbidden ones
            if '*' in columns:
                violations.append({
                    'rule_id': self.rule_id,
                    'description': self.description,
                    'severity': self.severity,
                    'details': f"Wildcard '*' used, which includes forbidden columns: {', '.join(self.forbidden_columns)}."
                })
                logging.debug(f"ForbiddenColumnRule triggered by wildcard '*' in Query for table '{table}'.")
                return violations
            used_forbidden = [col for col in self.forbidden_columns if col in columns]
            if used_forbidden:
                violations.append({
                    'rule_id': self.rule_id,
                    'description': self.description,
                    'severity': self.severity,
                    'details': f"Forbidden columns used: {', '.join(used_forbidden)}."
                })
                logging.debug(f"ForbiddenColumnRule triggered by columns {used_forbidden} in Query for table '{table}'.")
        return violations
