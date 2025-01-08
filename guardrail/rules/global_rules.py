# rules/global_rules.py

from .base_rule import Rule
from typing import Dict, Any, List


class LimitCheckRule(Rule):
    def validate(self, query: Dict[str, Any], table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations = []
        if self.conditions.get("limits_required") and not query.get("limits"):
            violations.append(
                {
                    "rule_id": self.rule_id,
                    "description": self.description,
                    "severity": self.severity,
                    "details": "No LIMIT applied.",
                }
            )
        return violations


class WildcardUsageRule(Rule):
    def validate(self, query: Dict[str, Any], table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations = []
        columns = query.get("columns", [])
        if "*" in columns:
            max_cols = self.conditions.get("max_columns_with_wildcard", 10)
            if table_info.get("column_count", 0) > max_cols:
                violations.append(
                    {
                        "rule_id": self.rule_id,
                        "description": self.description,
                        "severity": self.severity,
                        "details": f"Wildcard '*' used on table with {table_info.get('column_count')} columns.",
                    }
                )
        return violations


class WherePartitionRule(Rule):
    def validate(self, query: Dict[str, Any], table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations = []
        if self.conditions.get("must_use_partitions"):
            partition_cols = table_info.get("partition_values", [])
            where_cols = query.get("where_columns", [])
            non_partition_cols = [col for col in where_cols if col not in partition_cols]
            if non_partition_cols:
                violations.append(
                    {
                        "rule_id": self.rule_id,
                        "description": self.description,
                        "severity": self.severity,
                        "details": f"WHERE clause contains non-partition columns: {', '.join(non_partition_cols)}.",
                    }
                )
        return violations


class LimitValueRule(Rule):
    def validate(self, query: Dict[str, Any], table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations = []
        max_limit = self.conditions.get("max_limit")
        if "limits" in query and query["limits"]:
            for limit in query["limits"]:
                try:
                    limit_val = int(limit)
                    if limit_val > max_limit:
                        violations.append(
                            {
                                "rule_id": self.rule_id,
                                "description": self.description,
                                "severity": self.severity,
                                "details": f"LIMIT value {limit_val} exceeds maximum allowed {max_limit}.",
                            }
                        )
                except ValueError:
                    violations.append(
                        {
                            "rule_id": self.rule_id,
                            "description": self.description,
                            "severity": self.severity,
                            "details": f"Invalid LIMIT value: {limit}.",
                        }
                    )
        return violations
