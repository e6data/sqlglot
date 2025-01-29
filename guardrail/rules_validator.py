# validator.py
import logging
from typing import List, Dict, Any
from guardrail.rules import (
    LimitCheckRule,
    WildcardUsageRule,
    WherePartitionRule,
    LimitValueRule,
    RequiredColumnRule,
    ForbiddenColumnRule,
)

# Configure logging
logging.basicConfig(
    filename="validator.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,  # Changed to DEBUG for detailed logs
)


def load_rules(rules_service) -> List[Any]:
    raw_rules = rules_service.fetch_rules()
    rule_objects = []
    for rule_def in raw_rules:
        rule_type = rule_def.get("type")
        scope = rule_def.get("scope", "global")
        applicable_tables = rule_def.get("applicable_tables", [])

        if scope == "global":
            if rule_type == "limit_check":
                rule = LimitCheckRule(
                    rule_id=rule_def["id"],
                    description=rule_def["description"],
                    severity=rule_def["severity"],
                    conditions=rule_def["conditions"],
                )
            elif rule_type == "wildcard_usage":
                rule = WildcardUsageRule(
                    rule_id=rule_def["id"],
                    description=rule_def["description"],
                    severity=rule_def["severity"],
                    conditions=rule_def["conditions"],
                )
            elif rule_type == "where_partition":
                rule = WherePartitionRule(
                    rule_id=rule_def["id"],
                    description=rule_def["description"],
                    severity=rule_def["severity"],
                    conditions=rule_def["conditions"],
                )
            elif rule_type == "limit_value":
                rule = LimitValueRule(
                    rule_id=rule_def["id"],
                    description=rule_def["description"],
                    severity=rule_def["severity"],
                    conditions=rule_def["conditions"],
                )
            else:
                logging.warning(
                    f"Unknown global rule type: {rule_type}. Skipping rule ID: {rule_def['id']}"
                )
                continue

        elif scope == "table_specific":
            if rule_type == "required_column":
                rule = RequiredColumnRule(
                    rule_id=rule_def["id"],
                    description=rule_def["description"],
                    severity=rule_def["severity"],
                    applicable_tables=applicable_tables,
                    conditions=rule_def["conditions"],
                )
            elif rule_type == "forbidden_column":
                rule = ForbiddenColumnRule(
                    rule_id=rule_def["id"],
                    description=rule_def["description"],
                    severity=rule_def["severity"],
                    applicable_tables=applicable_tables,
                    conditions=rule_def["conditions"],
                )
            else:
                logging.warning(
                    f"Unknown table-specific rule type: {rule_type}. Skipping rule ID: {rule_def['id']}"
                )
                continue
        else:
            logging.warning(f"Unknown rule scope: {scope}. Skipping rule ID: {rule_def['id']}")
            continue

        rule_objects.append(rule)
    logging.info(f"Loaded {len(rule_objects)} rules from Rules Service.")
    return rule_objects


def validate_queries_dynamic(
    queries: List[Dict[str, Any]], table_map: Dict[str, Any], rules: List[Any]
) -> List[Dict[str, Any]]:
    violations = []

    for idx, query in enumerate(queries, start=1):
        table = query.get("table")
        logging.info(f"Validating Query {idx} on table '{table}'.")

        if table not in table_map:
            violation = {
                "query_index": idx,
                "table": table,
                "rule_id": "TABLE_NOT_FOUND",
                "description": f"Table '{table}' not found in table_map.",
                "severity": "high",
                "details": "Invalid table reference.",
            }
            logging.error(f"Violation: {violation}")
            violations.append(violation)
            continue

        table_info = table_map[table]

        for rule in rules:
            # For table-specific rules, ensure the rule applies to the current table
            if isinstance(rule, (RequiredColumnRule, ForbiddenColumnRule)):
                if table not in rule.applicable_tables:
                    continue  # Skip this rule for tables it's not applicable to
            # Apply the rule
            rule_violations = rule.validate(query, table_info)
            for v in rule_violations:
                violation = {
                    "query_index": idx,
                    "table": table,
                    "rule_id": v["rule_id"],
                    "description": v["description"],
                    "severity": v["severity"],
                    "details": v["details"],
                }
                logging.warning(f"Violation: {violation}")
                violations.append(violation)

    return violations


## OLD SHIT USE THE NEW RULE VALIDATOR
def validate_queries(queries, table_map, guardrail_configs):
    violations = []

    for idx, query in enumerate(queries, start=1):
        table = query["table"]
        columns = query["columns"]
        limits = query["limits"]
        where_columns = query["where_columns"]

        # TODO:: Need to cross check the configurations. Cross check it,
        #  it will depend upon the data-structure which will be passed by platform team
        limit_guardrail = guardrail_configs.get("limit", {})
        number_of_rows = limit_guardrail.get("number_of_rows")
        limit_action = limit_guardrail.get("limit_action", "warn")

        select_star = guardrail_configs.get("select_star", {})
        number_of_columns = select_star.get("number_of_columns")
        select_star_action = select_star.get("select_star_action", "warn")

        partition_columns = guardrail_configs.get("partition_columns", {})
        partition_columns_action = partition_columns.get("partition_columns_action", "warn")

        # Retrieve table metadata
        if table not in table_map:
            violations.append(
                {
                    "query_index": idx,
                    "table": table,
                    "violation": f"Table '{table}' not found in table_map.",
                }
            )
            continue

        table_info = table_map[table]
        column_count = table_info["column_count"]
        partition_values = table_info["partition_values"]

        if limit_guardrail:
            # Rule 1: Check for LIMIT violation
            if not limits:
                # TODO @Shreyas/@adithya: Use the `number_of_rows` information and then call the violation.
                violations.append(
                    {
                        "query_index": idx,
                        "table": table,
                        "violation": "No LIMIT applied.",
                        "action": limit_action,
                    }
                )
        if select_star:
            # Rule 2: Check for wildcard (*) usage violation
            if "*" in columns:
                # Assuming columns list is empty when '*' is used
                # TODO @Shreyas/@adithya: Use the `number_of_columns` information and then call the violation.
                if column_count > 10:
                    violations.append(
                        {
                            "query_index": idx,
                            "table": table,
                            "violation": f"Wildcard '*' used on table with {column_count} columns.",
                            "action": select_star_action,
                        }
                    )
        if partition_columns:
            # Rule 3: Check for WHERE clause on non-partition columns
            non_partition_columns = [col for col in where_columns if col not in partition_values]
            if non_partition_columns:
                violations.append(
                    {
                        "query_index": idx,
                        "table": table,
                        "violation": f"WHERE clause contains non-partition columns: {', '.join(non_partition_columns)}.",
                        "action": partition_columns_action,
                    }
                )

    return violations


# Run validation
