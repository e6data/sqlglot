from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    binary_from_function,
    date_trunc_to_time,
    build_formatted_time,
    max_or_greatest,
    min_or_least,
    rename_func,
    timestrtotime_sql,
    datestrtodate_sql,
    trim_sql,
)
from sqlglot.helper import is_float, is_int, seq_get, apply_index_offset, flatten
from sqlglot.tokens import TokenType
from sqlglot.parser import build_coalesce
from typing import Any, Optional

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def _to_int(expression: exp.Expression) -> exp.Expression:
    if not expression.type:
        from sqlglot.optimizer.annotate_types import annotate_types

        annotate_types(expression)
    if expression.type and expression.type.this not in exp.DataType.INTEGER_TYPES:
        return exp.cast(expression, to=exp.DataType.Type.BIGINT)
    return expression


def _build_date(args: t.List[exp.Expression]) -> exp.Expression:
    this = seq_get(args, 0)
    return exp.Date(this=this)


def _build_datetime(
    name: str, kind: exp.DataType.Type, safe: bool = False
) -> t.Callable[[t.List], exp.Func]:
    """
    This function creates a builder that handles various scenarios for converting or parsing DATETIME values in
    SQL expressions. It’s particularly useful for SQL dialect conversions or transpiling SQL queries from
    one dialect to another.

    :param name: The name of the SQL function (e.g., TO_TIME or TO_DATE).
    :param kind: The type of the target data (e.g., TIMESTAMP, DATE), represented by exp.DataType.Type.
    :param safe: A boolean flag indicating whether to use a “safe” mode for the resulting expression.

    :return: A callable function that takes a list of arguments (args) and returns an SQL expression (exp.Func)

    """

    def _builder(args: t.List) -> exp.Func:
        """
        This is the main logic that processes the arguments and constructs the SQL expression.

        :param args: A list of arguments passed to the SQL function.
        :return: An SQL expression (exp.Func) based on the arguments and the target data type.
        """
        # Retrieve the first argument from the list, if available.
        value = seq_get(args, 0)

        # Determine if the argument is an integer literal.
        int_value = value is not None and is_int(value.name)

        # Handle cases where the argument is a literal value.
        if isinstance(value, exp.Literal):
            # If there's only one string argument and it's not an integer,
            # cast it to the target data type (e.g., CAST('01:02:03' AS TIMESTAMP)).
            if len(args) == 1 and value.is_string and not int_value:
                return exp.cast(value, kind)

            # Handle cases where the target type is TIMESTAMP.
            if kind == exp.DataType.Type.TIMESTAMP:
                # If the value is an integer, interpret it as a Unix timestamp.
                if int_value:
                    return exp.UnixToTime(this=value, scale=seq_get(args, 1))

                # If the value is not a float, format it into a standard datetime expression.
                if not is_float(value.this):
                    return build_formatted_time(exp.StrToTime, "snowflake")(args)

        # Handle cases where the target type is DATE and the value is not an integer.
        if kind == exp.DataType.Type.DATE and not int_value:
            # Format the expression using a helper function to create a TsOrDsToDate expression.
            formatted_exp = build_formatted_time(exp.TsOrDsToDate, "E6")(args)

            # Set the "safe" flag on the resulting expression if specified.
            formatted_exp.set("safe", safe)
            return formatted_exp

        # Default case: Return a generic SQL function with the given name and arguments.
        return exp.Anonymous(this=name, expressions=args)

    # Return the builder function, which can be called with arguments to construct SQL expressions.
    return _builder


def _build_timestamp(args: t.List[exp.Expression]) -> exp.Expression:
    this = seq_get(args, 0)
    return exp.Timestamp(this=this)


def _build_with_arg_as_text(
    klass: t.Type[exp.Expression],
) -> t.Callable[[t.List[exp.Expression]], exp.Expression]:
    def _parse(args: t.List[exp.Expression]) -> exp.Expression:
        this = seq_get(args, 0)

        if this and not this.is_string:
            this = exp.cast(this, exp.DataType.Type.TEXT)

        expression = seq_get(args, 1)
        kwargs = {"this": this}

        if expression:
            kwargs["expression"] = expression

        return klass(**kwargs)

    return _parse


def _build_from_unixtime_withunit(args: t.List[exp.Expression]) -> exp.Func:
    this = seq_get(args, 0)
    unit = seq_get(args, 1)

    # if unit is None or unit.this.lower() not in {'seconds', 'milliseconds'}:
    #     raise ValueError(f"Unsupported unit for FROM_UNIXTIME_WITHUNIT: {unit if unit else 'Nothing'}")

    return exp.UnixToTime(this=this, scale=unit)


def _build_formatted_time_with_or_without_zone(
    exp_class: t.Type[E], default: t.Optional[bool | str] = None
) -> t.Callable[[t.List], E]:
    """Helper used for time expressions with optional time zone.

    Args:
        exp_class: the expression class to instantiate.
        dialect: target sql dialect.
        default: the default format, True being time.

    Returns:
        A callable that can be used to return the appropriately formatted time expression with zone.
    """

    def _builder(args: t.List):
        if len(args) == 2:
            return exp_class(
                this=seq_get(args, 1),
                format=format_time_for_parsefunctions(
                    seq_get(args, 0) or (E6().TIME_FORMAT if default is True else default or None)
                ),
            )
        return exp_class(
            this=seq_get(args, 1),
            format=format_time_for_parsefunctions(
                seq_get(args, 0) or (E6().TIME_FORMAT if default is True else default or None)
            ),
            zone=seq_get(args, 2),
        )

    return _builder


def build_datediff(expression_class: t.Type[E]) -> t.Callable[[t.List], E]:
    def _builder(args: t.List) -> E:
        # If there are only two arguments, assume the unit is 'day'
        if len(args) == 2:
            date_expr1 = args[0]
            date_expr2 = args[1]
            unit = exp.Literal.string("day")  # Default unit when not provided
        elif len(args) == 3:
            # Check if the first argument is a unit (which should be a string or a recognized type for units)
            if isinstance(args[0], exp.Literal) and args[0].is_string:
                unit = args[0]
                date_expr1 = args[2]
                date_expr2 = args[1]
            else:
                date_expr1 = args[0]
                date_expr2 = args[1]
                unit = args[2]  # Assume the third argument is a unit if not a recognized type
        else:
            raise ValueError("Incorrect number of arguments for DATEDIFF function")

        return expression_class(this=date_expr1, expression=date_expr2, unit=unit)

    return _builder


def _build_to_unix_timestamp(args: t.List[exp.Expression]) -> exp.Func:
    if len(args) == 2:
        return exp.Anonymous(this="TO_UNIX_TIMESTAMP", expressions=args)
    value = seq_get(args, 0)

    # If value is a string literal, cast it to TIMESTAMP
    if isinstance(value, exp.Literal) and value.is_string:
        value = exp.Cast(this=value, to=exp.DataType(this="TIMESTAMP"))

    # Check if the value is a cast to TIMESTAMP
    # if not (isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.TIMESTAMP)):
    #     raise ValueError("Argument for TO_UNIX_TIMESTAMP must be of type TIMESTAMP")

    return exp.TimeToUnix(this=value)


def _build_trim(args: t.List, is_left: bool = True):
    if len(args) < 2:
        return exp.Trim(
            this=seq_get(args, 0),
            position="LEADING" if is_left else "TRAILING",
        )
    else:
        return exp.Trim(
            this=seq_get(args, 1),
            expression=seq_get(args, 0),
            position="LEADING" if is_left else "TRAILING",
        )


def _build_convert_timezone(args: t.List) -> exp.Anonymous | exp.AtTimeZone:
    if len(args) == 3:
        return exp.Anonymous(this="CONVERT_TIMEZONE", expressions=args)
    return exp.AtTimeZone(this=seq_get(args, 1), zone=seq_get(args, 0))


def _build_datetime_for_DT(args: t.List) -> exp.AtTimeZone:
    if len(args) == 1:
        return exp.AtTimeZone(this=seq_get(args, 0), zone="UTC")
    return exp.AtTimeZone(this=seq_get(args, 0), zone=seq_get(args, 1))


def _parse_to_date(args: t.List) -> exp.TsOrDsToDate:
    date_expr = seq_get(args, 0)
    format_expr = seq_get(args, 1)
    if not format_expr:
        return exp.TsOrDsToDate(this=date_expr)
    return exp.TsOrDsToDate(this=date_expr, format=E6().convert_format_time(expression=format_expr))


def _parse_filter_array(args: t.List) -> exp.ArrayFilter:
    """
    Parses the FILTER_ARRAY function, ensuring that the lambda expression
    does not contain aggregate functions, as they are not supported in this context.

    TODO:: Need to discuss with Adithya, how we have tested this function & why we have passed `self` in the
            `seq_get` functions. It look incorrect to me
        I saw this type of pattern in methods of parser class. self is happening to be a list of args that are being sent.
         In order to get 1 arg, 2nd arg i am using `seq_get`. I donot know exactly why self is being list of two arguments. Need to deep dive on that.

    What This Function Does

        1.	Purpose:
            •	Parses a FILTER_ARRAY SQL function.
            •	Ensures that the lambda expression used in the FILTER_ARRAY does not contain aggregate functions, as these are not supported.
        2.	Key Logic:
            •	Extracts the array and lambda expressions from the arguments.
            •	Recursively checks if the lambda expression or its children contain any aggregate functions.
            •	Raises an error if aggregates are found; otherwise, it constructs and returns an ArrayFilter expression.
        3.	Why Check for Aggregates?
            •	Certain SQL dialects or use cases may not allow aggregate functions (e.g., SUM, MAX) inside lambda functions used in filter contexts. This ensures compliance with these rules.

    Example Usage

    Input SQL:

    FILTER_ARRAY(my_array, x -> x > 5)

    Process:
        1.	Extract Components:
            •	my_array is the array expression.
            •	x -> x > 5 is the lambda expression.
        2.	Check for Aggregates:
            •	The lambda expression (x > 5) is inspected to ensure it doesn’t include aggregate functions like SUM(x) or MAX(x).
        3.	Return Value:
            •	If no aggregates are found, the function returns an ArrayFilter object equivalent to:

    exp.ArrayFilter(this=my_array, expression=lambda_expr)

    """

    def contains_aggregate(node: exp.Expression) -> bool:
        """
        Recursively checks if the given node or any of its children
        contain an aggregate function.
        """
        if isinstance(node, exp.AggFunc):
            return True

        if not isinstance(node, exp.Expression):
            return False

        # Recursively check all child nodes.
        return any(contains_aggregate(child) for child in node.args.values())

    # Retrieve the array expression and the lambda expression from the arguments.
    array_expr = seq_get(args, 0)  # TODO:: Need to test this
    lambda_expr = seq_get(args, 1)  # TODO:: Need to test this

    if lambda_expr is None:
        raise ValueError("Lambda expression must be provided")

    # Get the root node of the lambda expression.
    root_node = lambda_expr.args.get("this")

    # If the lambda expression contains an aggregate function, raise a ValueError.
    if contains_aggregate(root_node):
        raise ValueError(
            "Lambda expressions in filter functions are not supported in 'IN' clause or on aggregate functions"
        )

    # Return an ArrayFilter expression with the parsed array and lambda expressions.
    return exp.ArrayFilter(this=array_expr, expression=lambda_expr)


def _build_regexp_count(args: t.List) -> exp.Anonymous | exp.RegexpCount:
    if len(args) == 2:
        return exp.RegexpCount(this=seq_get(args, 0), expression=seq_get(args, 1))
    else:
        return exp.Anonymous(this="REGEXP_COUNT", expressions=args)


def format_time_for_parsefunctions(expression):
    format_str = expression.this if isinstance(expression, exp.Literal) else expression
    for key, value in E6().TIME_MAPPING_FOR_PARSE_FUNCTIONS.items():
        format_str = format_str.replace(key, value)
    return format_str


def add_single_quotes(expression) -> str:
    quoted_str = f"'{expression}'"
    return quoted_str


def unit_to_str(expression: exp.Expression, default: str = "DAY") -> t.Optional[exp.Expression]:
    unit = expression.args.get("unit")

    if isinstance(unit, exp.Placeholder):
        return unit
    if unit:
        unit_name_new = unit.name.replace("SQL_TSI_", "")
        return exp.Literal.string(unit_name_new)
    return exp.Literal.string(default) if default else None


def _trim_sql(self: E6.Generator, expression: exp.Trim) -> str:
    target = self.sql(expression, "this")
    trim_type = self.sql(expression, "position")
    remove_chars = self.sql(expression, "expression")

    if trim_type.upper() == "LEADING":
        if remove_chars:
            return self.func("LTRIM", remove_chars, target)
        else:
            return self.func("LTRIM", target)
    elif trim_type.upper() == "TRAILING":
        if remove_chars:
            return self.func("RTRIM", remove_chars, target)
        else:
            return self.func("RTRIM", target)

    else:
        return trim_sql(self, expression)


def build_locate_strposition(args: t.List):
    if len(args) == 2:
        return exp.StrPosition(
            this=seq_get(args, 1),
            substr=seq_get(args, 0),
        )
    return exp.StrPosition(
        this=seq_get(args, 1),
        substr=seq_get(args, 0),
        position=seq_get(args, 2),
    )


class E6(Dialect):
    """
    The E6 Dialect for SQLGlot, customized for specific SQL syntax and behavior.
    This class defines strategies, mappings, and tokenization rules unique to the E6 dialect.
    """

    PRESERVE_ORIGINAL_NAMES = True

    # Strategy to normalize keywords: Here, keywords will be converted to lowercase.
    NORMALIZATION_STRATEGY = NormalizationStrategy.LOWERCASE

    # Define the offset for array indexing, starting from 1 instead of the default 0.
    INDEX_OFFSET = 1

    # Mapping for time formatting tokens, converting dialect-specific formats to Python-compatible ones.
    TIME_MAPPING = {
        "y": "%Y",  # Year as a four-digit number
        "Y": "%Y",  # Same as above
        "YYYY": "%Y",  # Four-digit year
        "yyyy": "%Y",  # Same as above
        "YY": "%y",  # Two-digit year
        "yy": "%y",  # Same as above
        "MMMM": "%B",  # Full month name
        "MMM": "%b",  # Abbreviated month name
        "MM": "%m",  # Two-digit month
        "M": "%-m",  # Single-digit month
        "dd": "%d",  # Two-digit day
        "d": "%-d",  # Single-digit day
        "HH": "%H",  # Two-digit hour (24-hour clock)
        "H": "%-H",  # Single-digit hour (24-hour clock)
        "hh": "%I",  # Two-digit hour (12-hour clock)
        "h": "%-I",  # Single-digit hour (12-hour clock)
        "mm": "%M",  # Two-digit minute
        "m": "%-M",  # Single-digit minute
        "ss": "%S",  # Two-digit second
        "s": "%-S",  # Single-digit second
        "E": "%a",  # Abbreviated weekday name
        "D": "%-j",
        "DD": "%j",
    }

    # Time mapping specific to parsing functions. This maps time format tokens from E6 to standard Python time formats.
    TIME_MAPPING_FOR_PARSE_FUNCTIONS = {
        "%Y": "%Y",
        "%y": "%y",
        "%m": "%m",
        "%d": "%d",
        "%e": "%e",
        "%H": "%H",
        "%k": "%k",
        "%I": "%h",
        "%S": "%S",
        "%s": "%s",
        "%f": "%f",
        "%b": "%b",
        "%M": "%i",
        "%a": "%a",
        "%W": "%W",
        "%j": "%j",
        "%i": "%i",
        "%r": "%r",
        "%T": "%T",
        "%v": "%v",
        "%x": "%x",
        "%%": "%%",
    }

    # Mapping units to SQL-compatible representations.
    UNIT_PART_MAPPING = {
        "'milliseconds'": "MILLISECOND",
        "'millisecond'": "MILLISECOND",
        "'s'": "SECOND",
        "'sec'": "SECOND",
        "'secs'": "SECOND",
        "'seconds'": "SECOND",
        "'second'": "SECOND",
        "'m'": "MINUTE",
        "'min'": "MINUTE",
        "'mins'": "MINUTE",
        "'minutes'": "MINUTE",
        "'minute'": "MINUTE",
        "'h'": "HOUR",
        "'hr'": "HOUR",
        "'hrs'": "HOUR",
        "'hours'": "HOUR",
        "'hour'": "HOUR",
        "'days'": "DAY",
        "'d'": "DAY",
        "'day'": "DAY",
        "'mon'": "MONTH",
        "'mons'": "MONTH",
        "'months'": "MONTH",
        "'month'": "MONTH",
        "'y'": "YEAR",
        "'yrs'": "YEAR",
        "'yr'": "YEAR",
        "'years'": "YEAR",
        "'year'": "YEAR",
        "'w'": "WEEK",
        "'weeks'": "WEEK",
        "'week'": "WEEK",
        "'qtr'": "QUARTER",
        "'quarter'": "QUARTER",
    }

    def convert_format_time(
        self, expression: t.Optional[exp.Literal | exp.Expression]
    ) -> t.Optional[str]:
        """
        Converts a time format string from one dialect's representation to another using the TIME_MAPPING.

        Args:
            expression (Optional[exp.Literal, exp.Expression]): The expression containing the time format string.
                - If it's a Literal, the format string is directly accessible via `expression.this`.
                - If it's another Expression, the format string is accessed through the 'format' argument.

        Returns:
            Optional[str]: The converted time format string, or None if the format string is not found.
        """
        # Determine the format string based on the type of expression
        # TODO:: Need to understand what is this Literal and what is expression
        #  # Yes we require this. Cuz for some cases what happens is the format arg will be a literal node and can be accessed using expression.this.
        #  # But this is not the case for all. For some functions what happens is the format part comes as `format` arg but not as `this`, this is due to declarations of those functions in other dialects
        #  # So as to acknowledge both cases we need this if.
        if expression is None:
            return None
        if isinstance(expression, exp.Literal):
            # For Literal expressions, retrieve the format string directly
            format_str = expression.this
        else:
            # For other expressions, retrieve the 'format' argument
            format_expr = expression.args.get("format")
            # Attempt to get the name attribute; if not present, use the expression itself
            format_str = getattr(format_expr, "name", format_expr)

        # If no format string is found, return None
        if format_str is None:
            return None

        # Initialize the format string to be transformed
        transformed_format = format_str.replace("%%", "%")

        # Iterate over the TIME_MAPPING to replace source formats with target formats
        for source_format, target_format in self.TIME_MAPPING.items():
            transformed_format = transformed_format.replace(target_format, source_format)

        return transformed_format

    def quote_identifier(self, expression: E, identify: bool = False) -> E:
        """
        Ensures SQL identifiers are quoted if they conflict with reserved keywords or require explicit quoting.

        Args:
            expression (exp.Expression): The SQL expression to check and possibly quote.
            identify (bool): A flag to force quoting identifiers (default is False).

        Returns:
            exp.Expression: The modified expression with identifiers quoted if necessary.
        """

        keywords_to_quote = {
            "ABS",
            "ABSENT",
            "ABSOLUTE",
            "ACTION",
            "ADA",
            "ADD",
            "ADMIN",
            "AFTER",
            "ALL",
            "ALLOCATE",
            "ALLOW",
            "ALTER",
            "ALWAYS",
            "AND",
            "ANY",
            "APPLY",
            "ARE",
            "ARRAY",
            "ARRAY_AGG",
            "ARRAY_CONCAT_AGG",
            "ARRAY_MAX_CARDINALITY",
            "AS",
            "ASC",
            "ASENSITIVE",
            "ASSERTION",
            "ASSIGNMENT",
            "ASYMMETRIC",
            "AT",
            "ATOMIC",
            "ATTRIBUTE",
            "ATTRIBUTES",
            "AUTHORIZATION",
            "AVG",
            "BEFORE",
            "BEGIN",
            "BEGIN_FRAME",
            "BEGIN_PARTITION",
            "BERNOULLI",
            "BETWEEN",
            "BIGINT",
            "BINARY",
            "BIT",
            "BLOB",
            "BOOLEAN",
            "BOTH",
            "BREADTH",
            "BY",
            "C",
            "CALL",
            "CALLED",
            "CARDINALITY",
            "CASCADE",
            "CASCADED",
            "CASE",
            "CAST",
            "CATALOG",
            "CATALOG_NAME",
            "CEIL",
            "CEILING",
            "CENTURY",
            "CHAIN",
            "CHAR",
            "CHAR_LENGTH",
            "CHARACTER",
            "CHARACTER_LENGTH",
            "CHARACTER_SET_CATALOG",
            "CHARACTER_SET_NAME",
            "CHARACTER_SET_SCHEMA",
            "CHARACTERISTICS",
            "CHARACTERS",
            "CHECK",
            "CLASSIFIER",
            "CLASS_ORIGIN",
            "CLOB",
            "CLOSE",
            "COALESCE",
            "COBOL",
            "COLLATE",
            "COLLATION",
            "COLLATION_CATALOG",
            "COLLATION_NAME",
            "COLLATION_SCHEMA",
            "COLLECT",
            "COLUMN",
            "COLUMN_NAME",
            "COMMAND_FUNCTION",
            "COMMAND_FUNCTION_CODE",
            "COMMIT",
            "COMMITTED",
            "CONDITION",
            "CONDITIONAL",
            "CONDITION_NUMBER",
            "CONNECT",
            "CONNECTION",
            "CONNECTION_NAME",
            "CONSTRAINT",
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_NAME",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINTS",
            "CONSTRUCTOR",
            "CONTAINS",
            "CONTINUE",
            "CONVERT",
            "CORR",
            "CORRESPONDING",
            "COUNT",
            "COVAR_POP",
            "COVAR_SAMP",
            "CREATE",
            "CROSS",
            "CUBE",
            "CUME_DIST",
            "CURRENT",
            "CURRENT_CATALOG",
            "CURRENT_DATE",
            "CURRENT_DEFAULT_TRANSFORM_GROUP",
            "CURRENT_PATH",
            "CURRENT_ROLE",
            "CURRENT_ROW",
            "CURRENT_SCHEMA",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
            "CURRENT_USER",
            "CURSOR",
            "CURSOR_NAME",
            "CYCLE",
            "DATA",
            "DATABASE",
            "DATE",
            "DATETIME_INTERVAL_CODE",
            "DATETIME_INTERVAL_PRECISION",
            "DAY",
            "DAYS",
            "DEALLOCATE",
            "DEC",
            "DECADE",
            "DECIMAL",
            "DECLARE",
            "DEFAULT_",
            "DEFAULTS",
            "DEFERRABLE",
            "DEFERRED",
            "DEFINE",
            "DEFINED",
            "DEFINER",
            "DEGREE",
            "DELETE",
            "DENSE_RANK",
            "DEPTH",
            "DEREF",
            "DERIVED",
            "DESC",
            "DESCRIBE",
            "DESCRIPTION",
            "DESCRIPTOR",
            "DETERMINISTIC",
            "DIAGNOSTICS",
            "DISALLOW",
            "DISCONNECT",
            "DISPATCH",
            "DISTINCT",
            "DOMAIN",
            "DOT_FORMAT",
            "DOUBLE",
            "DOW",
            "DOY",
            "DROP",
            "DYNAMIC",
            "DYNAMIC_FUNCTION",
            "DYNAMIC_FUNCTION_CODE",
            "EACH",
            "ELEMENT",
            "ELSE",
            "EMPTY",
            "ENCODING",
            "END",
            "END_EXEC",
            "END_FRAME",
            "END_PARTITION",
            "EPOCH",
            "EQUALS",
            "ERROR",
            "ESCAPE",
            "EVERY",
            "EXCEPT",
            "EXCEPTION",
            "EXCLUDE",
            "EXCLUDING",
            "EXEC",
            "EXECUTE",
            "EXISTS",
            "EXP",
            "EXPLAIN",
            "EXTEND",
            "EXTERNAL",
            "EXTRACT",
            "FALSE",
            "FETCH",
            "FILTER",
            "FINAL",
            "FIRST",
            "FIRST_VALUE",
            "FLOAT",
            "FLOOR",
            "FOLLOWING",
            "FOR",
            "FORMAT",
            "FOREIGN",
            "FORTRAN",
            "FOUND",
            "FRAC_SECOND",
            "FRAME_ROW",
            "FREE",
            "FROM",
            "FULL",
            "FUNCTION",
            "FUSION",
            "G",
            "GENERAL",
            "GENERATED",
            "GEOMETRY",
            "GET",
            "GLOBAL",
            "GO",
            "GOTO",
            "GRANT",
            "GRANTED",
            "GROUP",
            "GROUP_CONCAT",
            "GROUPING",
            "GROUPS",
            "HAVING",
            "HIERARCHY",
            "HOLD",
            "HOP",
            "HOUR",
            "HOURS",
            "IDENTITY",
            "IGNORE",
            "ILIKE",
            "IMMEDIATE",
            "IMMEDIATELY",
            "IMPLEMENTATION",
            "IMPORT",
            "IN",
            "INCLUDE",
            "INCLUDING",
            "INCREMENT",
            "INDICATOR",
            "INITIAL",
            "INITIALLY",
            "INNER",
            "INOUT",
            "INPUT",
            "INSENSITIVE",
            "INSERT",
            "INSTANCE",
            "INSTANTIABLE",
            "INT",
            "INTEGER",
            "INTERSECT",
            "INTERSECTION",
            "INTERVAL",
            "INTO",
            "INVOKER",
            "IS",
            "ISODOW",
            "ISOYEAR",
            "ISOLATION",
            "JAVA",
            "JOIN",
            "JSON",
            "JSON_ARRAY",
            "JSON_ARRAYAGG",
            "JSON_EXISTS",
            "JSON_OBJECT",
            "JSON_OBJECTAGG",
            "JSON_QUERY",
            "JSON_VALUE",
            "K",
            "KEY",
            "KEY_MEMBER",
            "KEY_TYPE",
            "LABEL",
            "LAG",
            "LANGUAGE",
            "LARGE",
            "LAST",
            "LAST_VALUE",
            "LATERAL",
            "LEAD",
            "LEADING",
            "LEFT",
            "LENGTH",
            "LEVEL",
            "LIBRARY",
            "LIKE",
            "LIKE_REGEX",
            "LIMIT",
            "TOP",
            "LN",
            "LOCAL",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "LOCATOR",
            "LOWER",
            "M",
            "MAP",
            "MATCH",
            "MATCHED",
            "MATCHES",
            "MATCH_NUMBER",
            "MATCH_RECOGNIZE",
            "MAX",
            "MAXVALUE",
            "MEASURES",
            "MEMBER",
            "MERGE",
            "MESSAGE_LENGTH",
            "MESSAGE_OCTET_LENGTH",
            "MESSAGE_TEXT",
            "METHOD",
            "MICROSECOND",
            "MILLISECOND",
            "MILLISECONDS",
            "MILLENNIUM",
            "MIN",
            "MINUTE",
            "MINUTES",
            "MINVALUE",
            "MOD",
            "MODIFIES",
            "MODULE",
            "MONTH",
            "MONTHS",
            "MORE_",
            "MULTISET",
            "MUMPS",
            "NAME",
            "NAMES",
            "NANOSECOND",
            "NATIONAL",
            "NATURAL",
            "NCHAR",
            "NCLOB",
            "NESTING",
            "NEW",
            "NEXT",
            "NO",
            "NONE",
            "NORMALIZE",
            "NORMALIZED",
            "NOT",
            "NTH_VALUE",
            "NTILE",
            "NULL",
            "NULLABLE",
            "NULLIF",
            "NULLS",
            "NUMBER",
            "NUMERIC",
            "OBJECT",
            "OCCURRENCES_REGEX",
            "OCTET_LENGTH",
            "OCTETS",
            "OF",
            "OFFSET",
            "OLD",
            "OMIT",
            "ON",
            "ONE",
            "ONLY",
            "OPEN",
            "OPTION",
            "OPTIONS",
            "OR",
            "ORDER",
            "ORDERING",
            "ORDINALITY",
            "OTHERS",
            "OUT",
            "OUTER",
            "OUTPUT",
            "OVER",
            "OVERLAPS",
            "OVERLAY",
            "OVERRIDING",
            "PAD",
            "PARAMETER",
            "PARAMETER_MODE",
            "PARAMETER_NAME",
            "PARAMETER_ORDINAL_POSITION",
            "PARAMETER_SPECIFIC_CATALOG",
            "PARAMETER_SPECIFIC_NAME",
            "PARAMETER_SPECIFIC_SCHEMA",
            "PARTIAL",
            "PARTITION",
            "PASCAL",
            "PASSING",
            "PASSTHROUGH",
            "PAST",
            "PATH",
            "PATTERN",
            "PER",
            "PERCENT",
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            "PERCENT_RANK",
            "PERIOD",
            "PERMUTE",
            "PIVOT",
            "PLACING",
            "PLAN",
            "PLI",
            "PORTION",
            "POSITION",
            "POSITION_REGEX",
            "POWER",
            "PRECEDES",
            "PRECEDING",
            "PRECISION",
            "PREPARE",
            "PRESERVE",
            "PREV",
            "PRIMARY",
            "PRIOR",
            "PRIVILEGES",
            "PROCEDURE",
            "PUBLIC",
            "QUARTER",
            "RANGE",
            "RANK",
            "READ",
            "READS",
            "REAL",
            "RECURSIVE",
            "REF",
            "REFERENCES",
            "REFERENCING",
            "REGR_AVGX",
            "REGR_AVGY",
            "REGR_COUNT",
            "REGR_INTERCEPT",
            "REGR_R2",
            "REGR_SLOPE",
            "REGR_SXX",
            "REGR_SXY",
            "REGR_SYY",
            "RELATIVE",
            "RELEASE",
            "REPEATABLE",
            "REPLACE",
            "RESET",
            "RESPECT",
            "RESTART",
            "RESTRICT",
            "RESULT",
            "RETURN",
            "RETURNED_CARDINALITY",
            "RETURNED_LENGTH",
            "RETURNED_OCTET_LENGTH",
            "RETURNED_SQLSTATE",
            "RETURNING",
            "RETURNS",
            "REVOKE",
            "RIGHT",
            "RLIKE",
            "ROLE",
            "ROLLBACK",
            "ROLLUP",
            "ROUTINE",
            "ROUTINE_CATALOG",
            "ROUTINE_NAME",
            "ROUTINE_SCHEMA",
            "ROW",
            "ROW_COUNT",
            "ROW_NUMBER",
            "ROWS",
            "RUNNING",
            "SAVEPOINT",
            "SCALAR",
            "SCALE",
            "SCHEMA",
            "SCHEMA_NAME",
            "SCOPE",
            "SCOPE_CATALOGS",
            "SCOPE_NAME",
            "SCOPE_SCHEMA",
            "SCROLL",
            "SEARCH",
            "SECOND",
            "SECONDS",
            "SECTION",
            "SECURITY",
            "SEEK",
            "SELECT",
            "SELF",
            "SENSITIVE",
            "SEPARATOR",
            "SEQUENCE",
            "SERIALIZABLE",
            "SERVER",
            "SERVER_NAME",
            "SESSION",
            "SESSION_USER",
            "SET",
            "SETS",
            "SET_MINUS",
            "SHOW",
            "SIMILAR",
            "SIMPLE",
            "SIZE",
            "SKIP_",
            "SMALLINT",
            "SOME",
            "SOURCE",
            "SPACE",
            "SPECIFIC",
            "SPECIFIC_NAME",
            "SPECIFICTYPE",
            "SQL",
            "SQLEXCEPTION",
            "SQLSTATE",
            "SQLWARNING",
            "SQL_BIGINT",
            "SQL_BINARY",
            "SQL_BIT",
            "SQL_BLOB",
            "SQL_BOOLEAN",
            "SQL_CHAR",
            "SQL_CLOB",
            "SQL_DATE",
            "SQL_DECIMAL",
            "SQL_DOUBLE",
            "SQL_FLOAT",
            "SQL_INTEGER",
            "SQL_INTERVAL_DAY",
            "SQL_INTERVAL_DAY_TO_HOUR",
            "SQL_INTERVAL_DAY_TO_MINUTE",
            "SQL_INTERVAL_DAY_TO_SECOND",
            "SQL_INTERVAL_HOUR",
            "SQL_INTERVAL_HOUR_TO_MINUTE",
            "SQL_INTERVAL_HOUR_TO_SECOND",
            "SQL_INTERVAL_MINUTE",
            "SQL_INTERVAL_MINUTE_TO_SECOND",
            "SQL_INTERVAL_MONTH",
            "SQL_INTERVAL_SECOND",
            "SQL_INTERVAL_YEAR",
            "SQL_INTERVAL_YEAR_TO_MONTH",
            "SQL_LONGVARBINARY",
            "SQL_LONGVARCHAR",
            "SQL_LONGVARNCHAR",
            "SQL_NCHAR",
            "SQL_NCLOB",
            "SQL_NUMERIC",
            "SQL_NVARCHAR",
            "SQL_REAL",
            "SQL_SMALLINT",
            "SQL_TIME",
            "SQL_TIMESTAMP",
            "SQL_TINYINT",
            "SQL_TSI_DAY",
            "SQL_TSI_FRAC_SECOND",
            "SQL_TSI_HOUR",
            "SQL_TSI_MICROSECOND",
            "SQL_TSI_MINUTE",
            "SQL_TSI_MONTH",
            "SQL_TSI_QUARTER",
            "SQL_TSI_SECOND",
            "SQL_TSI_WEEK",
            "SQL_TSI_YEAR",
            "SQL_VARBINARY",
            "SQL_VARCHAR",
            "SQRT",
            "START",
            "STATE",
            "STATEMENT",
            "STATIC",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "STREAM",
            "STRING_AGG",
            "STRUCTURE",
            "STYLE",
            "SUBCLASS_ORIGIN",
            "SUBMULTISET",
            "SUBSET",
            "SUBSTITUTE",
            "SUBSTRING",
            "SUBSTRING_REGEX",
            "SUCCEEDS",
            "SUM",
            "SYMMETRIC",
            "SYSTEM",
            "SYSTEM_TIME",
            "SYSTEM_USER",
            "TABLE",
            "TABLE_NAME",
            "TABLESAMPLE",
            "TEMPORARY",
            "THEN",
            "TIES",
            "TIME",
            "TIMESTAMP",
            "TIMESTAMP_TZ",
            "TIMESTAMPADD",
            "TIMESTAMPDIFF",
            "TIMEZONE_HOUR",
            "TIMEZONE_MINUTE",
            "TINYINT",
            "TO",
            "TOP_LEVEL_COUNT",
            "TRAILING",
            "TRANSACTION",
            "TRANSACTIONS_ACTIVE",
            "TRANSACTIONS_COMMITTED",
            "TRANSACTIONS_ROLLED_BACK",
            "TRANSFORM",
            "TRANSFORMS",
            "TRANSLATE",
            "TRANSLATE_REGEX",
            "TRANSLATION",
            "TREAT",
            "TRIGGER",
            "TRIGGER_CATALOG",
            "TRIGGER_NAME",
            "TRIGGER_SCHEMA",
            "TRIM",
            "TRIM_ARRAY",
            "TRUE",
            "TRUNCATE",
            "TRY_CAST",
            "TUMBLE",
            "TYPE",
            "UESCAPE",
            "UNBOUNDED",
            "UNCOMMITTED",
            "UNCONDITIONAL",
            "UNDER",
            "UNION",
            "UNIQUE",
            "UNKNOWN",
            "UNPIVOT",
            "UNNAMED",
            "UNNEST",
            "UPDATE",
            "UPPER",
            "UPSERT",
            "USAGE",
            "USER",
            "USER_DEFINED_TYPE_CATALOG",
            "USER_DEFINED_TYPE_CODE",
            "USER_DEFINED_TYPE_NAME",
            "USER_DEFINED_TYPE_SCHEMA",
            "USING",
            "UTF8",
            "UTF16",
            "UTF32",
            "VALUE",
            "VALUES",
            "VALUE_OF",
            "VAR_POP",
            "VAR_SAMP",
            "VARBINARY",
            "VARCHAR",
            "VARYING",
            "VERSION",
            "VERSIONING",
            "VIEW",
            "WEEK",
            "WHEN",
            "WHENEVER",
            "WHERE",
            "WIDTH_BUCKET",
            "WINDOW",
            "WITH",
            "WITHIN",
            "WITHOUT",
            "WORK",
            "WRAPPER",
            "WRITE",
            "XML",
            "YEAR",
            "YEARS",
            "ZONE",
        }

        # Check if the expression is an identifier and matches a reserved keyword.
        if isinstance(expression, exp.Identifier) and expression.name.upper() in keywords_to_quote:
            # Mark the identifier as quoted.
            expression.set("quoted", True)

        # Return the potentially modified expression.
        return expression

    class Tokenizer(tokens.Tokenizer):
        """
        The Tokenizer class is responsible for breaking down SQL statements into tokens.
        It processes the input SQL string character by character, grouping sequences into tokens that represent
        distinct syntactical elements. This tokenization is the first step in parsing, enabling the parser to
        understand and analyze the structure of the SQL statement.

        For instance, given the SQL statement:
        SELECT c1, c2, MIN(c3) FROM table1 GROUP BY c1, c2

        The Tokenizer would produce a sequence of tokens, each categorized appropriately
        (e.g.,
            SELECT as a keyword,
            c1 as an identifier,
            , as a delimiter,
            MIN as a function,
        etc.
        ). This breakdown facilitates the subsequent parsing stages, where the syntactical structure and
        semantics of the SQL statement are analyzed.

        We have overridden the Tokenizer class to define how your dialect handles various elements like

            - quotes
            - identifiers
            - keywords
            - Other lexical elements
        """

        # Define the escape character for strings.
        STRING_ESCAPES = ["\\"]

        # Define delimiters for identifiers.
        IDENTIFIERS = ['"']

        # Define delimiters for string literals.
        QUOTES = ["'"]

        # Comment syntax supported in the E6 dialect.
        COMMENTS = ["--", "//", ("/*", "*/")]

        # TODO:: Why other dialects have this long list of keywords but we are only relying on the
        #        these reserved keywords
        #       What is the meaning of this?
        # Need to deep dive in this. These are keywords list supported by a dialect and their mapping to specific kinds of keywords.
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            # Add E6-specific keywords here, e.g., "MY_KEYWORD": TokenType.KEYWORD
        }

    class Parser(parser.Parser):
        """
        The Parser class interprets the sequence of tokens to build an abstract syntax tree (AST).
        Override the Parser class to handle any syntax peculiarities of your dialect:


        The Parser class in SQLGlot processes a sequence of tokens generated from a SQL statement to construct
        an Abstract Syntax Tree (AST). This tree-like structure represents the hierarchical syntactic
        organization of the SQL query, enabling systematic analysis and manipulation.

        Given the SQL query:
        SELECT c1, c2, MIN(c3) FROM table1 GROUP BY c1, c2

        The Parser interprets the tokens produced by the Tokenizer to build the corresponding AST.
        In this context, the AST would be structured as follows:
        """

        # Define the set of data types that are supported for casting operations in the E6 dialect.

        SUPPORTED_CAST_TYPES = {
            "CHAR",
            "VARCHAR",
            "INT",
            "BIGINT",
            "BOOLEAN",
            "DATE",
            "FLOAT",
            "DOUBLE",
            "TIMESTAMP",
            "DECIMAL",
            "TIMESTAMP_TZ",
            "JSON",
        }

        def _parse_cast(self, strict: bool, safe: t.Optional[bool] = None) -> exp.Expression:
            """
            Overrides the base class's _parse_cast method to include validation
            against the SUPPORTED_CAST_TYPES set. If the target type is not supported,
            it raises an error.
            """
            cast_expression = super()._parse_cast(strict, safe)

            if isinstance(cast_expression, (exp.Cast, exp.TryCast)):
                target_type = cast_expression.to.this

                # Handle timestamp_tz as a special case
                if target_type == exp.DataType.Type.USERDEFINED:
                    # For user-defined types, check the kind in args
                    kind = cast_expression.to.args.get("kind")
                    if kind == "timestamp_tz":
                        # Allow timestamp_tz as a valid cast type
                        return cast_expression

                if target_type.name not in self.SUPPORTED_CAST_TYPES:
                    self.raise_error(f"Unsupported cast type: {target_type}")

            return cast_expression

        def _parse_position(self, haystack_first: bool = False) -> exp.StrPosition:
            args = self._parse_csv(self._parse_bitwise)

            if self._match(TokenType.IN):
                haystack = self._parse_bitwise()
                position = None

                # Check for `FROM` keyword and parse the starting position
                if self._match(TokenType.FROM):
                    position = self._parse_bitwise()

                return self.expression(
                    exp.StrPosition,
                    this=haystack,
                    substr=seq_get(args, 0),
                    position=position,
                )
            return super()._parse_position()

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "APPROX_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
            # TODO:: Need to understand this funcitons
            # Have to refer the documentation
            "APPROX_QUANTILES": exp.ApproxQuantile.from_arg_list,
            "APPROX_PERCENTILE": exp.ApproxQuantile.from_arg_list,
            "ARBITRARY": exp.AnyValue.from_arg_list,
            "ARRAY_AGG": exp.ArrayAgg.from_arg_list,
            "ARRAY_CONCAT": exp.ArrayConcat.from_arg_list,
            "ARRAY_CONTAINS": exp.ArrayContains.from_arg_list,
            "ARRAY_JOIN": exp.ArrayToString.from_arg_list,
            "ARRAY_TO_STRING": exp.ArrayToString.from_arg_list,
            "ARRAY_SLICE": lambda args: exp.ArraySlice(
                this=seq_get(args, 0),
                from_index=seq_get(args, 1),
                length=seq_get(args, 2),
                is_index=True,
            ),
            "ARRAY_POSITION": lambda args: exp.ArrayPosition(
                this=seq_get(args, 1), expression=seq_get(args, 0)
            ),
            "BITWISE_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BITWISE_OR": binary_from_function(exp.BitwiseOr),
            "BITWISE_XOR": binary_from_function(exp.BitwiseXor),
            "BITWISE_AND": binary_from_function(exp.BitwiseAnd),
            "CAST": _parse_cast,
            "CHARACTER_LENGTH": exp.Length.from_arg_list,
            "CHARINDEX": build_locate_strposition,
            "CHAR_LENGTH": exp.Length.from_arg_list,
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
            "CONVERT_TIMEZONE": _build_convert_timezone,
            "CONTAINS_SUBSTR": exp.Contains.from_arg_list,
            "CURRENT_DATE": exp.CurrentDate.from_arg_list,
            "CURRENT_TIMESTAMP": exp.CurrentTimestamp.from_arg_list,
            "DATE": _build_date,
            "DATE_ADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATE_DIFF": build_datediff(exp.DateDiff),
            "DATEDIFF": build_datediff(exp.DateDiff),
            "DATEPART": lambda args: exp.Extract(
                this=seq_get(args, 0), expression=seq_get(args, 1)
            ),
            "DATE_TRUNC": date_trunc_to_time,
            "DATETIME": _build_datetime_for_DT,
            "DAYOFWEEKISO": exp.DayOfWeekIso.from_arg_list,
            "DAYS": exp.Day.from_arg_list,
            "ELEMENT_AT": lambda args: exp.Bracket(
                this=seq_get(args, 0), expressions=[seq_get(args, 1)], offset=1, safe=True
            ),
            "FILTER_ARRAY": _parse_filter_array,
            "FIRST_VALUE": exp.FirstValue.from_arg_list,
            "FORMAT_DATE": lambda args: exp.TimeToStr(
                this=seq_get(args, 0), format=seq_get(args, 1)
            ),
            "FORMAT_TIMESTAMP": lambda args: exp.TimeToStr(
                this=seq_get(args, 0), format=seq_get(args, 1)
            ),
            "FROM_UNIXTIME_WITHUNIT": _build_from_unixtime_withunit,
            "FROM_ISO8601_TIMESTAMP": exp.FromISO8601Timestamp.from_arg_list,
            "GREATEST": exp.Max.from_arg_list,
            "JSON_EXTRACT": parser.build_extract_json_with_path(exp.JSONExtractScalar),
            "JSON_VALUE": parser.build_extract_json_with_path(exp.JSONExtractScalar),
            "LAST_DAY": exp.LastDay.from_arg_list,
            "LAST_VALUE": exp.LastValue,
            "LEFT": _build_with_arg_as_text(exp.Left),
            "LEN": exp.Length.from_arg_list,
            "LENGTH": exp.Length.from_arg_list,
            "LEAST": exp.Min.from_arg_list,
            "LISTAGG": exp.GroupConcat.from_arg_list,
            "LOCATE": build_locate_strposition,
            "LOG": exp.Log.from_arg_list,
            "LTRIM": lambda args: _build_trim(args),
            "MAX_BY": exp.ArgMax.from_arg_list,
            "MD5": exp.MD5Digest.from_arg_list,
            "MOD": lambda args: parser.build_mod(args),
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "NULLIF": exp.Nullif.from_arg_list,
            "NVL": lambda args: build_coalesce(args, is_nvl=True),
            "PARSE_DATE": _build_formatted_time_with_or_without_zone(exp.StrToDate, "E6"),
            "PARSE_DATETIME": _build_formatted_time_with_or_without_zone(exp.StrToTime, "E6"),
            "PARSE_TIMESTAMP": _build_formatted_time_with_or_without_zone(exp.StrToTime, "E6"),
            "POWER": exp.Pow.from_arg_list,
            "REGEXP_CONTAINS": exp.RegexpLike.from_arg_list,
            "REGEXP_COUNT": _build_regexp_count,
            "REGEXP_EXTRACT": exp.RegexpExtract.from_arg_list,
            "REGEXP_LIKE": exp.RegexpLike.from_arg_list,
            "REGEXP_REPLACE": exp.RegexpReplace.from_arg_list,
            "REPLACE": exp.RegexpReplace.from_arg_list,
            "ROUND": exp.Round.from_arg_list,
            "RIGHT": _build_with_arg_as_text(exp.Right),
            "RTRIM": lambda args: _build_trim(args, is_left=False),
            "SEQUENCE": exp.GenerateSeries.from_arg_list,
            "SHIFTRIGHT": binary_from_function(exp.BitwiseRightShift),
            "SHIFTLEFT": binary_from_function(exp.BitwiseLeftShift),
            "SIZE": exp.ArraySize.from_arg_list,
            "SPLIT": exp.Split.from_arg_list,
            # Function node for split_part is not there with equivalent functionality
            "SPLIT_PART": exp.SplitPart.from_arg_list,
            "STARTSWITH": exp.StartsWith.from_arg_list,
            "STARTS_WITH": exp.StartsWith.from_arg_list,
            "STDDEV": exp.Stddev.from_arg_list,
            "STDDEV_POP": exp.StddevPop.from_arg_list,
            "STRING_AGG": exp.GroupConcat.from_arg_list,
            "STRPOS": exp.StrPosition.from_arg_list,
            "SUBSTR": exp.Substring.from_arg_list,
            "TIMESTAMP": _build_timestamp,
            "TIMESTAMP_ADD": lambda args: exp.TimestampAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "TIMESTAMP_DIFF": lambda args: exp.TimestampDiff(
                this=seq_get(args, 0), expression=seq_get(args, 1), unit=seq_get(args, 2)
            ),
            "TO_CHAR": lambda args: exp.ToChar(
                this=seq_get(args, 0), format=E6().convert_format_time(expression=seq_get(args, 1))
            ),
            "TO_DATE": _parse_to_date,
            # "TO_DATE":  _build_datetime("TO_DATE", exp.DataType.Type.DATE),
            "TO_HEX": exp.Hex.from_arg_list,
            "TO_JSON": exp.JSONFormat.from_arg_list,
            "TO_TIMESTAMP": _build_datetime("TO_TIMESTAMP", exp.DataType.Type.TIMESTAMP),
            "TO_TIMESTAMP_NTZ": _build_datetime("TO_TIMESTAMP_NTZ", exp.DataType.Type.TIMESTAMP),
            "TO_UTF8": lambda args: exp.Encode(
                this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
            ),
            "TO_UNIX_TIMESTAMP": _build_to_unix_timestamp,
            "TO_VARCHAR": lambda args: exp.ToChar(
                this=seq_get(args, 0), format=E6().convert_format_time(expression=seq_get(args, 1))
            ),
            "TRUNC": date_trunc_to_time,
            "TRIM": lambda self: self._parse_trim(),
            "TYPEOF": lambda args: exp.Typeof(this=seq_get(args, 0)),
            "UNNEST": lambda args: exp.Explode(this=seq_get(args, 0)),
            # TODO:: I have removed the _parse_unnest_sql, was it really required
            # It was added due to some requirements before but those were asked to remove afterwards so it should not matter now
            "WEEK": exp.Week.from_arg_list,
            "WEEKISO": exp.Week.from_arg_list,
            "WEEKOFYEAR": exp.WeekOfYear.from_arg_list,
            "YEAR": exp.Year.from_arg_list,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "NAMED_STRUCT": lambda self: self._parse_json_object(),
            "OBJECT_CONSTRUCT": lambda self: self._parse_json_object(),
        }

        NO_PAREN_FUNCTIONS = parser.Parser.NO_PAREN_FUNCTIONS.copy()
        NO_PAREN_FUNCTIONS.pop(TokenType.CURRENT_USER)
        NO_PAREN_FUNCTIONS.pop(TokenType.CURRENT_TIME)

    class Generator(generator.Generator):
        """
        The Generator class is responsible for converting an abstract syntax tree (AST) back into a SQL string
        that adheres to a specific dialect’s syntax. When creating a custom dialect, you can override the Generator
        class to define how various expressions and data types should be formatted in your dialect.
        """

        EXTRACT_ALLOWS_QUOTES = False
        JSON_KEY_VALUE_PAIR_SEP = ","
        NVL2_SUPPORTED = True
        LAST_DAY_SUPPORTS_DATE_PART = False
        INTERVAL_ALLOWS_PLURAL_FORM = False
        NULL_ORDERING_SUPPORTED = None
        SUPPORTS_TABLE_ALIAS_COLUMNS = True
        SUPPORTS_MEDIAN = False

        CAST_SUPPORTED_TYPE_MAPPING = {
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.VARCHAR: "VARCHAR",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.TINYINT: "INT",
            exp.DataType.Type.SMALLINT: "INT",
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.DATE32: "DATE",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPNTZ: "TIMESTAMP",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.JSON: "JSON",
        }

        # TODO:: Adithya, why there was need to override this method.
        # So what was happening was this method will get called internally while .transpile is called. They have written this method with respect to other dialects.
        # But whenever we pass a normal query, by default parts like `NULLS LAST` etc were getting by defaults in order by clause which will differs the sequence of results displayed in original dialect and ours.
        # In order to tackle that, I overridden that so as to maintain structure of sqlglot with out altering original methods

        def pad_comment(self, comment: str) -> str:
            comment = comment.replace("/*", "/ *").replace("*/", "* /")
            comment = " " + comment if comment[0].strip() else comment
            comment = comment + " " if comment[-1].strip() else comment
            return comment

        def ordered_sql(self, expression: exp.Ordered) -> str:
            """
            Generate the SQL string for an ORDER BY clause in the E6 dialect.

            This method simplifies the ORDER BY clause by omitting any handling for NULL ordering,
            as the E6 dialect does not support explicit NULLS FIRST or NULLS LAST directives.

            Args:
                expression (exp.Ordered): The expression containing the column or expression to order by,
                                          along with sorting direction and null ordering preferences.

            Returns:
                str: The SQL string representing the ORDER BY clause.
            """
            # Determine the sorting direction based on the 'desc' argument
            sort_order = {True: " DESC", False: " ASC", None: ""}.get(expression.args.get("desc"))

            # Generate the SQL for the main expression to be ordered
            main_expression = self.sql(expression, "this")
            # TODO:: What is the significant of `this` parameter here
            # `this` is the whole sql part from select node that order by is part of

            # Initialize null ordering as an empty string
            nulls_sort_change = ""

            # Apply NULLS FIRST/LAST only if supported by the dialect
            if self.NULL_ORDERING_SUPPORTED:
                nulls_first = expression.args.get("nulls_first")
                nulls_sort_change = " NULLS FIRST" if nulls_first else " NULLS LAST"

            # Construct and return the final ORDER BY clause
            return f"{main_expression}{sort_order}{nulls_sort_change}"

        def convert_format_time(self, expression, **kwargs):
            """
            Transforms a time format string from one convention to another using the TIME_MAPPING dictionary.

            Args:
                expression (exp.Expression): The expression containing the time format string.

            Returns:
                str: The transformed time format string, or None if no format string is found.
            """
            # Check if the expression is a literal value
            # TODO:: Is this `if` condition extra, do we really require it
            # Yes we require this. Cuz for some cases what happens is the format arg will be a literal node and can be accessed using expression.this.
            # But this is not the case for all. For some functions what happens is the format part comes as `format` arg but not as `this`, this is due to declarations of those functions in other dialects
            # So as to acknowledge both cases we need this if.
            if isinstance(expression, exp.Literal):
                # Directly use the literal value as the format string
                format_str = expression.this
            else:
                # Attempt to retrieve the 'format' argument from the expression
                format_expr = expression.args.get("format")
                # Use the 'name' attribute of the format expression if it exists; otherwise, use the expression itself
                format_str = getattr(format_expr, "name", format_expr)

            # If no format string is found, return None
            if format_str is None:
                return None

            # Initialize the format string to be transformed
            format_string = format_str.replace("%%", "%")

            # Iterate over the TIME_MAPPING dictionary to replace each value with its corresponding key
            for key, value in E6().TIME_MAPPING.items():
                format_string = format_string.replace(value, key)

            # Return the transformed format string
            return format_string

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            """
            Generates the SQL string for a CAST operation in the E6 dialect.

            The method uses a custom type mapping (`CAST_SUPPORTED_TYPE_MAPPING`) to ensure that
            the target type in the CAST operation aligns with the E6 dialect.

            Args:
                expression (exp.Cast): The CAST expression containing the value and target type.
                safe_prefix (Optional[str]): An optional prefix for safe casting (not used here).

            Returns:
                str: The SQL string for the CAST operation.
            """
            # Extract the target type from the CAST expression
            target_type = expression.to.this

            # Map the target type to the corresponding E6 type
            e6_type = self.CAST_SUPPORTED_TYPE_MAPPING.get(target_type, target_type)

            # Generate the SQL string for the CAST operation
            return f"CAST({self.sql(expression.this)} AS {e6_type})"

        def coalesce_sql(self, expression: exp.Coalesce) -> str:
            func_name = "NVL" if expression.args.get("is_nvl") else "COALESCE"
            return rename_func(func_name)(self, expression)

        def interval_sql(self, expression: exp.Interval) -> str:
            """
            Generate an SQL INTERVAL expression from the given Interval object.

            This function constructs a string representing an SQL INTERVAL based on
            the provided `expression`. If both `expression.this` (the value) and
            `expression.unit` (the unit of time) are present, it returns a string
            formatted as 'INTERVAL {value} {unit}'. If either is missing, it returns
            an empty string.

            Parameters:
            expression (exp.Interval): An object containing the interval value and unit.

            Returns:
            str: A string representing the SQL INTERVAL or an empty string if the
                 necessary components are missing.

            Example:
            >>> expr = exp.Interval(this=exp.Literal(5), unit=exp.Literal('DAY'))
            >>> generator = Generator()
            >>> generator.interval_sql(expr)
            'INTERVAL 5 DAY'
            """
            # TODO:: Ask Adithya, how he has guessed about this `.this` & `.unit`
            # While you debug anything, you can see the tree like structures there and see what are our candidates to fetch and do manipulations
            # You can use evaluate exression also there to verfy what we want

            # Check if both 'this' (value) and 'unit' are present in the expression
            if expression.this and expression.unit:
                # Extract the name attributes of 'this' and 'unit'
                value = expression.this.name
                unit = expression.unit.name

                # Convert plural forms to singular if not allowed
                if not self.INTERVAL_ALLOWS_PLURAL_FORM:
                    unit = self.TIME_PART_SINGULARS.get(unit, unit)

                # Format the INTERVAL string
                interval_str = f"INTERVAL '{value} {unit}'"
                return interval_str
            elif expression.this and not expression.unit:
                # Handle compound intervals like '5 minutes 30 seconds'
                value = (
                    expression.this.name
                    if hasattr(expression.this, "name")
                    else str(expression.this)
                )

                # Parse compound interval and convert to E6 format
                import re

                # Pattern to match number-unit pairs in the compound interval
                pattern = (
                    r"(\d+)\s*(year|month|week|day|hour|minute|second|microsecond|millisecond)s?"
                )
                matches = re.findall(pattern, value.lower())

                if matches:
                    # Convert compound interval to sum of individual intervals
                    interval_parts = []
                    for num, unit in matches:
                        # Convert plural to singular if needed
                        if not self.INTERVAL_ALLOWS_PLURAL_FORM:
                            unit = self.TIME_PART_SINGULARS.get(unit.upper() + "S", unit.upper())
                        else:
                            unit = unit.upper()
                        interval_parts.append(f"INTERVAL '{num} {unit}'")

                    # Join with + operator
                    if len(interval_parts) > 1:
                        return " + ".join(interval_parts)
                    elif len(interval_parts) == 1:
                        return interval_parts[0]

                # If no pattern matches, return as-is with quotes
                return f"INTERVAL '{value}'"
            else:
                # Return an empty string if either 'this' or 'unit' is missing
                return f"INTERVAL {expression.this if expression.this else ''} {expression.unit if expression.unit else ''}"

        # Need to look at the problem here regarding double casts appearing
        def _last_day_sql(self: E6.Generator, expression: exp.LastDay) -> str:
            # date_expr = self.sql(expression,"this")
            date_expr = expression.args.get("this")
            date_expr = date_expr
            if isinstance(date_expr, exp.Literal):
                date_expr = f"CAST({date_expr} AS DATE)"
            unit = expression.args.get("unit")
            if unit:
                return self.func("LAST_DAY", date_expr, unit)
            return self.func("LAST_DAY", date_expr)

        def extract_sql(self: E6.Generator, expression: exp.Extract | exp.DayOfYear) -> str:
            unit = expression.this.name
            unit_mapped = E6.UNIT_PART_MAPPING.get(f"'{unit.lower()}'", unit)
            date_expr = (
                expression.expression if isinstance(expression, exp.Extract) else expression.this
            )

            if isinstance(expression, exp.DayOfYear):
                unit_mapped = "DOY"
                if isinstance(
                    date_expr, (exp.TsOrDsToDate, exp.TsOrDsToTimestamp, exp.TsOrDsToTime)
                ):
                    date_expr = exp.Cast(this=date_expr.this, to=exp.DataType(this="TIMESTAMP"))
                expression_sql = self.sql(date_expr)
            if isinstance(expression, exp.Extract):
                # For regular Extract operations, cast string literals and columns to TIMESTAMP for robust E6 syntax
                if (isinstance(date_expr, exp.Literal) and date_expr.is_string) or isinstance(
                    date_expr, exp.Column
                ):
                    date_expr = exp.Cast(this=date_expr, to=exp.DataType(this="DATE"))
                expression_sql = self.sql(date_expr)

            extract_str = f"EXTRACT({unit_mapped} FROM {expression_sql})"
            return extract_str

        def filter_array_sql(self: E6.Generator, expression: exp.ArrayFilter) -> str:
            cond = expression.expression
            if isinstance(cond, exp.Lambda):
                aliases = cond.expressions
                cond = cond.this
            elif isinstance(cond, exp.Predicate):
                aliases = [exp.Identifier(this="_u")]
                cond = cond
            else:
                self.unsupported("Unsupported filter condition")
                return ""

            # Check for aggregate functions
            if any(isinstance(node, exp.AggFunc) for node in cond.find_all(exp.Expression)):
                raise ValueError(
                    "array filter's Lambda expression are not supported with aggregate functions"
                )
                return ""

            if len(aliases) == 1:
                aliases_str = aliases[0]
            else:
                aliases_str = ", ".join(self.sql(alias) for alias in aliases)
                aliases_str = f"({aliases_str})"
            lambda_expr = f"{aliases_str} -> {self.sql(cond)}"
            return f"FILTER_ARRAY({self.sql(expression.this)}, {lambda_expr})"

        def explode_sql(self, expression: exp.Explode) -> str:
            # Extract array expressions
            array_expr = expression.args.get("expressions")
            if expression.this:
                return self.func("EXPLODE", expression.this)

            # Format array expressions to SQL
            if isinstance(array_expr, list):
                array_expr_sql = ", ".join(self.sql(arg) for arg in array_expr)
            else:
                array_expr_sql = self.sql(array_expr)

            # Process the alias
            alias = self.sql(expression, "alias")

            # Handle the columns for alias arguments (e.g., t(x))
            alias_args = expression.args.get("alias")
            alias_columns = ""

            if alias_args and alias_args.args.get("columns"):
                # Extract the columns for alias arguments
                alias_columns_list = [self.sql(col) for col in alias_args.args["columns"]]
                alias_columns = f"({', '.join(alias_columns_list)})"

            # Construct the alias string
            alias_sql = f" AS {alias}{alias_columns}" if alias else ""

            # Generate the final UNNEST SQL
            return f"EXPLODE({array_expr_sql}){alias_sql}"

        def format_date_sql(self: E6.Generator, expression: exp.TimeToStr) -> str:
            date_expr = expression.this
            format_expr = self.convert_format_time(expression)
            format_expr_quoted = f"'{format_expr}'"

            time_format_tokens = {"h", "hh", "s", "ss", "H", "HH", "m", "mm", "S", "SS"}
            requires_timestamp = any(token in format_expr for token in time_format_tokens)

            if (
                isinstance(date_expr, exp.CurrentDate)
                or isinstance(date_expr, exp.CurrentTimestamp)
                or isinstance(date_expr, exp.TsOrDsToDate)
            ):
                return self.func(
                    "FORMAT_TIMESTAMP" if requires_timestamp else "FORMAT_DATE",
                    date_expr,
                    format_expr_quoted,
                )
            if isinstance(date_expr, exp.Cast) and not (
                date_expr.to.this.name == "TIMESTAMP" or date_expr.to.this.name == "DATE"
            ):
                date_expr = f"CAST({date_expr} AS DATE)"
            return self.func(
                "FORMAT_TIMESTAMP" if requires_timestamp else "FORMAT_DATE",
                date_expr,
                format_expr_quoted,
            )

        def struct_sql(self, expression: exp.Struct) -> str:
            keys = []
            values = []

            for i, e in enumerate(expression.expressions):
                if isinstance(e, exp.PropertyEQ):
                    keys.append(
                        exp.Literal.string(e.name) if isinstance(e.this, exp.Identifier) else e.this
                    )
                    values.append(e.expression)
                else:
                    keys.append(exp.Literal.string(f"_{i}"))
                    values.append(e)

            return self.func("NAMED_STRUCT", *flatten(zip(keys, values)))

        def neq_sql(self, expression: exp.NEQ) -> str:
            return self.binary(expression, "!=")

        def tochar_sql(self, expression: exp.ToChar) -> str:
            date_expr = expression.this
            if len(expression.args) < 2:
                return self.func("TO_CHAR", expression.this)
            if (
                isinstance(date_expr, exp.Cast) and not (date_expr.to.this.name == "TIMESTAMP")
            ) or (
                not isinstance(date_expr, exp.Cast)
                and not exp.DataType.is_type(date_expr, exp.DataType.Type.DATE)
                or exp.DataType.is_type(date_expr, exp.DataType.Type.TIMESTAMP)
            ):
                date_expr = f"CAST({date_expr} AS TIMESTAMP)"
            format_expr = self.convert_format_time(expression)
            return f"TO_CHAR({date_expr},'{format_expr}')"

        def bracket_sql(self, expression: exp.Bracket) -> str:
            if (
                expression.expressions.__len__() == 1
                and expression.expressions[0].is_string
                and not expression._meta
            ):
                text_expr = expression.expressions[0].this
                return f'{self.sql(expression.this)}."{text_expr}"'
            return self.func(
                "TRY_ELEMENT_AT"
                if expression._meta and expression._meta.get("name", "").upper() == "TRY_ELEMENT_AT"
                else "ELEMENT_AT",
                expression.this,
                seq_get(
                    apply_index_offset(
                        expression.this,
                        expression.expressions,
                        1 - expression.args.get("offset", 0),
                    ),
                    0,
                ),
            )

        def generateseries_sql(self, expression: exp.GenerateSeries) -> str:
            start = expression.args["start"]
            end = expression.args["end"]
            step = expression.args.get("step")

            return self.func("SEQUENCE", start, end, step)

        def array_sql(self, expression: exp.Array) -> str:
            expressions_sql = ", ".join(self.sql(e) for e in expression.expressions)
            return f"ARRAY[{expressions_sql}]"

        def map_sql(self, expression: exp.Map | exp.VarMap) -> str:
            keys = expression.args.get("keys")
            values = expression.args.get("values")
            final_keys = keys
            final_values = values
            if isinstance(keys, exp.Split):
                key_str = keys.this.this if isinstance(keys.this, exp.Literal) else None
                final_keys = f"'{key_str}'"

            if isinstance(values, exp.Split):
                values_str = values.this.this if isinstance(values.this, exp.Literal) else None
                final_values = f"'{values_str}'"

            return f"MAP[{self.sql(final_keys)},{self.sql(final_values)}]"

        def length_sql(self, expression: exp.Length) -> str:
            """
            Overrides the Length SQL generation for E6.

            Purpose:
            --------
            In Snowflake and BigQuery, the `Length` function has an optional `binary` argument that
            defaults to `True`. This supports the length of binary/hex strings. In the E6 dialect, we
            do not need this `binary` argument. So, this method strips out the `binary` behavior when
            generating the SQL for `Length`.

            Args:
                expression (exp.Length): The Length expression from the AST.

            Returns:
                str: The SQL for the `Length` function in E6, without the binary argument.
            """
            # Get the SQL representation of the column or expression whose length is to be calculated
            length_expr = self.sql(expression, "this")

            # Directly return the Length function call without considering binary behavior
            return f"LENGTH({length_expr})"

        def anonymous_sql(self, expression: exp.Anonymous) -> str:
            # Map the function names that need to be rewritten with same order of arguments
            function_mapping_normal = {"REGEXP_INSTR": "INSTR"}
            # Extract the function name from the expression
            function_name = self.sql(expression, "this")

            if function_name in function_mapping_normal:
                # Check if the function name needs to be mapped to a different one
                mapped_function = function_mapping_normal.get(function_name, function_name)

                # Generate the SQL for the mapped function with its expressions
                return self.func(mapped_function, *expression.expressions)

            elif function_name.lower() == "table":
                return f"{self.sql(*expression.expressions)}"

            return self.func(function_name, *expression.expressions)

        def to_timestamp_sql(self: E6.Generator, expression: exp.StrToTime) -> str:
            date_expr = expression.this
            format_expr = self.convert_format_time(expression)
            format_str = f"'{format_expr}'"
            return self.func("TO_TIMESTAMP", date_expr, format_str)

        def string_agg_sql(self: E6.Generator, expression: exp.GroupConcat) -> str:
            """
            Generate the SQL for the STRING_AGG or LISTAGG function in E6.

            This method addresses an AST parsing issue where the separator for the STRING_AGG function
            sometimes appears under the DISTINCT node due to parsing intricacies. Instead of modifying
            expr_1 directly, this version clones expr_1 to retain DISTINCT while applying the separator
            correctly.

            Args:
                expression (exp.GroupConcat): The AST expression for GROUP_CONCAT

            Returns:
                str: The SQL representation for STRING_AGG/LISTAGG with proper separator handling.
            """
            separator = expression.args.get("separator")
            expr_1 = expression.this

            # If no separator was found, check if it's embedded in DISTINCT
            if separator is None and isinstance(expr_1, exp.Distinct):
                # If DISTINCT has two expressions, the second may represent the separator
                if len(expr_1.expressions) == 2 and isinstance(
                    expr_1.expressions[1], (exp.Literal, exp.Column, exp.Identifier)
                ):
                    separator = expr_1.expressions[1]  # Use second expression as separator

                    # Clone DISTINCT to keep it unchanged, then apply the first expression for aggregation
                    distinct_expr_clone = expr_1.copy()
                    distinct_expr_clone.set("expressions", [expr_1.expressions[0]])
                    expr_1 = distinct_expr_clone

            # Generate SQL using STRING_AGG/LISTAGG, with separator or default ''
            return self.func("LISTAGG", expr_1, separator or exp.Literal.string(""))

        def concat_ws_sql(self: E6.Generator, expression: exp.ConcatWs) -> str:
            """
            Generate the SQL for the CONCAT_WS function in E6.

            Implements Databricks CONCAT_WS behavior:
            - If sep is NULL the result is NULL (handled by e6 engine)
            - exprN that are NULL are ignored
            - If only separator provided or all exprN are NULL, returns empty string
            - Each exprN can be STRING or ARRAY of STRING
            - NULLs within arrays are filtered out
            - Arrays are flattened and individual elements joined with separator
            """
            if not expression.expressions:
                return "''"

            # Extract separator and arguments
            separator = expression.expressions[0]
            args = expression.expressions[1:] if len(expression.expressions) > 1 else []

            # If no arguments provided (only separator), return empty string
            if not args:
                return "''"

            # Collect all non-NULL expression nodes (flattening arrays)
            array_expressions = []

            for arg in args:
                if isinstance(arg, exp.Array):
                    # For array arguments: add non-NULL elements
                    for element in arg.expressions:
                        if not isinstance(element, exp.Null):
                            array_expressions.append(element)
                else:
                    # For string arguments: add if not NULL
                    if not isinstance(arg, exp.Null):
                        array_expressions.append(arg)

            # If no elements after filtering, return empty string
            if not array_expressions:
                return "''"

            # Single element case - just return the element
            if len(array_expressions) == 1:
                return self.sql(array_expressions[0])

            # Multiple elements: create array and join with separator
            # Build: ARRAY_TO_STRING(ARRAY[element1, element2, ...], separator)
            # Create Array expression with the actual expression nodes
            array_expr = exp.Array(expressions=array_expressions)

            # Use ARRAY_TO_STRING function directly instead of exp.ArrayToString
            # to avoid the ARRAY_JOIN mapping in TRANSFORMS
            return self.func("ARRAY_TO_STRING", array_expr, separator)

        # def struct_sql(self, expression: exp.Struct) -> str:
        #     struct_expr = expression.expressions
        #     return f"{struct_expr}"

        def TsOrDsToDate_sql(self: E6.Generator, expression: exp.TsOrDsToDate) -> str:
            format_str = self.convert_format_time(expression)
            if format_str is None:
                return self.func("TO_DATE", expression.this)
            else:
                return self.func("TO_DATE", expression.this, add_single_quotes(format_str))

        def to_unix_timestamp_sql(
            self: E6.Generator, expression: exp.TimeToUnix | exp.StrToUnix
        ) -> str:
            format_str = None
            time_expr = expression.this
            format_expr = expression.args.get("format")

            if format_expr:
                format_str = format_expr.this
                for key, value in E6().TIME_MAPPING_FOR_PARSE_FUNCTIONS.items():
                    format_str = format_str.replace(key, value)

            # Generate final function with or without format argument
            if format_str:
                unix_timestamp_expr = self.func(
                    "TO_UNIX_TIMESTAMP",
                    self.func(
                        "PARSE_DATETIME",
                        add_single_quotes(self.sql(format_str)),
                        self.sql(time_expr),
                    ),
                )
            else:
                unix_timestamp_expr = self.func("TO_UNIX_TIMESTAMP", self.sql(time_expr))

            if isinstance(expression.parent, (exp.UnixToTime, exp.UnixToStr)):
                return f"{unix_timestamp_expr}"
            return f"{unix_timestamp_expr}/1000"

        def lateral_sql(self, expression: exp.Lateral) -> str:
            expression.set("view", True)
            this = self.sql(expression, "this")

            alias = expression.args["alias"]
            columns = self.expressions(alias, key="columns", flat=True)
            columns_cleaned = columns.replace('"', "")

            table = f"{alias.name}" if alias and alias.name else ""
            columns = (
                f"{columns}"
                if columns and not columns_cleaned == "SEQ, KEY, PATH, INDEX, VALUE, THIS"
                else ""
            )

            op_sql = self.seg(f"LATERAL VIEW{' OUTER' if expression.args.get('outer') else ''}")

            if table and columns:
                return f"{op_sql}{self.sep()}{this} {table} AS {columns}"
            elif table:
                return f"{op_sql}{self.sep()}{this} AS {table}"
            else:
                return f"{op_sql}{self.sep()}{this}"

        def not_sql(self, expression: exp.Not) -> str:
            expr = expression.this
            if isinstance(expr, exp.Is):
                return f"{self.sql(expr.this)} IS NOT {self.sql(expr.expression)}"
            else:
                return super().not_sql(expression)

        def median_sql(self, expression: exp.Median):
            if not self.SUPPORTS_MEDIAN:
                this = expression.this

                # Check if Distinct keyword is present at the start
                if this and isinstance(this, exp.Distinct):
                    return self.function_fallback_sql(expression)

                order_by = exp.Order(expression=expression.this)
                order_by_column = f" {order_by} {this}"
                percentile_cont_expr = exp.PercentileCont(this=exp.Literal.number(0.5))
                within_group = exp.WithinGroup(
                    this=percentile_cont_expr, expression=order_by_column
                )
                return f"{within_group}"

            return self.function_fallback_sql(expression)

        def from_unixtime_sql(
            self: E6.Generator, expression: exp.UnixToTime | exp.UnixToStr
        ) -> str:
            unix_expr = expression.this
            format_expr = expression.args.get("format")
            scale_expr = expression.args.get("scale")

            # If scale is seconds, use FROM_UNIXTIME_WITHUNIT
            if scale_expr and scale_expr.this == "seconds":
                return self.func("FROM_UNIXTIME_WITHUNIT", unix_expr, scale_expr)
            # If scale is milliseconds, use FROM_UNIXTIME_WITHUNIT
            if scale_expr and scale_expr.this == "milliseconds":
                return self.func("FROM_UNIXTIME_WITHUNIT", unix_expr, scale_expr)
            if scale_expr and scale_expr.this == "microseconds":
                # Convert microseconds to milliseconds by dividing by 1000
                unix_expr_div = exp.Div(this=unix_expr, expression=exp.Literal.number(1000))
                scale_expr = exp.Literal.string("milliseconds")
                return self.func("FROM_UNIXTIME_WITHUNIT", unix_expr_div, scale_expr)

            if not format_expr:
                return self.func("FROM_UNIXTIME", unix_expr)
            format_expr_modified = self.convert_format_time(format_expr)
            format_expr_modified = add_single_quotes(format_expr_modified)

            if isinstance(unix_expr, (exp.TimeToUnix, exp.StrToUnix)):
                return self.func("FORMAT_TIMESTAMP", unix_expr.this, format_expr_modified)

            return self.func(
                "FORMAT_TIMESTAMP", self.func("FROM_UNIXTIME", unix_expr), format_expr_modified
            )

        def from_iso8601_timestamp_sql(self, expression: exp.FromISO8601Timestamp) -> str:
            return f"CAST({self.sql(expression.this)} AS timestamp_tz)"

        def timestamp_diff_sql(self, expression: exp.TimestampDiff) -> str:
            return self.func(
                "TIMESTAMP_DIFF", expression.this, expression.expression, unit_to_str(expression)
            )

        def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
            datetime_str = expression.this
            name: str = "DATETIME"

            meta: Optional[dict[str, Any]] = expression._meta
            if meta is not None:
                raw_name: Any = meta.get("name")
                if isinstance(raw_name, str) and raw_name.lower() == "from_utc_timestamp":
                    name = "FROM_UTC_TIMESTAMP"

            zone = expression.args.get("zone")
            return self.func(name, datetime_str, zone)

        def json_format_sql(self, expression: exp.JSONFormat) -> str:
            inner = expression.this
            if isinstance(inner, exp.Cast) and inner.to.this == exp.DataType.Type.JSON:
                return self.func("TO_JSON", inner.this)
            return self.func("TO_JSON", inner)

        def json_extract_sql(self, e: exp.JSONExtract | exp.JSONExtractScalar):
            path = e.expression
            if self.from_dialect == "databricks":
                if not self.sql(path).startswith("'$."):
                    path = add_single_quotes("$." + self.sql(path))
                else:
                    path = self.sql(path)

            return self.func("JSON_EXTRACT", e.this, path)

        def split_sql(self, expression: exp.Split | exp.RegexpSplit):
            this = expression.this
            delimitter = expression.expression
            if (
                this
                and delimitter
                and this.is_string
                and delimitter.is_string
                and delimitter.this not in this.this
                and not len(expression.args) == 3
            ):
                return f"{this}"
            return rename_func("SPLIT")(self, expression)

        # Define how specific expressions should be transformed into SQL strings
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Anonymous: anonymous_sql,
            exp.Any: lambda self, e: self.any_sql(e).replace("ANY", "SOME"),
            exp.FindInSet: lambda self, e: self.func(
                "ARRAY_POSITION", e.this, self.func("SPLIT", e.expression, exp.Literal.string(","))
            ),
            exp.AnyValue: rename_func("ARBITRARY"),
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
            exp.ArgMax: rename_func("MAX_BY"),
            exp.ArgMin: rename_func("MIN_BY"),
            exp.Array: array_sql,
            exp.Typeof: rename_func("TYPEOF"),
            exp.ArrayAgg: rename_func("ARRAY_AGG"),
            exp.ArrayConcat: rename_func("ARRAY_CONCAT"),
            exp.ArrayIntersect: rename_func("ARRAY_INTERSECT"),
            exp.ArrayContains: rename_func("ARRAY_CONTAINS"),
            exp.ArrayFilter: filter_array_sql,
            exp.ArrayToString: rename_func("ARRAY_JOIN"),
            exp.ArraySize: rename_func("size"),
            exp.ArraySlice: lambda self, e: self.func(
                "ARRAY_SLICE",
                e.args.get("this"),
                e.args.get("from_index"),
                e.args.get("length")
                if e.args.get("is_index")
                else e.args.get("length") + e.args.get("from_index"),
            ),
            exp.ArrayUniqueAgg: rename_func("ARRAY_AGG"),
            exp.ArrayPosition: lambda self, e: self.func("ARRAY_POSITION", e.expression, e.this),
            exp.AtTimeZone: attimezone_sql,
            exp.BitwiseLeftShift: lambda self, e: self.func("SHIFTLEFT", e.this, e.expression),
            exp.BitwiseNot: lambda self, e: self.func("BITWISE_NOT", e.this),
            exp.BitwiseAnd: lambda self, e: self.func("BITWISE_AND", e.this, e.expression),
            exp.BitwiseOr: lambda self, e: self.func("BITWISE_OR", e.this, e.expression),
            exp.BitwiseRightShift: lambda self, e: self.func("SHIFTRIGHT", e.this, e.expression),
            exp.BitwiseXor: lambda self, e: self.func("BITWISE_XOR", e.this, e.expression),
            exp.Bracket: bracket_sql,
            # We mapped this believing that for most of the cases,
            # CONCAT function in other dialects would mostly use for ARRAY concatenation
            exp.Concat: rename_func("CONCAT"),
            exp.ConcatWs: concat_ws_sql,
            exp.Contains: rename_func("CONTAINS_SUBSTR"),
            exp.CurrentDate: lambda *_: "CURRENT_DATE",
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.Coalesce: coalesce_sql,
            exp.Date: lambda self, e: self.func("DATE", e.this),
            exp.DateAdd: lambda self, e: self.func(
                "DATE_ADD",
                unit_to_str(e),
                _to_int(e.expression),
                e.this,
            ),
            # follows signature DATE_DIFF([ <unit>,] <date_expr1>, <date_expr2>) of E6. => date_expr1 - date_expr2, so interchanging the second and third arg
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
            ),
            exp.DateSub: rename_func("DATE_SUB"),
            exp.DateTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.Datetime: lambda self, e: self.func("DATETIME", e.this, e.expression),
            exp.Day: rename_func("DAYS"),
            exp.DayOfMonth: rename_func("DAYS"),
            exp.DayOfWeekIso: rename_func("DAYOFWEEKISO"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfYear: extract_sql,
            exp.Encode: lambda self, e: self.func("TO_UTF8", e.this),
            exp.Explode: explode_sql,
            exp.Extract: extract_sql,
            exp.FirstValue: rename_func("FIRST_VALUE"),
            exp.Format: rename_func("FORMAT"),
            exp.FormatDatetime: rename_func("FORMAT_DATETIME"),
            exp.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", e.args.get("zone"), "'UTC'", e.this
            ),
            exp.FromISO8601Timestamp: from_iso8601_timestamp_sql,
            exp.GenerateSeries: generateseries_sql,
            exp.GroupConcat: string_agg_sql,
            exp.Hex: rename_func("TO_HEX"),
            exp.Interval: interval_sql,
            exp.JSONExtract: json_extract_sql,
            exp.JSONExtractScalar: json_extract_sql,
            exp.JSONFormat: json_format_sql,
            exp.JSONObject: lambda self, e: self.func("NAMED_STRUCT", e.this, *e.expressions),
            exp.LastDay: _last_day_sql,
            exp.LastValue: rename_func("LAST_VALUE"),
            exp.Length: length_sql,
            exp.Log: lambda self, e: self.func("LOG", e.this, e.expression),
            exp.Lower: rename_func("LOWER"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            exp.Map: map_sql,
            exp.Max: max_or_greatest,
            exp.MD5Digest: lambda self, e: self.func("MD5", e.this),
            exp.Min: min_or_least,
            exp.Mod: lambda self, e: self.func("MOD", e.this, e.expression),
            exp.Nullif: rename_func("NULLIF"),
            exp.Pow: rename_func("POWER"),
            exp.Quarter: lambda self, e: self.extract_sql(
                exp.Extract(this=exp.Var(this="QUARTER"), expression=e.this)
            ),
            exp.RegexpExtract: rename_func("REGEXP_EXTRACT"),
            exp.RegexpLike: lambda self, e: self.func("REGEXP_LIKE", e.this, e.expression),
            # here I handled replacement arg carefully because, sometimes if replacement arg is not provided/extracted then it is getting None there overriding in E6
            exp.RegexpReplace: rename_func("REGEXP_REPLACE"),
            exp.RegexpSplit: split_sql,
            # exp.Select: select_sql,
            exp.Space: lambda self, e: self.func("REPEAT", exp.Literal.string(" "), e.this),
            exp.Split: split_sql,
            exp.SplitPart: rename_func("SPLIT_PART"),
            exp.Stddev: rename_func("STDDEV"),
            exp.StddevPop: rename_func("STDDEV_POP"),
            exp.StrPosition: lambda self, e: self.func(
                "LOCATE", e.args.get("substr"), e.this, e.args.get("position")
            ),
            exp.StrToDate: lambda self, e: self.func(
                "TO_DATE", e.this, add_single_quotes(self.convert_format_time(e))
            ),
            exp.StrToTime: to_timestamp_sql,
            exp.StrToUnix: to_unix_timestamp_sql,
            exp.StartsWith: rename_func("STARTS_WITH"),
            # exp.Struct: struct_sql,
            exp.TimeToStr: format_date_sql,
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimeStrToDate: datestrtodate_sql,
            exp.TimeToUnix: to_unix_timestamp_sql,
            exp.Timestamp: lambda self, e: self.func("TIMESTAMP", e.this),
            exp.TimestampAdd: lambda self, e: self.func(
                "TIMESTAMP_ADD", unit_to_str(e), e.expression, e.this
            ),
            exp.TimestampDiff: timestamp_diff_sql,
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.ToChar: tochar_sql,
            # WE REMOVE ONLY WHITE SPACES IN TRIM FUNCTION
            exp.Trim: _trim_sql,
            exp.TryCast: lambda self, e: self.func(
                "TRY_CAST", f"{self.sql(e.this)} AS {self.sql(e.to)}"
            ),
            exp.TsOrDsAdd: lambda self, e: self.func(
                "DATE_ADD",
                unit_to_str(e),
                _to_int(e.expression),
                e.this,
            ),
            exp.TsOrDsDiff: lambda self, e: self.func(
                "DATE_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
            ),
            exp.TsOrDsToDate: TsOrDsToDate_sql,
            exp.UnixToTime: from_unixtime_sql,
            exp.UnixToStr: from_unixtime_sql,
            exp.VarMap: map_sql,
            exp.Upper: rename_func("UPPER"),
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
        }

        RESERVED_KEYWORDS = {
            "add",
            "all",
            "and",
            "as",
            "asc",
            "before",
            "between",
            "bigint",
            "case",
            "char",
            "character",
            "continue",
            "convert",
            "cube",
            "current_date",
            "current_timestamp",
            "decimal",
            "dense_rank",
            "desc",
            "distinct",
            "div",
            "double",
            "else",
            "except",
            "exists",
            "false",
            "first_value",
            "float",
            "from",
            "group",
            "grouping",
            "having",
            "in",
            "inner",
            "int",
            "integer",
            "intersect",
            "interval",
            "is",
            "join",
            "key",
            "keys",
            "lag",
            "last_value",
            "lead",
            "left",
            "like",
            "limit",
            "localtime",
            "localtimestamp",
            "mod",
            "not",
            "nth_value",
            "ntile",
            "null",
            "of",
            "on",
            "or",
            "order",
            "outer",
            "over",
            "partition",
            "percent_rank",
            "rank",
            "regexp",
            "return",
            "right",
            "rlike",
            "row",
            "row_number",
            "select",
            "smallint",
            "then",
            "true",
            "union",
            "unsigned",
            "update",
            "use",
            "values",
            "varchar",
            "when",
            "where",
            "while",
            "window",
            "with",
            "xor",
            "dense_rank",
            "except",
            "first_value",
            "grouping",
            "groups",
            "intersect",
            "json_table",
            "lag",
            "last_value",
            "lead",
            "nth_value",
            "ntile",
            "of",
            "over",
            "percent_rank",
            "rank",
            "row_number",
            "sunday",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "variant",
        }

        UNSIGNED_TYPE_MAPPING = {
            exp.DataType.Type.UBIGINT: "BIGINT",
            exp.DataType.Type.UINT: "INT",
            exp.DataType.Type.UMEDIUMINT: "INT",
            exp.DataType.Type.USMALLINT: "INT",
            exp.DataType.Type.UTINYINT: "INT",
            exp.DataType.Type.UDECIMAL: "DECIMAL",
        }

        # Map generic data types to your dialect's specific data type names
        TYPE_MAPPING = {
            **UNSIGNED_TYPE_MAPPING,
            **CAST_SUPPORTED_TYPE_MAPPING,
            exp.DataType.Type.JSON: "JSON",
            exp.DataType.Type.STRUCT: "STRUCT",
            exp.DataType.Type.ARRAY: "ARRAY",
        }
