from __future__ import annotations

import typing as t

import sqlglot

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    binary_from_function,
    date_trunc_to_time,
    build_formatted_time,
    max_or_greatest,
    min_or_least,
    locate_to_strposition,
    rename_func,
    unit_to_str,
    regexp_replace_sql,
    approx_count_distinct_sql,
)
from sqlglot.expressions import ArrayFilter, RegexpExtract
from sqlglot.helper import flatten, is_float, is_int, seq_get, is_type

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def _build_datetime(
        name: str, kind: exp.DataType.Type, safe: bool = False
) -> t.Callable[[t.List], exp.Func]:
    def _builder(args: t.List) -> exp.Func:
        value = seq_get(args, 0)
        int_value = value is not None and is_int(value.name)

        if isinstance(value, exp.Literal):
            # Converts calls like `TO_TIME('01:02:03')` into casts
            if len(args) == 1 and value.is_string and not int_value:
                return exp.cast(value, kind)

            # cases so we can transpile them, since they're relatively common
            if kind == exp.DataType.Type.TIMESTAMP:
                if int_value:
                    return exp.UnixToTime(this=value, scale=seq_get(args, 1))
                if not is_float(value.this):
                    return build_formatted_time(exp.StrToTime, "snowflake")(args)

        if kind == exp.DataType.Type.DATE and not int_value:
            formatted_exp = build_formatted_time(exp.TsOrDsToDate, "e6")(args)
            formatted_exp.set("safe", safe)
            return formatted_exp

        return exp.Anonymous(this=name, expressions=args)

    return _builder


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

    if unit is None or unit.this.lower() not in {'seconds', 'milliseconds'}:
        raise ValueError(f"Unsupported unit for FROM_UNIXTIME_WITHUNIT: {unit if unit else 'Nothing'}")

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
                    seq_get(args, 0)
                    or (E6().TIME_FORMAT if default is True else default or None)
                )
            )
        return exp_class(
            this=seq_get(args, 1),
            format=format_time_for_parsefunctions(
                seq_get(args, 0)
                or (E6().TIME_FORMAT if default is True else default or None)
            ),
            zone=seq_get(args, 2)
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


# how others use use from_unixtime_withunit and how E6 differs.
def _from_unixtime_withunit_sql(self: E6.Generator, expression: exp.UnixToTime) -> str:
    timestamp = self.sql(expression, "this")
    scale = expression.args.get("scale")
    # this by default value for seconds is been kept for now
    if scale is None:
        scale = "'seconds'"
    #     raise ValueError("Unit 'seconds' or 'milliseconds' need to be provided")
    # if scale not in (exp.UnixToTime.SECONDS, exp.UnixToTime.MILLIS):
    #     raise ValueError(f"Scale (unit) must be provided for FROM_UNIXTIME_WITHUNIT")

    scale_str = self.sql(scale).lower()
    if scale_str == "'seconds'":
        return f"CAST(FROM_UNIXTIME_WITHUNIT({timestamp}, 'seconds') AS TIMESTAMP)"
    elif scale_str == "'milliseconds'":
        return f"CAST(FROM_UNIXTIME_WITHUNIT({timestamp}, 'milliseconds') AS TIMESTAMP)"
    else:
        raise ValueError(
            f"Unsupported unit for FROM_UNIXTIME_WITHUNIT: {scale_str} and we only support 'seconds' and 'milliseconds'")


def _build_to_unix_timestamp(args: t.List[exp.Expression]) -> exp.Func:
    value = seq_get(args, 0)

    # If value is a string literal, cast it to TIMESTAMP
    if isinstance(value, exp.Literal) and value.is_string:
        value = exp.Cast(this=value, to=exp.DataType(this="TIMESTAMP"))

    # Check if the value is a cast to TIMESTAMP
    # if not (isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.TIMESTAMP)):
    #     raise ValueError("Argument for TO_UNIX_TIMESTAMP must be of type TIMESTAMP")

    return exp.TimeToUnix(this=value)


def _to_unix_timestamp_sql(self: E6.Generator, expression: exp.TimeToUnix) -> str:
    timestamp = self.sql(expression, "this")
    # if not (isinstance(timestamp, exp.Cast) and timestamp.to.is_type(exp.DataType.Type.TIMESTAMP)):
    if isinstance(timestamp, exp.Literal):
        timestamp = f"CAST({timestamp} AS TIMESTAMP)"
    return f"TO_UNIX_TIMESTAMP({timestamp})"


# need to remove below but kept it for reference to write other methods.
def _parse_timestamp_sql(self: E6.Generator, expression: exp.StrToTime) -> str:
    format_str = expression.args.get("format").this
    format_string = format_str[0]
    # format_str = self.sql(expression, "format"),
    date_expr = self.sql(expression, "this")
    if isinstance(date_expr, str):
        date_expr = f"CAST({date_expr} AS TIMESTAMP)"
    if self.sql(expression, "zone"):
        zone = self.sql(expression, "zone")
        return f"PARSE_TIMESTAMP({format_string},{date_expr},{zone})"
    return f"PARSE_TIMESTAMP({format_string},{date_expr})"


def _build_convert_timezone(args: t.List) -> t.Union[exp.Anonymous, exp.FromTimeZone]:
    if len(args) == 3:
        return exp.Anonymous(this="CONVERT_TIMEZONE", expressions=args)
    return exp.AtTimeZone(this=seq_get(args, 1), zone=seq_get(args, 0))


def _build_datetime_for_DT(args: t.List) -> exp.AtTimeZone:
    if len(args) == 1:
        return exp.AtTimeZone(this=seq_get(args, 0), zone='UTC')
    return exp.AtTimeZone(this=seq_get(args, 0), zone=seq_get(args, 1))


def _build_regexp_extract(args: t.List) -> RegexpExtract:
    expr = seq_get(args, 0)
    pattern = seq_get(args, 1)

    # if exp.DataType.is_type(pattern, exp.DataType.Type.TEXT) or exp.DataType.is_type(pattern, exp.DataType.Type.INT) or isinstance():
    #     return exp.RegexpExtract(this=expr, expression=pattern)
    # else:
    #     raise ValueError("regexp_extract only supports integer and string datatypes")

    return exp.RegexpExtract(this=expr, expression=pattern)

def format_time_for_parsefunctions(expression):
    format_str = expression.this if isinstance(expression, exp.Literal) else expression
    for key, value in E6().TIME_MAPPING_for_parse_functions.items():
        format_str = format_str.replace(key, value)
    return format_str


class E6(Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.LOWERCASE

    TIME_MAPPING = {
        "y": "%Y",
        "Y": "%Y",
        "YYYY": "%Y",
        "yyyy": "%Y",
        "YY": "%y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "hh": "%I",
        "h": "%-I",
        "mm": "%M",
        "m": "%-M",
        "ss": "%S",
        "s": "%-S",
    }

    TIME_MAPPING_for_parse_functions = {
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
        "%M": "%M",
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

    UNIT_PART_MAPPING = {
        "'milliseconds'": "MILLISECOND",
        "'millisecond'": "MILLISECOND",
        "'seconds'": "SECOND",
        "'second'": "SECOND",
        "'minutes'": "MINUTE",
        "'minute'": "MINUTE",
        "'hours'": "HOUR",
        "'hour'": "HOUR",
        "'day'": "DAY",
        "'month'": "MONTH",
        "'year'": "YEAR",
        "'week'": "WEEK",
        "'quarter'": "QUARTER"
    }

    def format_time(self, expression):
        if expression.args.get("format") is None:
            return None
        format_str = expression.args.get("format").this,
        format_string = format_str[0]
        for key, value in E6().TIME_MAPPING.items():
            format_string = format_string.replace(value, key)
        return format_str

    def quote_identifier(self, expression: E, identify: bool = False) -> E:
        keywords_to_quote = {"ABS", "ABSENT", "ABSOLUTE", "ACTION", "ADA", "ADD", "ADMIN", "AFTER", "ALL", "ALLOCATE", "ALLOW", "ALTER", "ALWAYS", "AND", "ANY", "APPLY", "ARE", "ARRAY", "ARRAY_AGG", "ARRAY_CONCAT_AGG", "ARRAY_MAX_CARDINALITY", "AS", "ASC", "ASENSITIVE", "ASSERTION", "ASSIGNMENT", "ASYMMETRIC", "AT", "ATOMIC", "ATTRIBUTE", "ATTRIBUTES", "AUTHORIZATION", "AVG", "BEFORE", "BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION", "BERNOULLI", "BETWEEN", "BIGINT", "BINARY", "BIT", "BLOB", "BOOLEAN", "BOTH", "BREADTH", "BY", "C", "CALL", "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CATALOG_NAME", "CEIL", "CEILING", "CENTURY", "CHAIN", "CHAR", "CHAR_LENGTH", "CHARACTER", "CHARACTER_LENGTH", "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CHARACTERISTICS", "CHARACTERS", "CHECK", "CLASSIFIER", "CLASS_ORIGIN", "CLOB", "CLOSE", "COALESCE", "COBOL", "COLLATE", "COLLATION", "COLLATION_CATALOG", "COLLATION_NAME", "COLLATION_SCHEMA", "COLLECT", "COLUMN", "COLUMN_NAME", "COMMAND_FUNCTION", "COMMAND_FUNCTION_CODE", "COMMIT", "COMMITTED", "CONDITION", "CONDITIONAL", "CONDITION_NUMBER", "CONNECT", "CONNECTION", "CONNECTION_NAME", "CONSTRAINT", "CONSTRAINT_CATALOG", "CONSTRAINT_NAME", "CONSTRAINT_SCHEMA", "CONSTRAINTS", "CONSTRUCTOR", "CONTAINS", "CONTINUE", "CONVERT", "CORR", "CORRESPONDING", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CURSOR_NAME", "CYCLE", "DATA", "DATABASE", "DATE", "DATETIME_INTERVAL_CODE", "DATETIME_INTERVAL_PRECISION", "DAY", "DAYS", "DEALLOCATE", "DEC", "DECADE", "DECIMAL", "DECLARE", "DEFAULT_", "DEFAULTS", "DEFERRABLE", "DEFERRED", "DEFINE", "DEFINED", "DEFINER", "DEGREE", "DELETE", "DENSE_RANK", "DEPTH", "DEREF", "DERIVED", "DESC", "DESCRIBE", "DESCRIPTION", "DESCRIPTOR", "DETERMINISTIC", "DIAGNOSTICS", "DISALLOW", "DISCONNECT", "DISPATCH", "DISTINCT", "DOMAIN", "DOT_FORMAT", "DOUBLE", "DOW", "DOY", "DROP", "DYNAMIC", "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH", "ELEMENT", "ELSE", "EMPTY", "ENCODING", "END", "END_EXEC", "END_FRAME", "END_PARTITION", "EPOCH", "EQUALS", "ERROR", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXCLUDE", "EXCLUDING", "EXEC", "EXECUTE", "EXISTS", "EXP", "EXPLAIN", "EXTEND", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FILTER", "FINAL", "FIRST", "FIRST_VALUE", "FLOAT", "FLOOR", "FOLLOWING", "FOR", "FORMAT", "FOREIGN", "FORTRAN", "FOUND", "FRAC_SECOND", "FRAME_ROW", "FREE", "FROM", "FULL", "FUNCTION", "FUSION", "G", "GENERAL", "GENERATED", "GEOMETRY", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GRANTED", "GROUP", "GROUP_CONCAT", "GROUPING", "GROUPS", "HAVING", "HIERARCHY", "HOLD", "HOP", "HOUR", "HOURS", "IDENTITY", "IGNORE", "ILIKE", "IMMEDIATE", "IMMEDIATELY", "IMPLEMENTATION", "IMPORT", "IN", "INCLUDE", "INCLUDING", "INCREMENT", "INDICATOR", "INITIAL", "INITIALLY", "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSTANCE", "INSTANTIABLE", "INT", "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "INVOKER", "IS", "ISODOW", "ISOYEAR", "ISOLATION", "JAVA", "JOIN", "JSON", "JSON_ARRAY", "JSON_ARRAYAGG", "JSON_EXISTS", "JSON_OBJECT", "JSON_OBJECTAGG", "JSON_QUERY", "JSON_VALUE", "K", "KEY", "KEY_MEMBER", "KEY_TYPE", "LABEL", "LAG", "LANGUAGE", "LARGE", "LAST", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEFT", "LENGTH", "LEVEL", "LIBRARY", "LIKE", "LIKE_REGEX", "LIMIT", "TOP", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATOR", "LOWER", "M", "MAP", "MATCH", "MATCHED", "MATCHES", "MATCH_NUMBER", "MATCH_RECOGNIZE", "MAX", "MAXVALUE", "MEASURES", "MEMBER", "MERGE", "MESSAGE_LENGTH", "MESSAGE_OCTET_LENGTH", "MESSAGE_TEXT", "METHOD", "MICROSECOND", "MILLISECOND", "MILLISECONDS", "MILLENNIUM", "MIN", "MINUTE", "MINUTES", "MINVALUE", "MOD", "MODIFIES", "MODULE", "MONTH", "MONTHS", "MORE_", "MULTISET", "MUMPS", "NAME", "NAMES", "NANOSECOND", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NESTING", "NEW", "NEXT", "NO", "NONE", "NORMALIZE", "NORMALIZED", "NOT", "NTH_VALUE", "NTILE", "NULL", "NULLABLE", "NULLIF", "NULLS", "NUMBER", "NUMERIC", "OBJECT", "OCCURRENCES_REGEX", "OCTET_LENGTH", "OCTETS", "OF", "OFFSET", "OLD", "OMIT", "ON", "ONE", "ONLY", "OPEN", "OPTION", "OPTIONS", "OR", "ORDER", "ORDERING", "ORDINALITY", "OTHERS", "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING", "PAD", "PARAMETER", "PARAMETER_MODE", "PARAMETER_NAME", "PARAMETER_ORDINAL_POSITION", "PARAMETER_SPECIFIC_CATALOG", "PARAMETER_SPECIFIC_NAME", "PARAMETER_SPECIFIC_SCHEMA", "PARTIAL", "PARTITION", "PASCAL", "PASSING", "PASSTHROUGH", "PAST", "PATH", "PATTERN", "PER", "PERCENT", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "PERIOD", "PERMUTE", "PIVOT", "PLACING", "PLAN", "PLI", "PORTION", "POSITION", "POSITION_REGEX", "POWER", "PRECEDES", "PRECEDING", "PRECISION", "PREPARE", "PRESERVE", "PREV", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC", "QUARTER", "RANGE", "RANK", "READ", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELATIVE", "RELEASE", "REPEATABLE", "REPLACE", "RESET", "RESPECT", "RESTART", "RESTRICT", "RESULT", "RETURN", "RETURNED_CARDINALITY", "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH", "RETURNED_SQLSTATE", "RETURNING", "RETURNS", "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE", "ROUTINE_CATALOG", "ROUTINE_NAME", "ROUTINE_SCHEMA", "ROW", "ROW_COUNT", "ROW_NUMBER", "ROWS", "RUNNING", "SAVEPOINT", "SCALAR", "SCALE", "SCHEMA", "SCHEMA_NAME", "SCOPE", "SCOPE_CATALOGS", "SCOPE_NAME", "SCOPE_SCHEMA", "SCROLL", "SEARCH", "SECOND", "SECONDS", "SECTION", "SECURITY", "SEEK", "SELECT", "SELF", "SENSITIVE", "SEPARATOR", "SEQUENCE", "SERIALIZABLE", "SERVER", "SERVER_NAME", "SESSION", "SESSION_USER", "SET", "SETS", "SET_MINUS", "SHOW", "SIMILAR", "SIMPLE", "SIZE", "SKIP_", "SMALLINT", "SOME", "SOURCE", "SPACE", "SPECIFIC", "SPECIFIC_NAME", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIGINT", "SQL_BINARY", "SQL_BIT", "SQL_BLOB", "SQL_BOOLEAN", "SQL_CHAR", "SQL_CLOB", "SQL_DATE", "SQL_DECIMAL", "SQL_DOUBLE", "SQL_FLOAT", "SQL_INTEGER", "SQL_INTERVAL_DAY", "SQL_INTERVAL_DAY_TO_HOUR", "SQL_INTERVAL_DAY_TO_MINUTE", "SQL_INTERVAL_DAY_TO_SECOND", "SQL_INTERVAL_HOUR", "SQL_INTERVAL_HOUR_TO_MINUTE", "SQL_INTERVAL_HOUR_TO_SECOND", "SQL_INTERVAL_MINUTE", "SQL_INTERVAL_MINUTE_TO_SECOND", "SQL_INTERVAL_MONTH", "SQL_INTERVAL_SECOND", "SQL_INTERVAL_YEAR", "SQL_INTERVAL_YEAR_TO_MONTH", "SQL_LONGVARBINARY", "SQL_LONGVARCHAR", "SQL_LONGVARNCHAR", "SQL_NCHAR", "SQL_NCLOB", "SQL_NUMERIC", "SQL_NVARCHAR", "SQL_REAL", "SQL_SMALLINT", "SQL_TIME", "SQL_TIMESTAMP", "SQL_TINYINT", "SQL_TSI_DAY", "SQL_TSI_FRAC_SECOND", "SQL_TSI_HOUR", "SQL_TSI_MICROSECOND", "SQL_TSI_MINUTE", "SQL_TSI_MONTH", "SQL_TSI_QUARTER", "SQL_TSI_SECOND", "SQL_TSI_WEEK", "SQL_TSI_YEAR", "SQL_VARBINARY", "SQL_VARCHAR", "SQRT", "START", "STATE", "STATEMENT", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "STREAM", "STRING_AGG", "STRUCTURE", "STYLE", "SUBCLASS_ORIGIN", "SUBMULTISET", "SUBSET", "SUBSTITUTE", "SUBSTRING", "SUBSTRING_REGEX", "SUCCEEDS", "SUM", "SYMMETRIC", "SYSTEM", "SYSTEM_TIME", "SYSTEM_USER", "TABLE", "TABLE_NAME", "TABLESAMPLE", "TEMPORARY", "THEN", "TIES", "TIME", "TIMESTAMP", "TIMESTAMP_TZ", "TIMESTAMPADD", "TIMESTAMPDIFF", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYINT", "TO", "TOP_LEVEL_COUNT", "TRAILING", "TRANSACTION", "TRANSACTIONS_ACTIVE", "TRANSACTIONS_COMMITTED", "TRANSACTIONS_ROLLED_BACK", "TRANSFORM", "TRANSFORMS", "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION", "TREAT", "TRIGGER", "TRIGGER_CATALOG", "TRIGGER_NAME", "TRIGGER_SCHEMA", "TRIM", "TRIM_ARRAY", "TRUE", "TRUNCATE", "TRY_CAST", "TUMBLE", "TYPE", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", "UNCONDITIONAL", "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNPIVOT", "UNNAMED", "UNNEST", "UPDATE", "UPPER", "UPSERT", "USAGE", "USER", "USER_DEFINED_TYPE_CATALOG", "USER_DEFINED_TYPE_CODE", "USER_DEFINED_TYPE_NAME", "USER_DEFINED_TYPE_SCHEMA", "USING", "UTF8", "UTF16", "UTF32", "VALUE", "VALUES", "VALUE_OF", "VAR_POP", "VAR_SAMP", "VARBINARY", "VARCHAR", "VARYING", "VERSION", "VERSIONING", "VIEW", "WEEK", "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRAPPER", "WRITE", "XML", "YEAR", "YEARS", "ZONE"}

        if (
                isinstance(expression, exp.Identifier)
                and expression.name.lower() in keywords_to_quote
        ):
            expression.set("quoted", True)
            return expression
        return expression

    class Tokenizer(tokens.Tokenizer):
        STRING_ESCAPES = ["\\"]
        # identifiers ' worked fine for strings in functions
        IDENTIFIERS = ['"']
        QUOTES = ["'"]
        COMMENTS = ["--", "//", ("/*", "*/")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }

    class Parser(parser.Parser):
        SUPPORTED_CAST_TYPES = {
            "CHAR", "VARCHAR", "INT", "BIGINT", "BOOLEAN",
            "DATE", "FLOAT", "DOUBLE", "TIMESTAMP", "DECIMAL"
        }

        def _parse_cast(self, strict: bool, safe: t.Optional[bool] = None) -> exp.Expression:
            cast_expression = super()._parse_cast(strict, safe)

            if isinstance(cast_expression, (exp.Cast, exp.TryCast)):
                target_type = cast_expression.to.this

                if target_type.name not in self.SUPPORTED_CAST_TYPES:
                    self.raise_error(f"Unsupported cast type: {target_type}")

            return cast_expression

        # this is the temporary implementation assuming we don't support agg functions in any part of lambda function
        def _parse_filter_array(self) -> ValueError | ArrayFilter:
            array_expr = seq_get(self, 0)
            lambda_expr = seq_get(self, 1)

            root_node = lambda_expr.args.get('this')

            def does_root_node_contain_AGG_expr(root_node) -> bool:
                # check if root is of Agg
                if isinstance(root_node, exp.AggFunc):
                    return True
                # traverse through all the children and check for the same

                if not isinstance(root_node, exp.Expression):  # check if root_node is leaf node
                    return False

                child_nodes: dict = root_node.args
                for key, value in child_nodes.items():
                    contains_agg: bool = does_root_node_contain_AGG_expr(value)
                    if contains_agg:
                        return True
                return False

            if does_root_node_contain_AGG_expr(root_node):
                # parser.Parser.raise_error(parser.Parser,message=
                #                           f"Lambda expressions in filter functions are not supported in 'IN' clause or on aggregate functions")
                raise ValueError(
                    "Lambda expressions in filter functions are not supported in 'IN' clause or on aggregate functions")

            return exp.ArrayFilter(this=array_expr, expression=lambda_expr)

        def _parse_unnest_sql(self) -> exp.Expression:
            array_expr = seq_get(self, 0)
            if (isinstance(array_expr, exp.Cast) and not exp.DataType.is_type(array_expr.to.this,
                                                                              exp.DataType.Type.ARRAY)) or (
                    not isinstance(array_expr, exp.Array)):
                raise ValueError(f"UNNEST function only supports array type")

            return exp.Explode(this=array_expr)

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "APPROX_COUNT_DISTINCT": exp.ApproxDistinct,
            "APPROX_QUANTILES": exp.ApproxQuantile.from_arg_list,
            "ARBITRARY": exp.AnyValue,
            "ARRAY_AGG": exp.ArrayAgg.from_arg_list,
            "ARRAY_CONCAT": exp.ArrayConcat,
            "ARRAY_CONTAINS": exp.ArrayContains,
            "ARRAY_JOIN": exp.ArrayToString.from_arg_list,
            "ARRAY_TO_STRING": exp.ArrayToString,
            "BITWISE_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BITWISE_OR": binary_from_function(exp.BitwiseOr),
            "BITWISE_XOR": binary_from_function(exp.BitwiseXor),
            "CAST": _parse_cast,
            "CHARACTER_LENGTH": _build_with_arg_as_text(exp.Length),
            "CHARINDEX": locate_to_strposition,
            "CHAR_LENGTH": _build_with_arg_as_text(exp.Length),
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
            "CONVERT_TIMEZONE": _build_convert_timezone,
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
            "DAYS": exp.Day,
            "ELEMENT_AT": lambda args: exp.Bracket(
                this=seq_get(args, 0), expressions=[seq_get(args, 1)], offset=1, safe=True
            ),
            "FILTER_ARRAY": _parse_filter_array,
            "FIRST_VALUE": exp.FirstValue,
            "FORMAT_DATE": lambda args: exp.TimeToStr(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)), format=seq_get(args, 1)
            ),
            "FORMAT_TIMESTAMP": lambda args: exp.TimeToStr(
                this=exp.TsOrDsToTimestamp(this=seq_get(args, 0)), format=seq_get(args, 1)
            ),
            "FROM_UNIXTIME_WITHUNIT": _build_from_unixtime_withunit,
            "GREATEST": exp.Max,
            "json_extract": exp.JSONExtract.from_arg_list,
            "LAST_DAY": lambda args: exp.LastDay(this=seq_get(args, 0)),
            "LAST_VALUE": exp.LastValue,
            "LAG": lambda args: exp.Lag(
                this=seq_get(args, 0), offset=seq_get(args, 1)
            ),
            "LEAD": lambda args: exp.Lead(
                this=seq_get(args, 0), offset=seq_get(args, 1)
            ),
            "LEFT": _build_with_arg_as_text(exp.Left),
            "LEN": _build_with_arg_as_text(exp.Length),
            "LENGTH": _build_with_arg_as_text(exp.Length),
            "LEAST": exp.Min,
            "LISTAGG": exp.GroupConcat.from_arg_list,
            "LOCATE": locate_to_strposition,
            "LOG": exp.Log,
            "MD5": exp.MD5Digest.from_arg_list,
            "MOD": lambda args: parser.build_mod(args),
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "NULLIF": exp.Nullif,
            "PARSE_DATE": _build_formatted_time_with_or_without_zone(exp.StrToDate, "E6"),
            "PARSE_DATETIME": _build_formatted_time_with_or_without_zone(exp.StrToTime, "E6"),
            "PARSE_TIMESTAMP": _build_formatted_time_with_or_without_zone(exp.StrToTime, "E6"),
            "POWER": exp.Pow,
            "REGEXP_CONTAINS": exp.RegexpLike.from_arg_list,
            "REGEXP_EXTRACT": _build_regexp_extract,
            "REGEXP_LIKE": exp.RegexpLike.from_arg_list,
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0), expression=seq_get(args, 1), replacement=seq_get(args, 2),
            ),
            "REPLACE": exp.RegexpReplace.from_arg_list,
            "RIGHT": _build_with_arg_as_text(exp.Right),
            "SHIFTRIGHT": binary_from_function(exp.BitwiseRightShift),
            "SHIFTLEFT": binary_from_function(exp.BitwiseLeftShift),
            "SIZE": exp.ArraySize.from_arg_list,
            "SPLIT": exp.Split.from_arg_list,
            "SPLIT_PART": exp.RegexpSplit.from_arg_list,
            "STARTSWITH": exp.StartsWith,
            "STARTS_WITH": exp.StartsWith,
            "STDDEV": exp.Stddev,
            "STDDEV_POP": exp.StddevPop,
            "STRING_AGG": exp.GroupConcat.from_arg_list,
            "STRPOS": exp.StrPosition.from_arg_list,
            "SUBSTR": exp.Substring,
            "TIMESTAMP": _build_timestamp,
            "TIMESTAMP_ADD": lambda args: exp.TimestampAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "TIMESTAMP_DIFF": exp.TimestampDiff.from_arg_list,
            "TO_CHAR": build_formatted_time(exp.TimeToStr, "E6"),
            "TO_DATE": build_formatted_time(exp.StrToDate, "E6"),
            "TO_TIMESTAMP": _build_datetime("TO_TIMESTAMP", exp.DataType.Type.TIMESTAMP),
            "TO_TIMESTAMP_NTZ": _build_datetime("TO_TIMESTAMP_NTZ", exp.DataType.Type.TIMESTAMP),
            "TO_UNIX_TIMESTAMP": _build_to_unix_timestamp,
            "TO_VARCHAR": build_formatted_time(exp.TimeToStr, "E6"),
            "TRUNC": date_trunc_to_time,
            "UNNEST": _parse_unnest_sql,
            "WEEK": exp.Week,
            "YEAR": exp.Year,

        }

    class Generator(generator.Generator):
        EXTRACT_ALLOWS_QUOTES = False
        NVL2_SUPPORTED = True
        LAST_DAY_SUPPORTS_DATE_PART = False
        INTERVAL_ALLOWS_PLURAL_FORM = False
        NULL_ORDERING_SUPPORTED = None

        # def select_sql(self, expression: exp.Select) -> str:
        #     def collect_aliases_and_projections(expressions):
        #         aliases = {}
        #         projections = []
        #         for e in expressions:
        #             if isinstance(e, exp.Alias):
        #                 alias = e.args.get("alias").sql(dialect=self.dialect)
        #                 aliases[alias] = e.this
        #                 projections.append(e)
        #             else:
        #                 projections.append(e)
        #         return aliases, projections
        #
        #     def find_reused_aliases(projections, aliases):
        #         reused_aliases = set()
        #         for e in projections:
        #             if isinstance(e, exp.Alias):
        #                 alias = e.args.get("alias").sql(dialect=self.dialect)
        #                 for other_e in projections:
        #                     if other_e is not e and alias in other_e.sql(dialect=self.dialect):
        #                         reused_aliases.add(alias)
        #                         break
        #         return reused_aliases
        #
        #     def create_subquery(projections, reused_aliases):
        #         subquery_expressions = []
        #         new_projections = []
        #         for e in projections:
        #             if isinstance(e, exp.Alias):
        #                 alias = e.args.get("alias")
        #                 # subquery_expressions.append(e.this.as_(alias))
        #                 if alias.sql(dialect=self.dialect) in reused_aliases:
        #                     subquery_expressions.append(e.this.as_(alias))
        #                     new_projections.append(exp.column(f"t.{alias.sql(dialect=self.dialect)}"))
        #                 else:
        #                     new_projections.append(e)
        #             else:
        #                 new_projections.append(e)
        #
        #         subquery = exp.Select(expressions=subquery_expressions).subquery(alias="t")
        #         # Adjust projections to replace reused aliases with subquery reference
        #         adjusted_projections = []
        #         for e in new_projections:
        #             if isinstance(e, exp.Alias):
        #                 alias = e.args.get("alias").sql(dialect=self.dialect)
        #                 for alias_re in reused_aliases:
        #                     if alias_re in e.this.sql(dialect=self.dialect):
        #                         e = e.transform(lambda node: exp.column(f"t.{alias_re}") if isinstance(node,
        #                                                                                                exp.Column) and node.sql(
        #                             dialect=self.dialect) == alias_re else node)
        #                 adjusted_projections.append(e)
        #             else:
        #                 for alias_re in reused_aliases:
        #                     if alias_re in e.this.sql(dialect=self.dialect):
        #                         e = e.transform(lambda node: exp.column(f"t.{alias_re}") if isinstance(node,
        #                                                                                                exp.Column) and node.sql(
        #                             dialect=self.dialect) == alias_re else node)
        #                 adjusted_projections.append(e)
        #
        #         return adjusted_projections, subquery
        #
        #     # Collect all the aliases and projections defined in the SELECT clause
        #     aliases, projections = collect_aliases_and_projections(expression.expressions)
        #
        #     # Find reused aliases in the projections
        #     reused_aliases = find_reused_aliases(projections, aliases)
        #
        #     if reused_aliases:
        #         new_projections, subquery = create_subquery(projections, reused_aliases)
        #
        #         # Ensure the FROM clause is added if missing
        #         if expression.args.get("from"):
        #             from_clause = expression.args["from"]
        #             from_clause.append(subquery)
        #         else:
        #             # expression.set("from", subquery)
        #             expression.set("from",subquery)
        #
        #         expression.set("expressions", new_projections)
        #
        #     return super().select_sql(expression)

        def format_time(self, expression, **kwargs):
            if expression.args.get("format") is None:
                return None
            format_str = expression.args.get("format").this
            format_string = format_str
            for key, value in E6().TIME_MAPPING.items():
                format_string = format_string.replace(value, key)
            return format_string

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            # Get the target type of the cast expression
            target_type = expression.to.this
            # Find the corresponding type in E6 from the mapping
            e6_type = self.CAST_SUPPORTED_TYPE_MAPPING.get(target_type, target_type)
            # Generate the SQL for casting with the mapped type
            return f"CAST({self.sql(expression.this)} AS {e6_type})"

        def interval_sql(self: E6.Generator, expression: exp.Interval) -> str:
            if expression.this and expression.unit:
                value = expression.this.name
                unit = expression.unit.name
                interval_str = f"INTERVAL {value} {unit}"
                return interval_str
            else:
                return ""

        # Need to look at the problem here regarding double casts appearing
        def _last_day_sql(self: E6.Generator, expression: exp.LastDay) -> str:
            # date_expr = self.sql(expression,"this")
            date_expr = expression.args.get("this")
            date_expr = date_expr
            if isinstance(date_expr, exp.Literal):
                date_expr = f"CAST({date_expr} AS DATE)"
            return f"LAST_DAY({date_expr})"

        def extract_sql(self: E6.Generator, expression: exp.Extract) -> str:
            unit = expression.this.name
            expression_sql = self.sql(expression, "expression")
            extract_str = f"EXTRACT({unit} FROM {expression_sql})"
            return extract_str

        def filter_array_sql(self: E6.Generator, expression: exp.ArrayFilter) -> str:
            cond = expression.expression
            if isinstance(cond, exp.Lambda) and len(cond.expressions) == 1:
                alias = cond.expressions[0]
                cond = cond.this
            elif isinstance(cond, exp.Predicate):
                alias = "_u"
            else:
                self.unsupported("Unsupported filter condition")
                return ""

            # Check for aggregate functions
            if any(isinstance(node, exp.AggFunc) for node in cond.find_all(exp.Expression)):
                raise ValueError("array filter's Lambda expression are not supported with aggregate functions")
                return ""

            lambda_expr = f"{alias} -> {self.sql(cond)}"
            return f"FILTER_ARRAY({self.sql(expression.this)}, {lambda_expr})"

        def unnest_sql(self: E6.Generator, expression: exp.Explode) -> str:
            array_expr = expression.this
            if (isinstance(array_expr, exp.Cast) and not exp.DataType.is_type(array_expr.to.this,
                                                                              exp.DataType.Type.ARRAY)) or (
                    not isinstance(array_expr, exp.Array)):
                raise ValueError("UNNEST in E6 will only support Type ARRAY")
                return ""
            return f"UNNEST({array_expr})"

        def format_date_sql(self: E6.Generator, expression: exp.TimeToStr) -> str:
            date_expr = expression.this
            format_expr = self.format_time(expression)
            if isinstance(date_expr, exp.CurrentDate) or isinstance(date_expr, exp.CurrentTimestamp) or isinstance(
                    date_expr, exp.TsOrDsToDate):
                return f"FORMAT_DATE({date_expr},'{format_expr}')"
            if (not exp.DataType.is_type(date_expr, exp.DataType.Type.DATE) or exp.DataType.is_type(date_expr,
                                                                                                    exp.DataType.Type.TIMESTAMP)) or (
                    (isinstance(date_expr, exp.Cast) and not (date_expr.to.this.name == 'TIMESTAMP')) or (
                    isinstance(date_expr, exp.Cast) and not (date_expr.to.this.name == 'DATE'))):
                date_expr = f"CAST({date_expr} AS DATE)"
            return f"FORMAT_DATE({date_expr},'{format_expr}')"

        def tochar_sql(self, expression: exp.ToChar) -> str:
            date_expr = expression.this
            if (isinstance(date_expr, exp.Cast) and not (date_expr.to.this.name == 'TIMESTAMP')) or (
                    not isinstance(date_expr, exp.Cast) and
                    not exp.DataType.is_type(date_expr, exp.DataType.Type.DATE) or exp.DataType.is_type(date_expr,
                                                                                                        exp.DataType.Type.TIMESTAMP)):
                date_expr = f"CAST({date_expr} AS TIMESTAMP)"
            format_expr = self.format_time(expression)
            return f"TO_CHAR({date_expr},'{format_expr}')"

        # def struct_sql(self, expression: exp.Struct) -> str:
        #     struct_expr = expression.expressions
        #     return f"{struct_expr}"

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: rename_func("ARBITRARY"),
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ApproxQuantile: rename_func("APPROX_QUANTILES"),
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArrayConcat: rename_func("ARRAY_CONCAT"),
            exp.ArrayContains: rename_func("ARRAY_CONTAINS"),
            exp.ArrayFilter: filter_array_sql,
            exp.ArrayToString: rename_func("ARRAY_JOIN"),
            exp.ArraySize: rename_func("size"),
            exp.AtTimeZone: lambda self, e: self.func(
                "DATETIME", e.this, e.args.get("zone")
            ),
            exp.BitwiseLeftShift: lambda self, e: self.func("SHIFTLEFT", e.this, e.expression),
            exp.BitwiseNot: lambda self, e: self.func("BITWISE_NOT", e.this),
            exp.BitwiseOr: lambda self, e: self.func("BITWISE_OR", e.this, e.expression),
            exp.BitwiseRightShift: lambda self, e: self.func("SHIFTRIGHT", e.this, e.expression),
            exp.BitwiseXor: lambda self, e: self.func("BITWISE_XOR", e.this, e.expression),
            exp.Bracket: lambda self, e: self.func("ELEMENT_AT", e.this, e.expression),
            exp.CurrentDate: lambda *_: "CURRENT_DATE",
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.Date: lambda self, e: self.func("DATE", e.this),
            exp.DateAdd: lambda self, e: self.func(
                "DATE_ADD",
                unit_to_str(e),
                _to_int(e.expression),
                e.this,
            ),
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
            ),
            exp.DateTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.Explode: unnest_sql,
            exp.Extract: extract_sql,
            exp.FirstValue: rename_func("FIRST_VALUE"),
            exp.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", "'UTC'", e.args.get("zone"), e.this
            ),
            exp.GroupConcat: lambda self, e: self.func(
                "LISTAGG" if e.args.get("within_group") else "STRING_AGG",
                e.this,
                e.args.get("separator") or exp.Literal.string(',')
            ),
            exp.Interval: interval_sql,
            exp.JSONExtract: lambda self, e: self.func("json_extract", e.this, e.expression),
            exp.JSONExtractScalar: lambda self, e: self.func("json_extract", e.this, e.expression),
            exp.Lag: lambda self, e: self.func("LAG", e.this, e.args.get("offset")),
            exp.LastDay: _last_day_sql,
            exp.LastValue: rename_func("LAST_VALUE"),
            exp.Lead: lambda self, e: self.func("LEAD", e.this, e.args.get("offset")),
            exp.Length: rename_func("LENGTH"),
            exp.Log: rename_func("LN"),
            exp.Max: max_or_greatest,
            exp.MD5Digest: lambda self, e: self.func("MD5", e.this),
            exp.Min: min_or_least,
            exp.Mod: lambda self, e: self.func("MOD", e.this, e.expression),
            exp.Pow: rename_func("POWER"),
            exp.RegexpExtract: rename_func("REGEXP_EXTRACT"),
            exp.RegexpLike: lambda self, e: self.func("REGEXP_LIKE", e.this, e.expression),
            exp.RegexpReplace: regexp_replace_sql,
            exp.RegexpSplit: rename_func("SPLIT_PART"),
            # exp.Select: select_sql,
            exp.Split: rename_func("SPLIT"),
            exp.Stddev: rename_func("STDDEV"),
            exp.StddevPop: rename_func("STDDEV_POP"),
            exp.StrPosition: lambda self, e: self.func(
                "LOCATE", e.args.get("substr"), e.this, e.args.get("position")
            ),
            exp.StrToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.StartsWith: rename_func("STARTS_WITH"),
            # exp.Struct: struct_sql,
            exp.TimeToStr: format_date_sql,
            exp.TimeToUnix: rename_func("TO_UNIX_TIMESTAMP"),
            exp.Timestamp: lambda self, e: self.func("TIMESTAMP", e.this),
            exp.TimestampAdd: lambda self, e: self.func(
                "TIMESTAMP_ADD", unit_to_str(e), e.expression, e.this
            ),
            exp.TimestampDiff: lambda self, e: self.func(
                "TIMESTAMP_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
            ),
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", unit_to_str(e), e.this),
            exp.ToChar: tochar_sql,
            exp.Trim: lambda self, e: self.func("TRIM", e.this, ' '),
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
            exp.UnixToTime: _from_unixtime_withunit_sql,
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
        }

        UNSIGNED_TYPE_MAPPING = {
            exp.DataType.Type.UBIGINT: "BIGINT",
            exp.DataType.Type.UINT: "INT",
            exp.DataType.Type.UMEDIUMINT: "INT",
            exp.DataType.Type.USMALLINT: "INT",
            exp.DataType.Type.UTINYINT: "INT",
            exp.DataType.Type.UDECIMAL: "DECIMAL",
        }

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
            exp.DataType.Type.DECIMAL: "DECIMAL"
        }

        TYPE_MAPPING = {
            **UNSIGNED_TYPE_MAPPING,
            **CAST_SUPPORTED_TYPE_MAPPING,
            exp.DataType.Type.JSON: "JSON",
            exp.DataType.Type.STRUCT: "STRUCT",
            exp.DataType.Type.ARRAY: "ARRAY"
        }
