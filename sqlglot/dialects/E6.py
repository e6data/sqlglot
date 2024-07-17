from __future__ import annotations

import typing as t

import sqlglot

from sqlglot import exp, generator, parser, tokens, transforms
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
)
from sqlglot.helper import flatten, is_float, is_int, seq_get

if t.TYPE_CHECKING:
    from sqlglot._typing import E

def format_time(expression):
    # format_str = expression.this if isinstance(expression, exp.Literal) else expression
    format_str = expression.args['format'].this
    for key, value in E6().TIME_MAPPING.items():
        format_str = format_str.replace(key, value)
    return format_str


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
                format=E6().format_time_for_parsefunctions(
                    seq_get(args, 0)
                    or (E6().TIME_FORMAT if default is True else default or None)
                )
            )
        return exp_class(
            this=seq_get(args, 1),
            format=E6().format_time_for_parsefunctions(
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
            unit = args[0]
            date_expr1 = args[1]
            date_expr2 = args[2]
        else:
            raise ValueError("Incorrect number of arguments for DATEDIFF function")

        return expression_class(this=date_expr1, expression=date_expr2, unit=unit)

    return _builder


# how others use use from_unixtime_withunit and how E6 differs.
def _from_unixtime_withunit_sql(self: E6.Generator, expression: exp.UnixToTime) -> str:
    timestamp = self.sql(expression, "this")
    scale = expression.args.get("scale")
    if scale is None:
        raise ValueError("Unit 'seconds' or 'milliseconds' need to be provided")
    if scale not in (exp.UnixToTime.SECONDS, exp.UnixToTime.MILLIS):
        raise ValueError(f"Scale (unit) must be provided for FROM_UNIXTIME_WITHUNIT")

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
    if not (isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.TIMESTAMP)):
        raise ValueError("Argument for TO_UNIX_TIMESTAMP must be of type TIMESTAMP")

    return exp.TimeToUnix(this=value)


def _to_unix_timestamp_sql(self: E6.Generator, expression: exp.TimeToUnix) -> str:
    timestamp = self.sql(expression, "this")
    # if not (isinstance(timestamp, exp.Cast) and timestamp.to.is_type(exp.DataType.Type.TIMESTAMP)):
    if isinstance(timestamp, exp.Literal):
        timestamp = f"CAST({timestamp} AS TIMESTAMP)"
    return f"TO_UNIX_TIMESTAMP({timestamp})"


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

def interval_sql(self: E6.Generator, expression: exp.Interval) -> str:
    if expression.this and expression.unit:
        value = expression.this.name
        unit = expression.unit.name
        interval_str = f"INTERVAL {value} {unit}"
        return interval_str
    else:
        return ""


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

    def format_time_for_parsefunctions(self, expression):
        format_str = expression.this if isinstance(expression, exp.Literal) else expression
        for key, value in E6().TIME_MAPPING_for_parse_functions.items():
            format_str = format_str.replace(key, value)
        return format_str

    def format_time(self, expression):
        # format_str = expression.this if isinstance(expression, exp.Literal) else expression
        format_str = expression.args.get("format").this,
        format_string = format_str[0]
        for key, value in E6().TIME_MAPPING.items():
            format_string = format_string.replace(value, key)
        return format_str

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"']
        QUOTES = ['"']
        COMMENTS = ["--", "//", ("/*", "*/")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
        }

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ARBITRARY": exp.AnyValue,
            "ANY_VALUE": exp.AnyValue,
            "LISTAGG": exp.GroupConcat.from_arg_list,
            "STRING_AGG": exp.GroupConcat.from_arg_list,
            "POWER": exp.Pow,
            "LN": exp.Log,
            "LEFT": _build_with_arg_as_text(exp.Left),
            "RIGHT": _build_with_arg_as_text(exp.Right),
            "CHARACTER_LENGTH": _build_with_arg_as_text(exp.Length),
            "LEN": _build_with_arg_as_text(exp.Length),
            "CHAR_LEN": _build_with_arg_as_text(exp.Length),
            "REPLACE": exp.RegexpReplace.from_arg_list,
            "SUBSTR": exp.Substring,
            "CHARINDEX": locate_to_strposition,
            "LOCATE": locate_to_strposition,
            "SPLIT": exp.Split.from_arg_list,
            "SPLIT_PART": exp.RegexpSplit.from_arg_list,
            "STRPOS": exp.StrPosition.from_arg_list,
            "TO_CHAR": build_formatted_time(exp.TimeToStr, "e6"),
            "TO_VARCHAR": build_formatted_time(exp.TimeToStr, "e6"),
            "STARTS_WITH": exp.StartsWith,
            "STARTSWITH": exp.StartsWith,
            "CURRENT_DATE": exp.CurrentDate.from_arg_list,
            "CURRENT_TIMESTAMP": exp.CurrentTimestamp.from_arg_list,
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "TO_TIMESTAMP": _build_datetime("TO_TIMESTAMP", exp.DataType.Type.TIMESTAMP),
            "TO_DATE": build_formatted_time(exp.StrToDate, "e6"),
            "DATE": _build_date,
            "TIMESTAMP": _build_timestamp,
            "TO_TIMESTAMP_NTZ": _build_datetime("TO_TIMESTAMP_NTZ", exp.DataType.Type.TIMESTAMP),
            "FROM_UNIXTIME_WITHUNIT": _build_from_unixtime_withunit,
            "TO_UNIX_TIMESTAMP": _build_to_unix_timestamp,
            "PARSE_DATE": _build_formatted_time_with_or_without_zone(exp.StrToDate, "E6"),
            "PARSE_DATETIME": _build_formatted_time_with_or_without_zone(exp.StrToTime, "E6"),
            "PARSE_TIMESTAMP": _build_formatted_time_with_or_without_zone(exp.StrToTime, "E6"),
            "DATE_TRUNC": date_trunc_to_time,
            "DATE_ADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATEDIFF": build_datediff(exp.DateDiff),
            "DATE_DIFF": build_datediff(exp.DateDiff),
            "TIMESTAMP_ADD": lambda args: exp.TimestampAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "TIMESTAMP_DIFF": exp.TimestampDiff.from_arg_list,
            "DATEPART": lambda args: exp.Extract(
                this=seq_get(args, 0), expression=seq_get(args, 1)
            ),
            "WEEK": exp.Week,
            "YEAR": exp.Year,
            "DAYS": exp.Day,
            "LAST_DAY": lambda args: exp.LastDay(this=seq_get(args, 0)),
            "FORMAT_DATE": lambda args: exp.TimeToStr(
                this=exp.TsOrDsToDate(this=seq_get(args, 1)), format=seq_get(args, 0)
            ),
            "FORMAT_TIMESTAMP": lambda args: exp.TimeToStr(
                this=exp.TsOrDsToTimestamp(this=seq_get(args, 1)), format=seq_get(args, 0)
            ),
            "DATETIME": _build_datetime_for_DT,
            "CONVERT_TIMEZONE": _build_convert_timezone,
            "NULLIF": exp.Nullif,
            "GREATEST": exp.Max,
            "LEAST": exp.Min,
            "FIRST_VALUE": exp.FirstValue,
            "LAST_VALUE": exp.LastValue,
            "LEAD": lambda args: exp.Lead(
                this=seq_get(args, 0), offset=seq_get(args, 1)
            ),
            "LAG": lambda args: exp.Lag(
                this=seq_get(args, 0), offset=seq_get(args, 1)
            ),
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
            "STDDEV": exp.Stddev,
            "STDDEV_POP": exp.StddevPop,
            "BITWISE_OR": binary_from_function(exp.BitwiseOr),
            "BITWISE_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BITWISE_XOR": binary_from_function(exp.BitwiseXor),
            "SHIFTRIGHT": binary_from_function(exp.BitwiseRightShift),
            "SHIFTLEFT": binary_from_function(exp.BitwiseLeftShift),

        }

    class Generator(generator.Generator):
        EXTRACT_ALLOWS_QUOTES = False
        NVL2_SUPPORTED = True
        LAST_DAY_SUPPORTS_DATE_PART = False
        INTERVAL_ALLOWS_PLURAL_FORM = False

        def format_time_for_parsefunctions(self, expression):
            format_str = expression.args['format'].this
            for key, value in E6.TIME_MAPPING_for_parse_functions.items():
                format_str = format_str.replace(key, value)
            return format_str

        def format_time(self, expression):
            if expression.args.get("format") is None:
                return None
            format_str = expression.args.get("format").this,
            format_string = format_str[0]
            for key, value in E6().TIME_MAPPING.items():
                format_string = format_string.replace(value, key)
            return format_string

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.AnyValue: rename_func("ARBITRARY"),
            exp.GroupConcat: lambda self, e: self.func(
                "LISTAGG" if e.args.get("within_group") else "STRING_AGG",
                e.this,
                e.args.get("separator") or exp.Literal.string(',')
            ),
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.TimeToStr: lambda self, e: self.func(
                "TO_CHAR", exp.cast(e.this, exp.DataType.Type.TIMESTAMP), self.format_time(e)
            ),
            exp.Pow: rename_func("POWER"),
            exp.StrPosition: lambda self, e: self.func(
                "LOCATE", e.args.get("substr"), e.this, e.args.get("position")
            ),
            exp.ToChar: lambda self, e: self.func(
                "TO_CHAR", exp.cast(e.this, exp.DataType.Type.TIMESTAMP), self.format_time(e)
            ),
            exp.Extract: extract_sql,
            exp.Log: rename_func("LN"),
            exp.Length: rename_func("CHAR_LEN"),
            exp.RegexpReplace: rename_func("REPLACE"),
            exp.StartsWith: rename_func("STARTS_WITH"),
            exp.RegexpSplit: rename_func("SPLIT_PART"),
            exp.CurrentDate: lambda *_: "CURRENT_DATE",
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.Date: lambda self, e: f"CAST({self.sql(e.this)} AS DATE)",
            exp.Timestamp: lambda self, e: f"CAST({self.sql(e.this)} AS TIMESTAMP)",
            exp.Trim: lambda self, e: self.func("TRIM", e.this, ' '),
            # As per e6 trim, we only trim white spaces need to be looked upon
            exp.UnixToTime: _from_unixtime_withunit_sql,
            exp.TimeToUnix: rename_func("TO_UNIX_TIMESTAMP"),
            exp.StrToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            # exp.StrToTime: _parse_timestamp_sql,
            exp.DateTrunc: lambda self, e: self.func("DATE_TRUNC", e.text("unit"), e.this),
            exp.TimestampTrunc: lambda self, e: self.func("DATE_TRUNC", e.text("unit"), e.this),
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
            exp.TimestampAdd: lambda self, e: self.func(
                "TIMESTAMP_ADD", unit_to_str(e), e.expression, e.this
            ),
            exp.TimestampDiff: lambda self, e: self.func(
                "TIMESTAMP_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
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
            exp.LastDay: _last_day_sql,
            exp.AtTimeZone: lambda self, e: self.func(
                "DATETIME", e.this, e.args.get("zone")
            ),
            exp.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", "'UTC'", e.args.get("zone"), e.this
            ),
            exp.Interval: interval_sql,
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.Lag: lambda self, e: self.func("LAG", e.this, e.args.get("offset")),
            exp.Lead: lambda self, e: self.func("LEAD", e.this, e.args.get("offset")),
            exp.FirstValue: rename_func("FIRST_VALUE"),
            exp.LastValue: rename_func("LAST_VALUE"),
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.Stddev: rename_func("STDDEV"),
            exp.StddevPop: rename_func("STDDEV_POP"),
            exp.BitwiseLeftShift: lambda self, e: self.func("SHIFTLEFT", e.this, e.expression),
            exp.BitwiseNot: lambda self, e: self.func("BITWISE_NOT", e.this),
            exp.BitwiseOr: lambda self, e: self.func("BITWISE_OR", e.this, e.expression),
            exp.BitwiseXor: lambda self, e: self.func("BITWISE_XOR", e.this, e.expression),
            exp.BitwiseRightShift: lambda self, e: self.func("SHIFTRIGHT", e.this, e.expression),

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

