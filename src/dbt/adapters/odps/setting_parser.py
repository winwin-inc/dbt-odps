import json
import re
from enum import Enum
from typing import Dict, List, NamedTuple, Optional

from dbt.adapters.events.logging import AdapterLogger

from odps.dbapi import Connection

logger = AdapterLogger("ODPS")


class State(Enum):
    DEFAULT = 0
    SINGLE_LINE_COMMENT = 1
    MULTI_LINE_COMMENT = 2
    IN_SET = 3
    IN_KEY_VALUE = 4
    STOP = 5


class ParseResult(NamedTuple):
    settings: Dict[str, str]
    remaining_query: str
    errors: List[str]


_LEADING_COMMENT_RE = re.compile(r"^\s*/\*\s*(.*?)\s*\*/", re.DOTALL)


class SettingParser:
    @staticmethod
    def parse(
        query: str,
        connection: Optional[Connection] = None,
        schema: Optional[str] = None,
    ) -> ParseResult:
        """Parse SET statements from query and inject schema into the leading JSON comment.

        Args:
            query: The SQL operation string.
            connection: ODPS connection, used to look up the current schema name
                when schema is not given explicitly.
            schema: Schema name to inject into the leading /* {json} */ comment
                (dbt query comment), so that lineage collectors can resolve the
                schema of the executed table. It is normally the schema of the
                dbt node being executed, which differs from the connection
                default schema when a model overrides it via `+schema:`.
                When neither schema nor connection is given, the query is
                returned unchanged.
        """
        if connection is not None or schema is not None:
            if schema is None:
                schema = SettingParser._resolve_schema_name(connection)
            query = SettingParser._inject_schema(query, schema)
        parser = SettingParser()
        return parser.extract_set_statements(query)

    @staticmethod
    def _resolve_schema_name(connection: Connection) -> Optional[str]:
        """获取连接当前默认 schema 名。

        schema 注入只是给血缘采集用的附加信息，取不到时降级为「不注入」，
        不能因为一次元数据请求失败就让整条 SQL 执行失败。
        """
        try:
            schema = connection.odps.get_schema()
        except Exception as e:
            logger.warning(f"Failed to resolve current schema, skip injecting: {e}")
            return None
        if schema is None:
            return None
        return getattr(schema, "name", None)

    @staticmethod
    def _inject_schema(operation: str, schema_name: Optional[str]) -> str:
        """Inject schema information into the leading /* {json} */ comment if present.

        Transforms:
            /* {"app": "dbt", ...} */
            ALTER VIEW ...

        Into:
            /* {"app": "dbt", ..., "schema": ""} */
            ALTER VIEW ...
        """
        m = _LEADING_COMMENT_RE.match(operation)
        if not m:
            return operation

        json_str = m.group(1).strip()
        try:
            comment_data = json.loads(json_str)
        except json.JSONDecodeError:
            return operation
        if schema_name:
            comment_data["schema"] = schema_name
        new_json = json.dumps(comment_data, separators=(",", ":"))
        leading = m.group(0)
        prefix_len = len(leading) - len(leading.lstrip())
        prefix = leading[:prefix_len]
        new_comment = prefix + "/* " + new_json + " */"
        return operation.replace(m.group(0), new_comment, 1)

    def extract_set_statements(self, s: str) -> ParseResult:
        settings: dict[str, str] = {}
        errors = []
        exclude_ranges = []
        current_state = State.DEFAULT
        lower_s = s.lower()
        i = 0
        current_start_index = -1
        s_length = len(s)

        while i < s_length:
            if current_state == State.DEFAULT:
                if i <= s_length - 2 and s[i : i + 2] == "--":
                    current_state = State.SINGLE_LINE_COMMENT
                    i += 2
                elif i <= s_length - 2 and s[i : i + 2] == "/*":
                    current_state = State.MULTI_LINE_COMMENT
                    i += 2
                elif i <= s_length - 3 and lower_s[i : i + 3] == "set":
                    if i + 3 < s_length and s[i + 3].isspace():
                        current_state = State.IN_SET
                        current_start_index = i
                        i += 4  # Skip 'set' and whitespace
                    else:
                        i += 1
                else:
                    if i < s_length and not s[i].isspace():
                        current_state = State.STOP
                    i += 1

            elif current_state == State.SINGLE_LINE_COMMENT:
                while i < s_length and s[i] != "\n":
                    i += 1
                if i < s_length:
                    i += 1
                current_state = State.DEFAULT

            elif current_state == State.MULTI_LINE_COMMENT:
                while i < s_length:
                    if i + 1 < s_length and s[i : i + 2] == "*/":
                        i += 2
                        current_state = State.DEFAULT
                        break
                    else:
                        i += 1

            elif current_state == State.IN_SET:
                while i < s_length and s[i].isspace():
                    i += 1
                if i < s_length:
                    current_state = State.IN_KEY_VALUE
                else:
                    errors.append("Invalid SET statement: missing key-value after 'set'")
                    current_start_index = -1
                    current_state = State.DEFAULT

            elif current_state == State.IN_KEY_VALUE:
                key_value_start = i
                found_semicolon = False
                while i < s_length:
                    if s[i] == ";" and (i == 0 or s[i - 1] != "\\"):
                        found_semicolon = True
                        i += 1
                        break
                    i += 1
                if found_semicolon:
                    kv = s[key_value_start : i - 1].strip()
                    if self._parse_key_value(kv, settings, errors):
                        exclude_ranges.append((current_start_index, i))
                else:
                    errors.append("Invalid SET statement: missing semicolon")
                current_state = State.DEFAULT
                current_start_index = -1

            elif current_state == State.STOP:
                i = s_length
        # Build remaining query
        remaining = []
        current_pos = 0
        for start, end in sorted(exclude_ranges, key=lambda x: x[0]):
            if current_pos < start:
                remaining.append(s[current_pos:start])
            current_pos = end
        if current_pos < s_length:
            remaining.append(s[current_pos:])

        return ParseResult(settings=settings, remaining_query="".join(remaining), errors=errors)

    def _parse_key_value(self, kv: str, settings: Dict[str, str], errors: List[str]) -> bool:
        eq_idx = kv.find("=")
        if eq_idx == -1:
            errors.append(f"Invalid key-value pair '{kv}': missing '='")
            return False
        key = kv[:eq_idx].strip()
        if not key:
            errors.append(f"Invalid key-value pair '{kv}': empty key")
            return False
        value = kv[eq_idx + 1 :].strip() if eq_idx < len(kv) - 1 else ""
        value = value.replace("\\;", ";")
        settings[key] = value
        return True