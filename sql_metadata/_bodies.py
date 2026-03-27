"""Extract CTE and subquery body SQL from the sqlglot AST.

Uses ``exp.sql()`` via a custom :class:`_PreservingGenerator` that uppercases
keywords and function names but preserves function signatures (e.g. keeps
``IFNULL`` instead of rewriting to ``COALESCE``, keeps ``DIV`` instead of
``CAST``).

Two public entry points:

* :func:`extract_cte_bodies` — called by :attr:`Parser.with_queries`.
* :func:`extract_subquery_bodies` — called by :attr:`Parser.subqueries`.
"""

import copy
from typing import Dict, List, Optional

from sqlglot import exp
from sqlglot.generator import Generator


class _PreservingGenerator(Generator):
    """Custom SQL generator that preserves function signatures.

    sqlglot normalises certain functions when rendering SQL (e.g.
    ``IFNULL`` → ``COALESCE``, ``DIV`` → ``CAST(… / … AS INT)``).
    This generator overrides those transformations so that the output
    only differs from the input in keyword/function-name casing and
    explicit ``AS`` insertion.
    """

    TRANSFORMS = {
        **Generator.TRANSFORMS,
        exp.CurrentDate: lambda self, e: "CURRENT_DATE()",
        exp.IntDiv: lambda self, e: (
            f"{self.sql(e, 'this')} DIV {self.sql(e, 'expression')}"
        ),
    }

    def coalesce_sql(self, expression):
        args = [expression.this] + expression.expressions
        if len(args) == 2:
            return f"IFNULL({self.sql(args[0])}, {self.sql(args[1])})"
        return super().coalesce_sql(expression)

    def dateadd_sql(self, expression):
        return (
            f"DATE_ADD({self.sql(expression, 'this')}, "
            f"{self.sql(expression, 'expression')})"
        )

    def datesub_sql(self, expression):
        return (
            f"DATE_SUB({self.sql(expression, 'this')}, "
            f"{self.sql(expression, 'expression')})"
        )

    def tsordsadd_sql(self, expression):
        this = self.sql(expression, "this")
        expr_node = expression.expression
        # Detect negated expression pattern from date_sub → TsOrDsAdd(x, y * -1)
        if isinstance(expr_node, exp.Mul):
            right = expr_node.expression
            if (
                isinstance(right, exp.Neg)
                and isinstance(right.this, exp.Literal)
                and right.this.this == "1"
            ):
                left = self.sql(expr_node, "this")
                return f"DATE_SUB({this}, {left})"
        return f"DATE_ADD({this}, {self.sql(expression, 'expression')})"

    def not_sql(self, expression):
        child = expression.this
        # Rewrite NOT x IS NULL → x IS NOT NULL
        if isinstance(child, exp.Is) and isinstance(child.expression, exp.Null):
            return f"{self.sql(child, 'this')} IS NOT NULL"
        # Rewrite NOT x IN (...) → x NOT IN (...)
        if isinstance(child, exp.In):
            return f"{self.sql(child, 'this')} NOT IN ({self.expressions(child)})"
        return super().not_sql(expression)


_GENERATOR = _PreservingGenerator()


def _body_sql(node: exp.Expression) -> str:
    """Render an AST node to SQL, stripping identifier quoting."""
    body = copy.deepcopy(node)
    for ident in body.find_all(exp.Identifier):
        ident.set("quoted", False)
    return _GENERATOR.generate(body)


def extract_cte_bodies(
    ast: Optional[exp.Expression],
    raw_sql: str,
    cte_names: List[str],
    cte_name_map: Optional[dict] = None,
) -> Dict[str, str]:
    """Extract CTE body SQL for each name in *cte_names*.

    Walks the AST for ``exp.CTE`` nodes, matches each alias against
    *cte_names*, and renders the body via :func:`_body_sql`.

    :param ast: Root AST node.
    :param raw_sql: Original SQL string (kept for API compatibility).
    :param cte_names: Ordered list of CTE names to extract bodies for.
    :param cte_name_map: Placeholder → original qualified name mapping.
    :returns: Mapping of ``{cte_name: body_sql}``.
    """
    if not ast or not cte_names:
        return {}

    # Build mapping from AST alias (which may be a __DOT__ placeholder)
    # back to the original qualified CTE name in cte_names.
    alias_to_name: Dict[str, str] = {}
    for name in cte_names:
        # The AST alias may be the placeholder form (e.g. "db__DOT__cte")
        placeholder = name.replace(".", "__DOT__")
        alias_to_name[placeholder.upper()] = name
        alias_to_name[name.upper()] = name
        # Also match just the short name (last segment)
        alias_to_name[name.split(".")[-1].upper()] = name

    results: Dict[str, str] = {}
    for cte in ast.find_all(exp.CTE):
        alias = cte.alias
        if alias.upper() in alias_to_name:
            original_name = alias_to_name[alias.upper()]
            results[original_name] = _body_sql(cte.this)

    return results


def _collect_subqueries_postorder(
    node: exp.Expression, names_upper: Dict[str, str], out: Dict[str, str]
) -> None:
    """Recursively collect subquery bodies in post-order."""
    for child in node.iter_expressions():
        _collect_subqueries_postorder(child, names_upper, out)
    if isinstance(node, exp.Subquery) and node.alias:
        alias_upper = node.alias.upper()
        if alias_upper in names_upper:
            original_name = names_upper[alias_upper]
            out[original_name] = _body_sql(node.this)


def extract_subquery_bodies(
    ast: Optional[exp.Expression],
    raw_sql: str,
    subquery_names: List[str],
) -> Dict[str, str]:
    """Extract subquery body SQL for each name in *subquery_names*.

    Uses a post-order AST walk so that inner subqueries appear before
    outer ones, matching the order from :func:`extract_subquery_names`.

    :param ast: Root AST node.
    :param raw_sql: Original SQL string (kept for API compatibility).
    :param subquery_names: List of subquery alias names to extract.
    :returns: Mapping of ``{subquery_name: body_sql}``.
    """
    if not ast or not subquery_names:
        return {}

    names_upper = {n.upper(): n for n in subquery_names}
    results: Dict[str, str] = {}
    _collect_subqueries_postorder(ast, names_upper, results)
    return results
