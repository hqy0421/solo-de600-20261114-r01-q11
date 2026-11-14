"""Batch recovery of corrupted financial CSV exports.

Financial CSV exports arrive through the stream in byte chunks, one batch
per manifest.  A few missing or extra quote characters make newlines and
commas inside description fields look like record or field boundaries, so a
naive parse produces the wrong records.

This module recovers a batch by *only inserting or deleting quote
characters*.  Given per-column constraints, the expected record count and
the exact expected :class:`~decimal.Decimal` amount total from an external
manifest, it searches for the lowest-cost set of quote edits (within a
budget) that makes the whole batch parse as valid records satisfying every
constraint.

Guarantees
----------
* Only quotes are ever inserted or deleted; no other byte is touched and no
  line is dropped -- a solution always tiles the complete batch bytes.
* The search is deterministic: the same batch bytes and the same manifest
  always produce the same result, no matter how the bytes were chunked when
  they arrived.  Recovery only ever runs on the fully reassembled batch.
* If several lowest-cost solutions produce *different* records, all of them
  are returned and the result is flagged ``'ambiguous'`` -- one is never
  picked arbitrarily.
* If no solution exists within the edit budget, the result is flagged
  ``'infeasible'`` and carries diagnostics instead of silently dropping
  rows.

Streaming usage
---------------
>>> from streamz import Stream
>>> from streamz.csvrecovery import recover_csv, assemble_csv_batches, BATCH_END
>>> source = Stream()
>>> batches = source.assemble_csv_batches(BATCH_END)   # bytes -> batch bytes
>>> results = batches.recover_csv(manifest)            # batch -> RecoveryResult
>>> results.sink(print)                                # doctest: +SKIP

``recover_csv`` also accepts elements that are already a full batch
(``bytes`` or an iterable of byte chunks) or a ``(payload, manifest)`` tuple
when the manifest differs per batch.
"""
from __future__ import annotations

import heapq
import logging
import re
from bisect import bisect_right
from decimal import Decimal, InvalidOperation, localcontext
from itertools import count

from .core import Stream

logger = logging.getLogger(__name__)

__all__ = [
    'ColumnSpec', 'Manifest', 'EditCosts', 'Fix', 'Solution', 'RecoveryResult',
    'recover_batch', 'recover_csv', 'assemble_csv_batches', 'BATCH_END',
]

_MASK = (1 << 61) - 1
_OUT, _IN = 0, 1
_INSERT = 'insert_quote'
_DELETE = 'delete_quote'

_DECIMAL_RE = re.compile(r'[+-]?(\d+(\.\d*)?|\.\d+)')
_INTEGER_RE = re.compile(r'[+-]?\d+')


def _mix(h, b):
    return (h * 131 + b + 1) & _MASK


def _mix_fields(hr, hf):
    return (hr * 131 + hf + 1) & _MASK


def _hash_fields(fields):
    """Deterministic content hash of a tuple of byte fields."""
    h = 0
    for f in fields:
        hf = 0
        for b in f:
            hf = _mix(hf, b)
        h = _mix_fields(h, hf)
    return h


# ---------------------------------------------------------------------------
# Incremental field validators
#
# A column automaton is a ``(transitions, accepting)`` pair where
# ``transitions[state]`` maps a content byte to the next state.  It accepts
# exactly the same field contents as ``ColumnSpec.validate`` (modulo
# ``max_length``, which is only re-checked on decoded fields), so partial
# field contents that can never become valid are pruned during the repair
# search instead of after it.  Columns whose kind has no automaton are
# validated only on decoded candidates, as before.
# ---------------------------------------------------------------------------

_DIGITS = tuple(b'0123456789')


def _integer_auto(allow_empty):
    # [+-]?\d+
    trans = ({43: 1, 45: 1, **{d: 2 for d in _DIGITS}},
             {d: 2 for d in _DIGITS},
             {d: 2 for d in _DIGITS})
    accept = {2}
    if allow_empty:
        accept.add(0)
    return trans, frozenset(accept)


def _decimal_auto(allow_empty):
    # [+-]?(\d+(\.\d*)?|\.\d+)
    trans = ({43: 1, 45: 1, 46: 3, **{d: 2 for d in _DIGITS}},
             {46: 3, **{d: 2 for d in _DIGITS}},
             {46: 4, **{d: 2 for d in _DIGITS}},
             {d: 5 for d in _DIGITS},
             {d: 5 for d in _DIGITS},
             {d: 5 for d in _DIGITS})
    accept = {2, 4, 5}
    if allow_empty:
        accept.add(0)
    return trans, frozenset(accept)


def _thousands_auto(allow_empty, sep_char):
    # [+-]?(\d{1,3}(sep\d{3})+|\d+)(\.\d*)?  -- returns None when the
    # separator collides with the decimal syntax (the regex itself is
    # ambiguous then), in which case no incremental validation happens.
    if sep_char in '0123456789+-.':
        return None
    sep = ord(sep_char)
    if sep > 127:
        return None
    trans = ({43: 1, 45: 1, **{d: 2 for d in _DIGITS}},        # 0 start
             {d: 2 for d in _DIGITS},                          # 1 sign
             {46: 9, sep: 5, **{d: 3 for d in _DIGITS}},       # 2: 1 digit
             {46: 9, sep: 5, **{d: 4 for d in _DIGITS}},       # 3: 2 digits
             {46: 9, sep: 5, **{d: 6 for d in _DIGITS}},       # 4: 3 digits
             {d: 7 for d in _DIGITS},                          # 5: sep seen
             {46: 9, **{d: 6 for d in _DIGITS}},               # 6: 4+ digits
             {d: 8 for d in _DIGITS},                          # 7: 1 grp digit
             {d: 10 for d in _DIGITS},                         # 8: 2 grp digits
             {d: 11 for d in _DIGITS},                         # 9: dot
             {46: 9, sep: 5},                                  # 10: group done
             {d: 11 for d in _DIGITS})                         # 11: frac digits
    accept = {2, 3, 4, 6, 9, 10, 11}
    if allow_empty:
        accept.add(0)
    return trans, frozenset(accept)


def _enum_auto(values, allow_empty, encoding):
    trans = [{}]
    accept = set()
    if allow_empty:
        accept.add(0)
    for v in values:
        if v == '':
            continue  # empty is governed by allow_empty, like validate()
        try:
            b = v.encode(encoding)
        except (UnicodeEncodeError, LookupError):
            return None
        node = 0
        for byte in b:
            nxt = trans[node].get(byte)
            if nxt is None:
                nxt = len(trans)
                trans[node][byte] = nxt
                trans.append({})
            node = nxt
        accept.add(node)
    return tuple(trans), frozenset(accept)


# ---------------------------------------------------------------------------
# Configuration objects
# ---------------------------------------------------------------------------

class ColumnSpec:
    """Constraint for one CSV column.

    Parameters
    ----------
    kind : str
        One of ``'string'`` (anything), ``'integer'``, ``'decimal'``,
        ``'regex'``, ``'enum'``, ``'date'``, ``'datetime'``.
    name : str, optional
        Column name; used to validate the header row when every column has a
        name, and to resolve ``Manifest(amount_column=...)``.
    pattern : str, optional
        Regular expression (full match) for ``kind='regex'``.
    values : iterable, optional
        Allowed values for ``kind='enum'``.
    fmt : str, optional
        ``datetime.strptime`` format for ``kind='date'`` / ``'datetime'``.
    allow_empty : bool
        Whether an empty field is accepted (default True).
    max_length : int, optional
        Maximum field length in characters.
    """

    __slots__ = ('kind', 'name', 'allow_empty', 'max_length', '_regex',
                 '_values', '_fmt', '_auto')

    def __init__(self, kind='string', name=None, pattern=None, values=None,
                 fmt=None, allow_empty=True, max_length=None):
        if kind not in ('string', 'integer', 'decimal', 'regex', 'enum',
                        'date', 'datetime'):
            raise ValueError('unknown column kind %r' % (kind,))
        self.kind = kind
        self.name = name
        self.allow_empty = allow_empty
        self.max_length = max_length
        self._regex = None
        self._values = None
        self._fmt = None
        self._auto = None
        if kind == 'integer':
            self._regex = _INTEGER_RE
            self._auto = _integer_auto(allow_empty)
        elif kind == 'decimal':
            self._regex = _DECIMAL_RE
            self._auto = _decimal_auto(allow_empty)
        elif kind == 'regex':
            if pattern is None:
                raise ValueError("kind='regex' requires pattern=")
            self._regex = re.compile(pattern)
        elif kind == 'enum':
            if values is None:
                raise ValueError("kind='enum' requires values=")
            self._values = frozenset(values)
            self._auto = _enum_auto(self._values, allow_empty, 'utf-8')
        elif kind in ('date', 'datetime'):
            if fmt is None:
                raise ValueError("kind=%r requires fmt=" % kind)
            self._fmt = fmt

    def validate(self, s):
        if s == '':
            return self.allow_empty
        if self.max_length is not None and len(s) > self.max_length:
            return False
        if self._regex is not None:
            return self._regex.fullmatch(s) is not None
        if self._values is not None:
            return s in self._values
        if self._fmt is not None:
            from datetime import datetime
            try:
                datetime.strptime(s, self._fmt)
            except ValueError:
                return False
            return True
        return True

    def __repr__(self):
        return 'ColumnSpec(kind=%r, name=%r)' % (self.kind, self.name)


class _Fmt:
    """Compiled byte-level format details."""

    __slots__ = ('quote', 'delim', 'quote_re', 'special_re')

    def __init__(self, quote, delim):
        self.quote = quote
        self.delim = delim
        self.quote_re = re.compile(re.escape(bytes([quote])))
        specials = bytes([quote, delim, 0x0D, 0x0A])
        self.special_re = re.compile(b'[' + re.escape(specials) + b']')


class Manifest:
    """External manifest describing one batch.

    Parameters
    ----------
    columns : sequence of ColumnSpec or dict
        Per-column constraints, in order.  Dicts are passed to
        :class:`ColumnSpec` as keyword arguments.
    record_count : int
        Expected number of data records (excluding the header).
    amount_column : int or str, optional
        Index or name of the amount column.  Required when ``amount_total``
        is given.
    amount_total : Decimal or str, optional
        Exact expected sum of the amount column, compared with
        :class:`decimal.Decimal` arithmetic.
    has_header : bool
        Whether the first line is a header row (default True).  The header
        is validated against the column names when all columns have names,
        otherwise against the column count.  It is not counted in
        ``record_count`` nor summed.
    delimiter, quotechar : str
        Single-character CSV delimiter and quote (default ``,`` and ``"``).
    encoding : str
        Text encoding of the batch (default ``'utf-8'``).
    thousands_separator : str, optional
        If given, this character is stripped from amount fields before
        parsing (e.g. ``','`` for ``"1,234.56"``).
    strip_bom : bool
        Strip a leading UTF-8 BOM before processing (default True).  Fix
        positions still refer to the original byte string.
    """

    def __init__(self, columns, record_count, amount_column=None,
                 amount_total=None, has_header=True, delimiter=',',
                 quotechar='"', encoding='utf-8', thousands_separator=None,
                 strip_bom=True):
        cols = []
        for c in columns:
            cols.append(c if isinstance(c, ColumnSpec) else ColumnSpec(**c))
        self.columns = tuple(cols)
        self.ncols = len(self.columns)
        if self.ncols == 0:
            raise ValueError('manifest needs at least one column')
        self.record_count = int(record_count)
        if self.record_count < 0:
            raise ValueError('record_count must be >= 0')
        if isinstance(amount_column, str):
            names = [c.name for c in self.columns]
            if amount_column not in names:
                raise ValueError('unknown amount column %r' % amount_column)
            amount_column = names.index(amount_column)
        if amount_total is not None and amount_column is None:
            raise ValueError('amount_total requires amount_column')
        self.amount_column = amount_column
        if amount_total is None:
            self.amount_total = None
        elif isinstance(amount_total, Decimal):
            self.amount_total = amount_total
        elif isinstance(amount_total, float):
            self.amount_total = Decimal(str(amount_total))
        else:
            self.amount_total = Decimal(amount_total)
        self.has_header = bool(has_header)
        if len(delimiter) != 1 or len(quotechar) != 1:
            raise ValueError('delimiter and quotechar must be single characters')
        if delimiter == quotechar or delimiter in '\r\n' or quotechar in '\r\n':
            raise ValueError('delimiter/quotechar must differ and not be newlines')
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        if thousands_separator is not None and len(thousands_separator) != 1:
            raise ValueError('thousands_separator must be a single character')
        self.thousands_separator = thousands_separator
        if (thousands_separator is not None and amount_column is not None
                and self.columns[amount_column].kind == 'decimal'):
            # keep the column constraint consistent with parse_amount
            sep = re.escape(thousands_separator)
            self.columns[amount_column]._regex = re.compile(
                r'[+-]?(\d{1,3}(%s\d{3})+|\d+)(\.\d*)?' % sep)
            self.columns[amount_column]._auto = _thousands_auto(
                self.columns[amount_column].allow_empty, thousands_separator)
        if encoding != 'utf-8':
            for c in self.columns:
                if c.kind == 'enum':
                    c._auto = _enum_auto(c._values, c.allow_empty, encoding)
        self.strip_bom = bool(strip_bom)
        self._fmt = _Fmt(ord(quotechar), ord(delimiter))
        self._autos = tuple(c._auto for c in self.columns)

    def parse_amount(self, s):
        """Parse an amount field to Decimal, or return None if invalid."""
        if self.thousands_separator is not None:
            s = s.replace(self.thousands_separator, '')
        if not _DECIMAL_RE.fullmatch(s):
            return None
        try:
            d = Decimal(s)
        except InvalidOperation:
            return None
        if not d.is_finite():
            return None
        return d

    def header_names(self):
        names = [c.name for c in self.columns]
        if any(n is None for n in names):
            return None
        return tuple(names)


class EditCosts:
    """Cost of inserting or deleting one quote character."""

    __slots__ = ('insert', 'delete')

    def __init__(self, insert=1, delete=1):
        insert = int(insert)
        delete = int(delete)
        if insert < 0 or delete < 0 or insert + delete == 0:
            raise ValueError('costs must be >= 0 and not both zero')
        self.insert = insert
        self.delete = delete

    def __repr__(self):
        return 'EditCosts(insert=%r, delete=%r)' % (self.insert, self.delete)


class Fix:
    """One quote edit at a byte position in the original batch.

    ``action`` is ``'insert_quote'`` (a quote was inserted before the byte at
    ``position``; ``position == len(data)`` means appended at the end) or
    ``'delete_quote'`` (the quote byte at ``position`` was removed).
    """

    __slots__ = ('position', 'action')

    def __init__(self, position, action):
        self.position = position
        self.action = action

    def __repr__(self):
        return 'Fix(position=%r, action=%r)' % (self.position, self.action)

    def __eq__(self, other):
        return (isinstance(other, Fix) and self.position == other.position
                and self.action == other.action)

    def __hash__(self):
        return hash((self.position, self.action))

    def __lt__(self, other):
        return (self.position, self.action) < (other.position, other.action)


class Solution:
    """One lowest-cost recovery: the repaired records and the quote edits."""

    __slots__ = ('records', 'fixes', 'cost')

    def __init__(self, records, fixes, cost):
        self.records = records
        self.fixes = fixes
        self.cost = cost

    def __repr__(self):
        return 'Solution(cost=%r, fixes=%r, records=%d rows)' % (
            self.cost, list(self.fixes), len(self.records))


class RecoveryResult:
    """Outcome of :func:`recover_batch` for one batch.

    Attributes
    ----------
    status : str
        ``'clean'``     -- constraints already satisfied, no edits needed;
        ``'recovered'`` -- a unique lowest-cost repair was found;
        ``'ambiguous'`` -- several lowest-cost repairs give different
                           records; all are in ``solutions``;
        ``'infeasible'``-- no repair within budget satisfies the manifest.
    records : tuple of tuples of str or None
        The recovered data records (header excluded) when the solution is
        unique, else None.
    fixes : tuple of Fix or None
        Quote edits of the unique solution, sorted by position.
    solutions : tuple of Solution
        All lowest-cost solutions (empty when infeasible).
    cost : int or None
        Total edit cost of the solution(s).
    header : tuple of str or None
        The recovered header fields, when the manifest has a header.
    diagnostics : dict
        Extra information (row counts, achievable totals, truncation flags).
    """

    __slots__ = ('status', 'records', 'fixes', 'solutions', 'cost', 'header',
                 'diagnostics')

    def __init__(self, status, records, fixes, solutions, cost, header,
                 diagnostics):
        self.status = status
        self.records = records
        self.fixes = fixes
        self.solutions = solutions
        self.cost = cost
        self.header = header
        self.diagnostics = diagnostics

    @property
    def ok(self):
        return self.status in ('clean', 'recovered')

    def to_csv(self, include_header=True, delimiter=','):
        """Serialize the unique solution back to standard CSV text."""
        import csv
        import io
        if self.records is None:
            raise ValueError('no unique records to serialize (status=%r)'
                             % self.status)
        sio = io.StringIO()
        w = csv.writer(sio, delimiter=delimiter, lineterminator='\n')
        if include_header and self.header is not None:
            w.writerow(self.header)
        w.writerows(self.records)
        return sio.getvalue()

    def __repr__(self):
        return ('RecoveryResult(status=%r, cost=%r, records=%s, solutions=%d)'
                % (self.status, self.cost,
                   None if self.records is None else len(self.records),
                   len(self.solutions)))


# ---------------------------------------------------------------------------
# Byte-level deterministic parsing
# ---------------------------------------------------------------------------

def _line_starts(buf):
    """Start offsets of every line.  A trailing newline does not start one."""
    if not buf:
        return []
    starts = [0]
    for m in re.finditer(rb'\n', buf):
        starts.append(m.end())
    if starts[-1] == len(buf):
        starts.pop()
    return starts


def _walk_one(buf, pos, fmt, limit):
    """Parse exactly one record starting at ``pos`` with no edits.

    Returns ``(end_pos, fields)`` where ``fields`` is a list of bytes, or
    None if the record does not terminate before ``limit`` (unclosed quote
    or overlong record).

    Parse semantics (deterministic): a quote toggles the in-quotes state;
    inside quotes ``""`` is an escaped literal quote; commas and newlines
    inside quotes are content; a ``\\r`` immediately before ``\\n`` outside
    quotes is ignored; a newline outside quotes ends the record; at EOF a
    pending field ends the record unless a quote is still open.
    """
    n = len(buf)
    quote = fmt.quote
    delim = fmt.delim
    quote_re = fmt.quote_re
    special_re = fmt.special_re
    fields = []
    parts = []
    in_q = False
    p = pos
    while p < n:
        if p >= limit:
            return None
        if in_q:
            m = quote_re.search(buf, p)
            if m is None:
                return None
            c = m.start()
            parts.append(buf[p:c])
            if c + 1 < n and buf[c + 1] == quote:
                parts.append(buf[c:c + 1])
                p = c + 2
            else:
                in_q = False
                p = c + 1
        else:
            m = special_re.search(buf, p)
            if m is None:
                if n > limit:
                    return None
                parts.append(buf[p:n])
                fields.append(b''.join(parts))
                return n, fields
            c = m.start()
            if c >= limit:
                return None
            parts.append(buf[p:c])
            b = buf[c]
            if b == quote:
                in_q = True
                p = c + 1
            elif b == delim:
                fields.append(b''.join(parts))
                parts = []
                p = c + 1
            elif b == 0x0A:
                fields.append(b''.join(parts))
                return c + 1, fields
            elif b == 0x0D and c + 1 < n and buf[c + 1] == 0x0A:
                p = c + 1  # skip CR of CRLF
            else:
                parts.append(buf[c:c + 1])
                p = c + 1
    if in_q:
        return None
    fields.append(b''.join(parts))
    return n, fields


def _parse_with_edits(buf, start, end, edits, fmt):
    """Parse exactly one record from ``buf[start:end]`` with quote edits.

    ``edits`` is a sorted tuple of ``(pos, action, mode)`` where action is
    ``_INSERT``/``_DELETE`` and mode disambiguates an insertion inside
    quotes at an original quote byte: ``'toggle'`` (the inserted quote
    closes the field, the original one reopens it) or ``'pair'`` (inserted
    and original quotes form an escaped-quote pair).  Returns the list of
    byte fields; raises ValueError if the edited span does not parse as
    exactly one record.
    """
    quote = fmt.quote
    delim = fmt.delim
    qbyte = bytes([quote])
    fields = []
    parts = []
    in_q = False
    pending = True  # a field is open
    ei = 0
    ne = len(edits)
    p = start
    while True:
        # apply edits at gap/position p: inserts first, then deletes
        while ei < ne and edits[ei][0] == p and edits[ei][1] == _INSERT:
            mode = edits[ei][2]
            ei += 1
            if in_q:
                if mode == 'pair' or (
                        mode != 'toggle' and p < end and buf[p] == quote):
                    # inserted quote pairs with the original one: escaped quote
                    parts.append(qbyte)
                    p += 1
                else:
                    in_q = False
            else:
                in_q = True
        if p >= end:
            break
        if ei < ne and edits[ei][0] == p and edits[ei][1] == _DELETE:
            ei += 1
            p += 1
            continue
        b = buf[p]
        if in_q:
            if b == quote:
                if p + 1 < end and buf[p + 1] == quote:
                    parts.append(qbyte)
                    p += 2
                else:
                    in_q = False
                    p += 1
            else:
                parts.append(buf[p:p + 1])
                p += 1
        else:
            if b == quote:
                in_q = True
                p += 1
            elif b == delim:
                fields.append(b''.join(parts))
                parts = []
                p += 1
            elif b == 0x0A:
                fields.append(b''.join(parts))
                parts = []
                pending = False
                if p + 1 != end:
                    raise ValueError('record content past its terminator')
                p += 1
            elif b == 0x0D and p + 1 < end and buf[p + 1] == 0x0A:
                p += 1
            else:
                parts.append(buf[p:p + 1])
                p += 1
    if in_q:
        raise ValueError('unclosed quote')
    if pending:
        fields.append(b''.join(parts))
    return fields


# ---------------------------------------------------------------------------
# Inner repair search: cheapest quote edits to parse one record from a line
# ---------------------------------------------------------------------------

def _repair_from(buf, starts, li, nlines, fmt, ncols, ins_cost, del_cost,
                 cap, max_lines, max_bytes, max_pops=100000, autos=None):
    """All cheapest repaired records starting at line ``li``.

    Dijkstra search over states ``(pos, in_quotes, col, field_hash,
    record_hash, inserted_here, vstate)``.  Transitions consume one byte
    as-is (cost 0), delete a quote byte (cost ``del_cost``) or insert a
    virtual quote before the current byte (cost ``ins_cost``); states with
    cost above ``cap`` are pruned.  Insertions are only considered at
    structural boundaries (next to a quote/delimiter/newline or at the
    ends): an insertion in the middle of a content run can be moved to the
    run boundary without changing the parse or the cost.  ``vstate`` is the
    current column's incremental-validator state (``autos``); partial field
    contents that can never satisfy the column constraint are pruned
    immediately, which collapses the search on lines that cannot start a
    valid record no matter how quotes are edited.

    Returns ``(candidates, truncated)`` where candidates is a list of
    ``(end_pos, cost, record_hash, edits)`` with ``edits`` a sorted tuple of
    ``(pos, action, mode)``, deduplicated by ``(end_pos, record_hash)``
    keeping the cheapest.  ``truncated`` is True when the search hit
    ``max_pops`` and cheaper-than-``cap`` candidates may be missing.
    """
    n = len(buf)
    start = starts[li]
    pos_limit = min(n, start + max_bytes,
                    starts[li + max_lines] if li + max_lines < nlines else n)
    quote = fmt.quote
    delim = fmt.delim
    c_last = ncols - 1

    if autos is None:
        autos = (None,) * ncols
    a_trans = [a[0] if a is not None else None for a in autos]
    a_acc = [a[1] if a is not None else frozenset() for a in autos]
    inits = tuple(0 if a is not None else -1 for a in autos)

    insert_ok = bytearray(pos_limit + 1 - start)
    specials = (quote, delim, 0x0A, 0x0D)
    for q in range(start, pos_limit):
        if q == 0 or buf[q] in specials or buf[q - 1] in specials:
            insert_ok[q - start] = 1
    insert_ok[pos_limit - start] = 1  # EOF / span-end gap

    start_state = (start, _OUT, 0, 0, 0, 0, inits[0])
    best = {start_state: 0}
    prev = {}
    cands = {}
    heap = [(0, 0, start_state)]
    tie = count(1)
    truncated = False
    pops = 0

    def register(end_pos, hr, cost, state):
        key = (end_pos, hr)
        old = cands.get(key)
        if old is None or cost < old[0]:
            cands[key] = (cost, state)

    while heap:
        c0, _, s = heapq.heappop(heap)
        if c0 != best.get(s):
            continue
        pops += 1
        if pops > max_pops:
            truncated = True
            break
        p, in_q, col, hf, hr, ins, vs = s

        def push(s2, c2, edit):
            if s2[0] > pos_limit or c2 > cap:
                return
            old = best.get(s2)
            if old is None or c2 < old:
                best[s2] = c2
                prev[s2] = (s, edit)
                heapq.heappush(heap, (c2, next(tie), s2))

        if p == n:
            if in_q:
                if not ins:
                    push((p, _OUT, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, ''))
            elif col == c_last and (vs < 0 or vs in a_acc[col]):
                register(p, _mix_fields(hr, hf), c0, s)
            continue

        can_insert = not ins and insert_ok[p - start]
        b = buf[p]
        if in_q:
            if b == quote:
                if p + 1 < n and buf[p + 1] == quote:
                    # escaped quote: one literal quote byte of content
                    vs2 = -1
                    if vs >= 0:
                        vs2 = a_trans[col][vs].get(quote, -2)
                    if vs2 != -2:
                        push((p + 2, _IN, col, _mix(hf, quote), hr, 0, vs2),
                             c0, None)
                else:
                    push((p + 1, _OUT, col, hf, hr, 0, vs), c0, None)
                push((p + 1, _IN, col, hf, hr, 0, vs), c0 + del_cost,
                     (p, _DELETE, ''))
                if can_insert:
                    # virtual quote closes the field...
                    push((p, _OUT, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, 'toggle'))
                    # ...or pairs with this original quote as an escape
                    vs2 = -1
                    if vs >= 0:
                        vs2 = a_trans[col][vs].get(quote, -2)
                    if vs2 != -2:
                        push((p + 1, _IN, col, _mix(hf, quote), hr, 0, vs2),
                             c0 + ins_cost, (p, _INSERT, 'pair'))
            else:
                vs2 = -1
                if vs >= 0:
                    vs2 = a_trans[col][vs].get(b, -2)
                if vs2 != -2:
                    push((p + 1, _IN, col, _mix(hf, b), hr, 0, vs2),
                         c0, None)
                if can_insert:
                    push((p, _OUT, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, ''))
        else:
            if b == quote:
                push((p + 1, _IN, col, hf, hr, 0, vs), c0, None)
                push((p + 1, _OUT, col, hf, hr, 0, vs), c0 + del_cost,
                     (p, _DELETE, ''))
                if can_insert:
                    push((p, _IN, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, ''))
            elif b == delim:
                if (col < c_last and (vs < 0 or vs in a_acc[col])):
                    push((p + 1, _OUT, col + 1, 0, _mix_fields(hr, hf), 0,
                          inits[col + 1]), c0, None)
                if can_insert:
                    push((p, _IN, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, ''))
            elif b == 0x0A:
                if col == c_last and (vs < 0 or vs in a_acc[col]):
                    register(p + 1, _mix_fields(hr, hf), c0, s)
                if can_insert:
                    push((p, _IN, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, ''))
            elif b == 0x0D and p + 1 < n and buf[p + 1] == 0x0A:
                push((p + 1, _OUT, col, hf, hr, 0, vs), c0, None)
                if can_insert:
                    push((p, _IN, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, ''))
            else:
                vs2 = -1
                if vs >= 0:
                    vs2 = a_trans[col][vs].get(b, -2)
                if vs2 != -2:
                    push((p + 1, _OUT, col, _mix(hf, b), hr, 0, vs2),
                         c0, None)
                if can_insert:
                    push((p, _IN, col, hf, hr, 1, vs), c0 + ins_cost,
                         (p, _INSERT, ''))

    out = []
    for (end_pos, hr), (cost, state) in cands.items():
        edits = []
        s = state
        while s != start_state:
            ps, edit = prev[s]
            if edit is not None:
                edits.append(edit)
            s = ps
        edits.sort()
        out.append((end_pos, cost, hr, tuple(edits)))
    return out, truncated


# ---------------------------------------------------------------------------
# Candidates
# ---------------------------------------------------------------------------

class _Cand:
    """One way to read one record: span, cost, edits, decoded fields, amount."""

    __slots__ = ('end_line', 'cost', 'edits', 'fields', 'amount', 'hr')

    def __init__(self, end_line, cost, edits, fields, amount, hr):
        self.end_line = end_line
        self.cost = cost
        self.edits = edits
        self.fields = fields
        self.amount = amount
        self.hr = hr


class _Provider:
    """Lazily computes and caches the candidate records starting at each line."""

    def __init__(self, buf, starts, manifest, costs, cap, max_lines,
                 max_bytes, exhaustive, max_candidates, diagnostics,
                 walk_cache=None, max_pops=100000):
        self.buf = buf
        self.starts = starts
        self.nlines = len(starts)
        self.manifest = manifest
        self.costs = costs
        self.cap = cap  # per-record edit-cost limit for repair searches
        self.max_lines = max_lines
        self.max_bytes = max_bytes
        self.exhaustive = exhaustive
        self.max_candidates = max_candidates
        self.diagnostics = diagnostics
        self.max_pops = max_pops
        self.cache = {}
        # clean-parse results are cost-independent and shared across caps
        self.walk_cache = walk_cache if walk_cache is not None else {}

    def _decode_validate(self, fields_b):
        m = self.manifest
        if len(fields_b) != m.ncols:
            return None
        out = []
        for f, col in zip(fields_b, m.columns):
            try:
                s = f.decode(m.encoding)
            except UnicodeDecodeError:
                return None
            if not col.validate(s):
                return None
            out.append(s)
        amount = None
        if m.amount_column is not None:
            amount = m.parse_amount(out[m.amount_column])
            if amount is None:
                return None
        return tuple(out), amount

    def _make_cand(self, end_pos, cost, edits, fields_b):
        if end_pos == len(self.buf):
            j = self.nlines
        else:
            # end_pos is the start of line j (records end at line starts)
            j = bisect_right(self.starts, end_pos) - 1
        dv = self._decode_validate(fields_b)
        if dv is None:
            return None
        fields, amount = dv
        return _Cand(j, cost, edits, fields, amount, _hash_fields(fields_b))

    def get(self, li):
        c = self.cache.get(li)
        if c is None:
            c = self._compute(li)
            self.cache[li] = c
        return c

    def _compute(self, li):
        m = self.manifest
        start = self.starts[li]
        if not self.exhaustive:
            if li in self.walk_cache:
                walked = self.walk_cache[li]
            else:
                limit = min(len(self.buf), start + self.max_bytes,
                            self.starts[li + self.max_lines]
                            if li + self.max_lines < self.nlines
                            else len(self.buf))
                w = _walk_one(self.buf, start, m._fmt, limit)
                if w is None:
                    walked = None
                else:
                    end, fields_b = w
                    walked = self._make_cand(end, 0, (), fields_b)
                self.walk_cache[li] = walked
            if walked is not None:
                return [walked]
        return self._repair(li)

    def _repair(self, li):
        m = self.manifest
        raws, truncated = _repair_from(
            self.buf, self.starts, li, self.nlines, m._fmt,
            m.ncols, self.costs.insert, self.costs.delete,
            self.cap, self.max_lines, self.max_bytes, self.max_pops,
            autos=m._autos)
        if truncated:
            self.diagnostics['search_truncated'] = True
        cands = {}
        for end_pos, cost, _hr, edits in raws:
            try:
                fields_b = _parse_with_edits(
                    self.buf, self.starts[li], end_pos, edits, m._fmt)
            except ValueError:
                continue
            cand = self._make_cand(end_pos, cost, edits, fields_b)
            if cand is None:
                continue
            key = (cand.end_line, cand.fields)
            old = cands.get(key)
            if old is None or (cost, edits) < (old.cost, old.edits):
                cands[key] = cand
        out = sorted(cands.values(),
                     key=lambda c: (c.end_line, c.cost, c.edits, c.fields))
        if len(out) > self.max_candidates:
            out = out[:self.max_candidates]
            self.diagnostics['candidates_truncated'] = True
        return out

    def add_repairs(self, li):
        """Force a repair search at line ``li`` and merge into its candidates."""
        existing = self.cache.get(li, None)
        repaired = self._repair(li)
        if existing is None:
            self.cache[li] = repaired
            return
        seen = {(c.end_line, c.fields): c for c in repaired}
        for c in existing:
            key = (c.end_line, c.fields)
            old = seen.get(key)
            if old is None or (c.cost, c.edits) < (old.cost, old.edits):
                seen[key] = c
        out = sorted(seen.values(),
                     key=lambda c: (c.end_line, c.cost, c.edits, c.fields))
        if len(out) > self.max_candidates:
            out = out[:self.max_candidates]
            self.diagnostics['candidates_truncated'] = True
        self.cache[li] = out


# ---------------------------------------------------------------------------
# Outer search: tile all lines with records meeting count and total
# ---------------------------------------------------------------------------

def _solve(provider, nlines, record_count, start_line, start_cost, budget,
           amount_total, max_states=2000000):
    """Lowest-cost tilings of lines ``start_line..nlines`` into records.

    Returns ``(solutions, final_states, truncated)`` where each solution is
    a list of ``_Cand`` (in order) with minimal total cost among those whose
    amounts sum to ``amount_total`` (if given).  ``final_states`` maps
    ``amount_sum -> [cost, {path_hash: backptr}]`` at ``(nlines,
    record_count)`` for diagnostics.  ``truncated`` is True when the search
    exceeded ``max_states`` and gave up; solutions is empty then and the
    batch is *not* proven infeasible.

    Lines with exactly one candidate form deterministic chains; a chain is
    collapsed into one mega-transition and DP states only materialise at
    lines where the search branches.  This is exact: rows/sum/cost/hash all
    accumulate associatively along a chain, and the row/cost prunes are
    monotone, so checking them at the chain end is equivalent to checking
    every intermediate line.
    """
    zero = Decimal(0)
    R = record_count
    get = provider.get

    pow131 = [1] * (nlines + 2)
    for k in range(1, len(pow131)):
        pow131[k] = (pow131[k - 1] * 131) & _MASK

    # chain_jump[i] = (end, drows, dsum, dcost, dhash): the deterministic
    # records from line i up to the first line that does not have exactly
    # one candidate (or EOF).  False marks non-chain lines.
    chain_jump = {}

    def jump(i):
        cached = chain_jump.get(i)
        if cached is not None:
            return cached if cached is not False else None
        trail = []
        j = i
        while True:
            cj = chain_jump.get(j)
            if cj is not None and cj is not False:
                end, trows, tsum, tcost, thash = cj
                break
            if cj is False:
                end, trows, tsum, tcost, thash = j, 0, zero, 0, 0
                break
            cands = get(j)
            if len(cands) != 1:
                chain_jump[j] = False
                end, trows, tsum, tcost, thash = j, 0, zero, 0, 0
                break
            c = cands[0]
            trail.append(j)
            j = c.end_line
            if j >= nlines:
                end, trows, tsum, tcost, thash = j, 0, zero, 0, 0
                break
        # compose the walked records in front of the tail, memoising every
        # suffix so overlapping chains stay O(1) each
        for t in reversed(trail):
            c = get(t)[0]
            thash = ((c.hr + 1) * pow131[trows] + thash) & _MASK
            trows += 1
            if c.amount is not None:
                tsum += c.amount
            tcost += c.cost
            chain_jump[t] = (end, trows, tsum, tcost, thash)
        return chain_jump[i]

    # states[line][rows] = {amount_sum: [cost, {path_hash: backptr}]}
    states = {start_line: {0: {zero: [start_cost, {0: None}]}}}
    nstates = 0
    for i in range(start_line, nlines + 1):
        row = states.get(i)
        if not row:
            continue
        cands = get(i) if i < nlines else ()
        # transitions: (end, drows, dsum, dcost, dhash, recon)
        if len(cands) == 1:
            e, dr, ds, dc, dh = jump(i)
            transitions = ((e, dr, ds, dc, dh, ('chain', i, dr)),)
        else:
            transitions = []
            for c in cands:
                j = c.end_line
                dr = 1
                ds = c.amount if c.amount is not None else zero
                dc = c.cost
                dh = (c.hr + 1) & _MASK
                end = j
                ext = 0
                if j < nlines:
                    cj = chain_jump.get(j)
                    if cj is None and len(get(j)) == 1:
                        cj = jump(j)
                    if cj:
                        end, r2, s2, c2, h2 = cj
                        dh = (dh * pow131[r2] + h2) & _MASK
                        dr += r2
                        ds += s2
                        dc += c2
                        ext = r2
                transitions.append((end, dr, ds, dc, dh,
                                    ('candchain', c, ext)))
        for r, st in row.items():
            for ssum, (ccost, paths) in st.items():
                for phr0, bp in paths.items():
                    for end, dr, ds, dc, dh, recon in transitions:
                        nr = r + dr
                        if nr > R:
                            continue
                        nc = ccost + dc
                        if nc > budget:
                            continue
                        if nlines - end < R - nr:
                            continue  # not enough lines left for the rows
                        if end < nlines and not get(end):
                            continue  # dead end
                        ns = ssum + ds
                        nphr = (phr0 * pow131[dr] + dh) & _MASK
                        tgt = states.setdefault(end, {}).setdefault(nr, {})
                        ent = tgt.get(ns)
                        nbp = (bp, recon)
                        if ent is None:
                            tgt[ns] = [nc, {nphr: nbp}]
                            nstates += 1
                            if nstates > max_states:
                                return [], {}, True
                        elif nc < ent[0]:
                            ent[0] = nc
                            ent[1] = {nphr: nbp}
                        elif nc == ent[0]:
                            ent[1][nphr] = nbp
    final = states.get(nlines, {}).get(R, {})
    if amount_total is not None:
        matching = {s: e for s, e in final.items() if s == amount_total}
    else:
        matching = final
    if not matching:
        return [], final, False
    best = min(e[0] for e in matching.values())
    solutions = []
    seen_records = set()
    for s, (cost, paths) in matching.items():
        if cost != best:
            continue
        for bp in paths.values():
            cands = []
            while bp is not None:
                bp, recon = bp
                kind = recon[0]
                if kind == 'chain':
                    _, li, dr = recon
                    recs = []
                    for _ in range(dr):
                        c = get(li)[0]
                        recs.append(c)
                        li = c.end_line
                    cands.extend(reversed(recs))
                else:
                    _, c, dr = recon
                    li = c.end_line
                    recs = []
                    for _ in range(dr):
                        c = get(li)[0]
                        recs.append(c)
                        li = c.end_line
                    cands.extend(reversed(recs))
                    cands.append(recon[1])
            cands.reverse()
            records = tuple(c.fields for c in cands)
            if records in seen_records:
                continue
            seen_records.add(records)
            solutions.append(cands)
    return solutions, final, False


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def recover_batch(data, manifest, costs=None, budget=8,
                  max_lines_per_record=16, max_record_bytes=65536,
                  exhaustive=False, max_solutions=32, lookback=2,
                  max_states=2000000):
    """Recover one CSV batch under a manifest.

    Parameters
    ----------
    data : bytes
        The complete batch body (all chunks reassembled).
    manifest : Manifest
        Column constraints, record count and expected amount total.
    costs : EditCosts, optional
        Cost of one quote insertion/deletion (default 1/1).
    budget : int
        Maximum total edit cost allowed (default 8).
    max_lines_per_record : int
        Maximum physical lines one record may span (default 16).
    max_record_bytes : int
        Maximum byte length of one record (default 65536).
    exhaustive : bool
        When True, run the repair search from every line (slowest, strongest
        optimality guarantee).  When False (default), a fast pass repairs
        only lines whose naive parse fails (plus ``lookback`` neighbours);
        the exhaustive pass still runs automatically if the fast pass finds
        no feasible solution.
    max_solutions : int
        Maximum number of distinct optimal solutions returned (default 32).
    lookback : int
        In the fast pass, also run the repair search this many lines before
        each failed line (default 2), so repairs that merge with the
        previous record are found.  Automatically widened to
        ``max_lines_per_record`` before the exhaustive fallback runs.
    max_states : int
        Safety limit on the outer search's state count (default 2,000,000).
        When exceeded, the result is ``infeasible`` with
        ``diagnostics['search_truncated'] == True`` — the batch is not
        proven unrecoverable; raise the limit (or the budget) to retry.

    Returns
    -------
    RecoveryResult
    """
    costs = costs or EditCosts()
    budget = int(budget)
    if budget < 0:
        raise ValueError('budget must be >= 0')
    buf = bytes(data)
    bom = 0
    if manifest.strip_bom and buf.startswith(b'\xef\xbb\xbf'):
        bom = 3
        buf = buf[3:]
    starts = _line_starts(buf)
    nlines = len(starts)
    R = manifest.record_count
    diagnostics = {'lines': nlines, 'record_count': R, 'budget': budget}

    def infeasible(reason, **extra):
        diagnostics.update(extra)
        diagnostics['reason'] = reason
        return RecoveryResult('infeasible', None, None, (), None, None,
                              diagnostics)

    # ---- header -----------------------------------------------------------
    start_line = 0
    header_fields = None
    header_edits = ()
    header_cost = 0
    if manifest.has_header:
        if nlines == 0:
            return infeasible('empty batch: no header line')
        hc = _header_candidates(buf, starts, manifest, costs, budget,
                                max_lines_per_record, max_record_bytes)
        if hc is None:
            return infeasible('header line does not match the manifest')
        header_fields, header_edits, header_cost = hc
        if header_cost > budget:
            return infeasible('header repair exceeds the edit budget')
        start_line = 1

    body_lines = nlines - start_line
    if R > body_lines:
        return infeasible('record_count %d exceeds the %d body lines'
                          % (R, body_lines))
    if R == 0:
        if body_lines != 0:
            return infeasible('record_count is 0 but %d body lines remain'
                              % body_lines)
        if manifest.amount_total is not None and manifest.amount_total != 0:
            return infeasible('record_count is 0 but amount_total is %s'
                              % manifest.amount_total)
        fixes = tuple(Fix(p + bom, a) for p, a, _mode in header_edits)
        sol = Solution((), fixes, header_cost)
        status = 'clean' if header_cost == 0 else 'recovered'
        return RecoveryResult(status, (), fixes, (sol,), header_cost,
                              header_fields, diagnostics)

    # ---- candidate search + outer solve ------------------------------------
    # Iterative deepening on the per-record repair-cost cap: realistic
    # corruptions cost 1-2 edits per record, so capping the per-record
    # search far below the total budget keeps the repair DP small.  A
    # solution found with total cost C <= cap is provably optimal (any
    # cheaper solution would have per-record costs <= C <= cap and would
    # have been found); if C > cap, one verification re-run at cap = C
    # certifies optimality.
    walk_cache = {}

    def run(exhaustive_mode, cap, lb):
        provider = _Provider(buf, starts, manifest, costs, cap,
                             max_lines_per_record, max_record_bytes,
                             exhaustive_mode, 128, diagnostics,
                             walk_cache=walk_cache)
        if not exhaustive_mode:
            # Fast pass: follow the clean tiling; run the repair search only
            # at lines whose naive parse fails, plus ``lb`` neighbours
            # (continuation lines of clean multi-line records are skipped).
            li = start_line
            while li < nlines:
                cands = provider.get(li)
                if len(cands) == 1 and cands[0].cost == 0:
                    li = cands[0].end_line
                else:
                    for lj in range(max(start_line, li - lb), li):
                        provider.add_repairs(lj)
                    li += 1
        return _solve(provider, nlines, R, start_line, header_cost, budget,
                      manifest.amount_total, max_states)

    # Amount sums are accumulated with extra precision so that totals are
    # exact (no decimal-context rounding) for any realistic batch.
    with localcontext() as _ctx:
        _ctx.prec = 500
        if exhaustive:
            cap = budget
            solutions, final, search_trunc = run(True, budget, lookback)
            phase = 2
        else:
            phase = 1
            lb = lookback
            widened = False
            cap = 1 if budget > 0 else 0
            while True:
                solutions, final, search_trunc = run(False, cap, lb)
                if search_trunc:
                    break
                if solutions:
                    C = header_cost + sum(c.cost for c in solutions[0])
                    if C <= cap or cap >= budget:
                        break
                    cap = C  # one verification pass at the found cost
                elif cap >= budget:
                    if not widened and lb < max_lines_per_record:
                        # any repair covering a failed line starts within
                        # max_lines_per_record of it; widen once and retry
                        lb = max_lines_per_record
                        widened = True
                        continue
                    solutions, final, search_trunc = run(True, budget, lb)
                    phase = 2
                    break
                else:
                    cap = min(max(cap * 2, cap + 1), budget)
    diagnostics['phase'] = phase
    diagnostics['cost_cap'] = cap
    if search_trunc:
        diagnostics['search_truncated'] = True

    if not solutions:
        if search_trunc:
            return infeasible(
                'search exceeded the state limit of %d; the batch is not '
                'proven unrecoverable — raise max_states or budget to retry'
                % max_states, search_truncated=True)
        extra = {}
        if final:
            best = min(e[0] for e in final.values())
            totals = sorted({str(s) for s, e in final.items()
                             if e[0] == best})
            extra['min_cost_ignoring_total'] = best
            extra['totals_at_min_cost'] = totals[:5]
            if manifest.amount_total is not None:
                extra['expected_total'] = str(manifest.amount_total)
            reason = ('no repair within budget reaches the expected '
                      'amount total')
        else:
            reason = ('no tiling of the batch into %d valid records within '
                      'budget' % R)
        return infeasible(reason, **extra)

    # ---- build solutions ----------------------------------------------------
    solutions.sort(key=lambda cands: tuple(c.fields for c in cands))
    num_solutions = len(solutions)
    truncated = num_solutions > max_solutions
    if truncated:
        solutions = solutions[:max_solutions]
        diagnostics['solutions_truncated'] = True
    diagnostics['num_solutions'] = num_solutions

    out = []
    for cands in solutions:
        edits = list(header_edits)
        for c in cands:
            edits.extend(c.edits)
        edits.sort()
        fixes = tuple(Fix(p + bom, a) for p, a, _mode in edits)
        records = tuple(c.fields for c in cands)
        cost = header_cost + sum(c.cost for c in cands)
        out.append(Solution(records, fixes, cost))
    cost = out[0].cost
    diagnostics['cost'] = cost
    diagnostics['edits'] = len(out[0].fixes)

    if num_solutions == 1:
        sol = out[0]
        status = 'clean' if cost == 0 else 'recovered'
        return RecoveryResult(status, sol.records, sol.fixes, tuple(out),
                              cost, header_fields, diagnostics)
    return RecoveryResult('ambiguous', None, None, tuple(out), cost,
                          header_fields, diagnostics)


def _header_candidates(buf, starts, manifest, costs, budget, max_lines,
                       max_bytes):
    """Best valid parse of the header line, or None.

    Returns ``(fields, edits, cost)``.  The header must end at the first
    line boundary and match the column names (when all are known) or at
    least the column count.
    """
    fmt = manifest._fmt
    names = manifest.header_names()

    def valid(fields_b):
        if len(fields_b) != manifest.ncols:
            return None
        out = []
        for f in fields_b:
            try:
                out.append(f.decode(manifest.encoding))
            except UnicodeDecodeError:
                return None
        out = tuple(out)
        if names is not None and out != names:
            return None
        return out

    limit = min(len(buf), starts[0] + max_bytes,
                starts[1] if len(starts) > 1 else len(buf))
    w = _walk_one(buf, starts[0], fmt, limit)
    if w is not None and w[0] == (starts[1] if len(starts) > 1 else len(buf)):
        fields = valid(w[1])
        if fields is not None:
            return fields, (), 0
    end_line1 = starts[1] if len(starts) > 1 else len(buf)
    raws, _trunc = _repair_from(buf, starts, 0, len(starts), fmt,
                                manifest.ncols, costs.insert, costs.delete,
                                budget, 1, max_bytes)
    best = None
    for end_pos, cost, _hr, edits in raws:
        if end_pos != end_line1:
            continue
        try:
            fields_b = _parse_with_edits(buf, starts[0], end_pos, edits, fmt)
        except ValueError:
            continue
        fields = valid(fields_b)
        if fields is None:
            continue
        key = (cost, edits, fields)
        if best is None or key < best[0]:
            best = (key, fields, edits, cost)
    if best is None:
        return None
    return best[1], best[2], best[3]


# ---------------------------------------------------------------------------
# Stream nodes
# ---------------------------------------------------------------------------

class _BatchEnd:
    def __repr__(self):
        return 'BATCH_END'


BATCH_END = _BatchEnd()


@Stream.register_api()
class assemble_csv_batches(Stream):
    """Accumulate byte chunks into complete batches.

    Elements are byte chunks of the current batch.  When the ``boundary``
    element arrives, the accumulated chunks are concatenated and emitted as
    one ``bytes`` batch (the boundary element itself is not part of the
    batch).  This reassembly is what makes downstream recovery independent
    of how the bytes were chunked.

    Parameters
    ----------
    boundary : object or callable
        Sentinel object marking the end of a batch (default
        :data:`BATCH_END`), or a predicate called on each element.

    Examples
    --------
    >>> from streamz import Stream
    >>> source = Stream()
    >>> batches = source.assemble_csv_batches()
    >>> batches.sink(print)
    >>> source.emit(b'a,b\\n')
    >>> source.emit(b'1,2\\n')
    >>> source.emit(BATCH_END)
    b'a,b\\n1,2\\n'
    """

    def __init__(self, upstream, boundary=BATCH_END, stream_name=None):
        if callable(boundary):
            self._is_boundary = boundary
        else:
            self._is_boundary = lambda x: x is boundary or x == boundary
        self._buffer = []
        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        if self._is_boundary(x):
            if not self._buffer:
                return None
            data = b''.join(self._buffer)
            self._buffer = []
            return self._emit(data, metadata=metadata)
        if not isinstance(x, (bytes, bytearray, memoryview)):
            raise TypeError('expected bytes chunks or the batch boundary, '
                            'got %r' % type(x).__name__)
        self._buffer.append(bytes(x))
        return None

    def flush(self, metadata=None):
        """Emit the currently buffered partial batch, if any."""
        if not self._buffer:
            return None
        data = b''.join(self._buffer)
        self._buffer = []
        return self._emit(data, metadata=metadata)


@Stream.register_api()
class recover_csv(Stream):
    """Recover one CSV batch per element.

    Each upstream element must be either

    * ``bytes`` (a complete batch),
    * an iterable of byte chunks (concatenated into the batch), or
    * a ``(payload, manifest)`` tuple when the manifest differs per batch.

    Emits a :class:`RecoveryResult`.  Infeasible or ambiguous batches are
    emitted (not raised) so one poisoned batch does not kill the stream;
    inspect ``result.status``.

    Parameters
    ----------
    manifest : Manifest, optional
        The manifest applied to every batch; required unless each element
        carries its own.
    costs : EditCosts, optional
    budget : int
        Maximum total quote-edit cost per batch (default 8).
    **kwargs :
        Further :func:`recover_batch` options (``max_lines_per_record``,
        ``max_record_bytes``, ``exhaustive``, ``max_solutions``,
        ``lookback``, ``max_states``).

    Examples
    --------
    >>> from streamz import Stream
    >>> source = Stream()
    >>> results = source.recover_csv(manifest)   # doctest: +SKIP
    """

    def __init__(self, upstream, manifest=None, costs=None, budget=8,
                 stream_name=None, **kwargs):
        self.manifest = manifest
        self.costs = costs
        self.budget = budget
        self.kwargs = kwargs
        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        manifest = self.manifest
        payload = x
        if (isinstance(x, tuple) and len(x) == 2
                and isinstance(x[1], Manifest)):
            payload, manifest = x
        if manifest is None:
            raise ValueError('no manifest: pass one to recover_csv() or '
                             'emit (payload, manifest) tuples')
        data = _coerce_bytes(payload)
        result = recover_batch(data, manifest, costs=self.costs,
                               budget=self.budget, **self.kwargs)
        return self._emit(result, metadata=metadata)


def _coerce_bytes(payload):
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload)
    if isinstance(payload, str):
        raise TypeError('expected bytes, not str; encode the batch first')
    return b''.join(bytes(c) for c in payload)
