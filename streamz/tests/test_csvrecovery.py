import csv
import io
import random
import time
from decimal import Decimal, localcontext

import pytest

from streamz import Stream
from streamz.csvrecovery import (
    BATCH_END,
    ColumnSpec,
    EditCosts,
    Manifest,
    RecoveryResult,
    assemble_csv_batches,
    recover_batch,
    recover_csv,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def exact_total(records, index=2):
    with localcontext() as ctx:
        ctx.prec = 500
        return +sum((Decimal(r[index]) for r in records), Decimal(0))


def make_csv_bytes(records, header=('id', 'note', 'amount'), lineterminator='\n'):
    sio = io.StringIO()
    w = csv.writer(sio, lineterminator=lineterminator)
    if header is not None:
        w.writerow(header)
    w.writerows(records)
    return sio.getvalue().encode('utf-8')


def make_manifest(records, has_header=True, **kw):
    return Manifest(
        columns=[ColumnSpec('integer', name='id', allow_empty=False),
                 ColumnSpec('string', name='note'),
                 ColumnSpec('decimal', name='amount', allow_empty=False)],
        record_count=len(records),
        amount_column='amount',
        amount_total=exact_total(records),
        has_header=has_header,
        **kw)


def signature(result):
    sols = tuple(sorted(tuple(tuple(r) for r in s.records)
                        for s in result.solutions))
    fixes = None if result.fixes is None else tuple(
        (f.position, f.action) for f in result.fixes)
    return (result.status, result.cost, result.records, fixes, sols)


def corrupt_delete(data, pos):
    assert data[pos:pos + 1] == b'"'
    return data[:pos] + data[pos + 1:]


def corrupt_insert(data, pos):
    return data[:pos] + b'"' + data[pos:]


# ---------------------------------------------------------------------------
# clean batches
# ---------------------------------------------------------------------------

def test_clean_batch_passthrough():
    records = [('1', 'a,b', '10.00'), ('2', 'x\ny', '20.00'), ('3', 'z', '-5.50')]
    data = make_csv_bytes(records)
    result = recover_batch(data, make_manifest(records))
    assert result.status == 'clean'
    assert result.cost == 0
    assert result.fixes == ()
    assert result.records == tuple(tuple(r) for r in records)
    assert result.header == ('id', 'note', 'amount')
    assert len(result.solutions) == 1


def test_clean_batch_no_header():
    records = [('1', 'a', '1.00'), ('2', 'b', '2.00')]
    data = make_csv_bytes(records, header=None)
    result = recover_batch(data, make_manifest(records, has_header=False))
    assert result.status == 'clean'
    assert result.records == tuple(tuple(r) for r in records)
    assert result.header is None


def test_escaped_quotes_in_content():
    records = [('1', 'say "hi" there', '3.25'), ('2', 'a""b', '1.00')]
    data = make_csv_bytes(records)
    result = recover_batch(data, make_manifest(records))
    assert result.status == 'clean'
    assert result.records == tuple(tuple(r) for r in records)


def test_crlf_line_endings():
    records = [('1', 'a,b', '10.00'), ('2', 'x\ny', '20.00')]
    data = make_csv_bytes(records, lineterminator='\r\n')
    result = recover_batch(data, make_manifest(records))
    assert result.status == 'clean'
    assert result.records == tuple(tuple(r) for r in records)


def test_empty_fields_and_optional_columns():
    records = [('1', '', '10.00'), ('2', 'x', '20.00')]
    data = make_csv_bytes(records)
    result = recover_batch(data, make_manifest(records))
    assert result.status == 'clean'
    assert result.records[0][1] == ''


def test_decimal_exactness():
    # 0.1 + 0.2 != 0.3 in float; the sum must be exact decimal arithmetic
    records = [('1', 'a', '0.10'), ('2', 'b', '0.20'), ('3', 'c', '0.30'),
               ('4', 'd', '999999999999999999.99')]
    data = make_csv_bytes(records)
    total = exact_total(records)
    result = recover_batch(data, make_manifest(records))
    assert result.status == 'clean'
    assert result.diagnostics['cost'] == 0
    # a total computed with floats would not match exactly
    bad = make_manifest(records)
    bad.amount_total = total + Decimal('0.01')
    result2 = recover_batch(data, bad)
    assert result2.status == 'infeasible'
    # and huge totals are accumulated without rounding
    big = [('1', 'a', '999999999999999999.99'),
           ('2', 'b', '0.000000000000000001')]
    big_data = make_csv_bytes(big)
    big_manifest = make_manifest(big)
    assert big_manifest.amount_total == Decimal('999999999999999999.990000000000000001')
    assert recover_batch(big_data, big_manifest).status == 'clean'
    off = make_manifest(big)
    with localcontext() as ctx:
        ctx.prec = 500
        off.amount_total = +(big_manifest.amount_total
                             + Decimal('0.000000000000000001'))
    assert recover_batch(big_data, off).status == 'infeasible'


def test_thousands_separator():
    records = [('1', 'a', '1,234.50'), ('2', 'b', '2,000.00')]
    data = make_csv_bytes(records)
    manifest = make_manifest(
        [('1', 'a', '1234.50'), ('2', 'b', '2000.00')],
        thousands_separator=',')
    result = recover_batch(data, manifest)
    assert result.status == 'clean'
    assert result.records == tuple(tuple(r) for r in records)


# ---------------------------------------------------------------------------
# quote corruption recovery
# ---------------------------------------------------------------------------

def test_missing_opening_quote():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'x', '20.00')]
    data = make_csv_bytes(records)
    pos = data.index(b'"a')  # opening quote of the note field
    broken = corrupt_delete(data, pos)
    result = recover_batch(broken, make_manifest(records))
    assert result.status == 'recovered'
    assert result.cost == 1
    assert result.records == tuple(tuple(r) for r in records)
    assert len(result.fixes) == 1
    assert result.fixes[0].action == 'insert_quote'


def test_missing_closing_quote():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'x', '20.00')]
    data = make_csv_bytes(records)
    pos = data.index(b'c",') + 1  # closing quote of the note field
    broken = corrupt_delete(data, pos)
    result = recover_batch(broken, make_manifest(records))
    assert result.status == 'recovered'
    assert result.cost == 1
    assert result.records == tuple(tuple(r) for r in records)


def test_extra_quote():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'xy', '20.00')]
    data = make_csv_bytes(records)
    pos = data.index(b'xy,20') + 1  # inside the second note
    broken = corrupt_insert(data, pos)
    result = recover_batch(broken, make_manifest(records))
    assert result.status == 'recovered'
    assert result.cost == 1
    assert result.records == tuple(tuple(r) for r in records)


def test_multiple_corruptions():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'xy', '20.00'),
               ('3', 'p,q', '30.00')]
    data = make_csv_bytes(records)
    broken = corrupt_delete(data, data.index(b'"a'))       # missing open
    broken = corrupt_insert(broken, broken.index(b'xy,20') + 1)  # extra quote
    broken = corrupt_delete(broken, broken.index(b'q",') + 1)  # missing close
    result = recover_batch(broken, make_manifest(records), budget=6)
    assert result.status == 'recovered'
    assert result.cost == 3
    assert result.records == tuple(tuple(r) for r in records)


def test_extra_quote_merging_records_gets_split_back():
    records = [('1', 'a', '1.00'), ('2', 'b', '2.00'), ('3', 'c', '3.00')]
    data = make_csv_bytes(records)
    pos = data.index(b'a,1.00') + 1  # stray quote at end of first note
    broken = corrupt_insert(data, pos)
    result = recover_batch(broken, make_manifest(records))
    assert result.status == 'recovered'
    assert len(result.records) == 3  # no rows dropped, no rows merged
    assert result.records == tuple(tuple(r) for r in records)


def test_budget_exceeded_is_infeasible():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'xy', '20.00'),
               ('3', 'p,q', '30.00')]
    data = make_csv_bytes(records)
    broken = corrupt_delete(data, data.index(b'"a'))
    broken = corrupt_insert(broken, broken.index(b'xy,20') + 1)
    broken = corrupt_delete(broken, broken.index(b'q",') + 1)
    result = recover_batch(broken, make_manifest(records), budget=2)
    assert result.status == 'infeasible'
    assert result.records is None
    assert result.solutions == ()
    assert 'reason' in result.diagnostics


def test_wrong_total_is_infeasible():
    records = [('1', 'a', '1.00'), ('2', 'b', '2.00')]
    data = make_csv_bytes(records)
    manifest = make_manifest(records)
    manifest.amount_total = Decimal('3.01')
    result = recover_batch(data, manifest)
    assert result.status == 'infeasible'
    assert result.diagnostics['min_cost_ignoring_total'] == 0
    assert result.diagnostics['totals_at_min_cost'] == ['3.00']


def test_wrong_record_count_is_infeasible():
    records = [('1', 'a', '1.00'), ('2', 'b', '2.00')]
    data = make_csv_bytes(records)
    manifest = make_manifest(records)
    manifest.record_count = 3
    result = recover_batch(data, manifest)
    assert result.status == 'infeasible'


def test_exhaustive_mode_agrees_with_fast_pass():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'xy', '20.00'),
               ('3', 'p,q', '30.00')]
    data = make_csv_bytes(records)
    broken = corrupt_delete(data, data.index(b'"a'))
    broken = corrupt_insert(broken, broken.index(b'xy,20') + 1)
    manifest = make_manifest(records)
    fast = recover_batch(broken, manifest)
    full = recover_batch(broken, manifest, exhaustive=True)
    assert signature(fast) == signature(full)
    assert fast.status == 'recovered'


def test_fast_pass_agrees_with_exhaustive_randomized():
    # the deepening fast pass (with chain compression) must find exactly
    # the same optimal solutions as the exhaustive reference search
    rnd = random.Random(2024)
    for _trial in range(12):
        n = rnd.randint(1, 30)
        records = []
        for i in range(n):
            kind = rnd.randint(0, 4)
            if kind == 0:
                note = 'plain%d' % i
            elif kind == 1:
                note = 'with,comma%d' % i
            elif kind == 2:
                note = 'with\nnewline%d' % i
            elif kind == 3:
                note = 'with"quote%d' % i
            else:
                note = 'mix,"ed"\nall%d' % i
            records.append((str(i), note, '%d.%02d' % (rnd.randint(0, 99),
                                                      rnd.randint(0, 99))))
        header = rnd.random() < 0.8
        data = make_csv_bytes(records,
                              header=('id', 'note', 'amount') if header
                              else None)
        broken = data
        for _ in range(rnd.randint(0, 3)):
            if rnd.random() < 0.5:
                qp = [i for i, b in enumerate(broken) if b == 0x22]
                if not qp:
                    break
                broken = corrupt_delete(broken, rnd.choice(qp))
            else:
                broken = corrupt_insert(broken,
                                        rnd.randrange(len(broken) + 1))
        manifest = make_manifest(records, has_header=header)
        budget = rnd.randint(2, 6)
        fast = recover_batch(broken, manifest, budget=budget)
        full = recover_batch(broken, manifest, budget=budget,
                             exhaustive=True)
        if fast.diagnostics.get('search_truncated') or \
                full.diagnostics.get('search_truncated'):
            continue  # resource-limited searches may legitimately differ
        assert signature(fast) == signature(full)


def test_lookback_finds_merge_with_previous_record():
    # both quotes of a quoted field are missing; the first fragment still
    # looks like a valid record, so the repair must merge with it
    records = [('7', 'x,1\nz', '3.50')]
    data = make_csv_bytes(records)
    broken = data.replace(b'"x,1\nz"', b'x,1\nz')
    assert broken != data
    result = recover_batch(broken, make_manifest(records))
    assert result.status == 'recovered'
    assert result.cost == 2
    assert result.records == (('7', 'x,1\nz', '3.50'),)


# ---------------------------------------------------------------------------
# ambiguity
# ---------------------------------------------------------------------------

def test_ambiguous_recovery_reports_all_solutions():
    # a stray quote inside a quoted field: deleting it (content unchanged)
    # and pairing it with an inserted quote (content gains a literal quote)
    # are both valid, cost 1, and reach the expected total -- but the
    # recovered records differ, so the result must be flagged ambiguous
    manifest = Manifest(
        columns=[ColumnSpec('integer', name='id', allow_empty=False),
                 ColumnSpec('string', name='note'),
                 ColumnSpec('decimal', name='amount', allow_empty=False)],
        record_count=1, amount_column='amount',
        amount_total=Decimal('5.00'), has_header=False)
    data = b'1,"ab"cd",5.00\n'
    result = recover_batch(data, manifest)
    assert result.status == 'ambiguous'
    assert result.records is None  # never picks one arbitrarily
    assert result.cost == 1
    found = {s.records for s in result.solutions}
    assert found == {(('1', 'abcd', '5.00'),), (('1', 'ab"cd', '5.00'),)}
    # every solution is complete: exactly the expected number of rows
    for s in result.solutions:
        assert len(s.records) == 1
        assert len(s.fixes) == 1


def test_ambiguity_respects_max_solutions():
    manifest = Manifest(
        columns=[ColumnSpec('integer', name='id', allow_empty=False),
                 ColumnSpec('string', name='note'),
                 ColumnSpec('decimal', name='amount', allow_empty=False)],
        record_count=1, amount_column='amount',
        amount_total=Decimal('5.00'), has_header=False)
    result = recover_batch(b'1,"ab"cd",5.00\n', manifest, max_solutions=1)
    assert result.status == 'ambiguous'
    assert len(result.solutions) == 1
    assert result.diagnostics['solutions_truncated']
    assert result.diagnostics['num_solutions'] == 2


# ---------------------------------------------------------------------------
# chunking independence
# ---------------------------------------------------------------------------

def chunkings(data, n_trials=25, seed=1234):
    rnd = random.Random(seed)
    yield [data]  # one chunk
    yield [data[i:i + 1] for i in range(len(data))]  # one byte per chunk
    yield [data[:1], data[1:]]  # split after first byte
    for _ in range(n_trials):
        cuts = sorted(rnd.sample(range(1, len(data)), min(len(data) - 1,
                                                          rnd.randint(1, 20))))
        chunks, prev = [], 0
        for c in cuts:
            chunks.append(data[prev:c])
            prev = c
        chunks.append(data[prev:])
        yield chunks


def test_chunking_independence():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'xy', '20.00'),
               ('3', 'plain', '30.00'), ('4', 'p,q\nr\ns', '40.00')]
    data = make_csv_bytes(records)
    broken = corrupt_delete(data, data.index(b'"a'))
    broken = corrupt_insert(broken, broken.index(b'xy,20') + 1)
    broken = corrupt_delete(broken, broken.index(b's",') + 1)
    manifest = make_manifest(records)

    expected = None
    for chunks in chunkings(broken):
        source = Stream()
        results = (source
                   .assemble_csv_batches(BATCH_END)
                   .recover_csv(manifest, budget=6)
                   .sink_to_list())
        for chunk in chunks:
            source.emit(chunk)
        source.emit(BATCH_END)
        assert len(results) == 1
        sig = signature(results[0])
        if expected is None:
            expected = sig
        else:
            assert sig == expected
    assert expected[0] == 'recovered'
    assert expected[1] == 3  # cost
    assert expected[2] == tuple(tuple(r) for r in records)


def test_recover_csv_accepts_chunk_lists_and_tuples():
    records = [('1', 'a', '1.00')]
    data = make_csv_bytes(records)
    manifest = make_manifest(records)

    source = Stream()
    out = source.recover_csv(manifest).sink_to_list()
    source.emit(data)                      # one bytes element
    source.emit([data[:5], data[5:]])      # iterable of chunks
    source.emit((data, manifest))          # (payload, manifest) pair
    assert len(out) == 3
    assert all(signature(r) == signature(out[0]) for r in out)
    assert out[0].status == 'clean'


def test_per_batch_manifests():
    recs1 = [('1', 'a', '1.00')]
    recs2 = [('1', 'a,b\nc', '2.00'), ('2', 'd', '3.00')]
    data1 = make_csv_bytes(recs1)
    data2 = corrupt_delete(make_csv_bytes(recs2),
                           make_csv_bytes(recs2).index(b'"a'))
    m1 = make_manifest(recs1)
    m2 = make_manifest(recs2)

    source = Stream()
    out = source.recover_csv().sink_to_list()
    source.emit((data1, m1))
    source.emit((data2, m2))
    assert [r.status for r in out] == ['clean', 'recovered']
    assert out[1].records == tuple(tuple(r) for r in recs2)


def test_assemble_multiple_batches_and_flush():
    source = Stream()
    assembler = source.assemble_csv_batches()
    out = assembler.sink_to_list()
    source.emit(b'a')
    source.emit(b'b')
    source.emit(BATCH_END)
    source.emit(b'c')
    source.emit(BATCH_END)
    source.emit(b'd')
    assembler.flush()
    assert out == [b'ab', b'c', b'd']


def test_recover_csv_requires_manifest():
    source = Stream()
    source.recover_csv().sink_to_list()
    with pytest.raises(ValueError):
        source.emit(b'plain bytes without manifest')


# ---------------------------------------------------------------------------
# headers and validation
# ---------------------------------------------------------------------------

def test_header_mismatch_is_infeasible():
    records = [('1', 'a', '1.00')]
    data = make_csv_bytes(records, header=('id', 'note', 'WRONG'))
    result = recover_batch(data, make_manifest(records))
    assert result.status == 'infeasible'


def test_column_constraints_reject_bad_repairs():
    # the comma inside the note must stay inside quotes; the repair that
    # splits it into the amount column is rejected by the decimal constraint
    records = [('1', 'a,b', '10.00')]
    data = make_csv_bytes(records)
    broken = corrupt_delete(data, data.index(b'"a'))
    result = recover_batch(broken, make_manifest(records))
    assert result.status == 'recovered'
    assert result.records == (('1', 'a,b', '10.00'),)


def test_record_count_zero():
    data = make_csv_bytes([], header=('id', 'note', 'amount'))
    manifest = make_manifest([])
    result = recover_batch(data, manifest)
    assert result.status == 'clean'
    assert result.records == ()


def test_to_csv_roundtrip():
    records = [('1', 'a,b\nc', '10.00'), ('2', 'x"y', '20.00')]
    data = make_csv_bytes(records)
    broken = corrupt_delete(data, data.index(b'"a'))
    result = recover_batch(broken, make_manifest(records))
    assert result.status == 'recovered'
    text = result.to_csv()
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed == [['id', 'note', 'amount']] + [list(r) for r in records]


# ---------------------------------------------------------------------------
# larger batches / determinism
# ---------------------------------------------------------------------------

def test_large_batch_performance_and_determinism():
    rnd = random.Random(42)
    n = 20000
    records = []
    for i in range(n):
        note = 'note %d' % i
        if i % 17 == 0:
            note = 'embed,comma\nand newline %d' % i
        if i % 31 == 0:
            note = 'with "quotes" %d' % i
        records.append((str(i), note, '%d.%02d' % (i % 1000, i % 100)))
    data = make_csv_bytes(records)
    manifest = make_manifest(records)

    # sprinkle a few quote corruptions (positions recomputed after each
    # edit, since every insertion/deletion shifts the bytes after it)
    broken = data
    for _ in range(2):
        qp = [i for i, b in enumerate(broken) if b == 0x22]
        broken = corrupt_delete(broken, rnd.choice(qp))
    for _ in range(2):
        broken = corrupt_insert(broken, rnd.randrange(len(broken)))

    t0 = time.time()
    r1 = recover_batch(broken, manifest, budget=8)
    elapsed = time.time() - t0
    r2 = recover_batch(broken, manifest, budget=8)
    assert elapsed < 60  # generous CI bound; typically a few seconds
    assert signature(r1) == signature(r2)  # deterministic
    assert r1.status in ('clean', 'recovered')
    assert len(r1.records) == n
    total = sum((Decimal(row[2]) for row in r1.records), Decimal(0))
    assert total == manifest.amount_total


def test_result_repr_and_ok():
    records = [('1', 'a', '1.00')]
    result = recover_batch(make_csv_bytes(records), make_manifest(records))
    assert result.ok
    assert 'clean' in repr(result)
    bad = recover_batch(make_csv_bytes(records), make_manifest(records),
                        budget=0)
    assert bad.ok
