from decimal import Decimal

from reconciliation.engine import result, serializable


def test_minor_units_above_binary_float_precision():
    check = result('money', Decimal('9007199254740993.01'), Decimal('9007199254740993.02'))
    assert check['status'] == 'FAIL'
    assert check['difference'] == Decimal('0.01')
    assert serializable(check)['difference'] == '0.01'


def test_auditable_pass_and_warning():
    assert result('missing', 0, 0)['status'] == 'PASS'
    check = result('stale', 0, 1, severity='warning', evidence={'source': 'WAL'}, ids=['a'])
    assert check['status'] == 'WARNING'
    assert check['source_evidence'] == {'source': 'WAL'}
    assert check['related_record_ids'] == ['a']


def test_currency_totals_are_compared_per_currency():
    expected = [{'currency': 'USD', 'total': Decimal('100')}, {'currency': 'GBP', 'total': Decimal('200')}]
    observed = [{'currency': 'USD', 'total': Decimal('200')}, {'currency': 'GBP', 'total': Decimal('100')}]
    check = result('currency_totals', expected, observed)
    assert check['status'] == 'FAIL'
    assert check['difference'] == {'GBP': Decimal(-100), 'USD': Decimal(100)}
