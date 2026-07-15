"""Unit tests for ETL-FND-001 lookback scenario fixtures (no database)."""

from datetime import date, timedelta

import pytest

from tests.fixtures.test_data_definitions import (
    PROC_STATUS_COMPLETE,
    PROC_STATUS_TP,
    get_tp_complete_lookback_scenario,
)


@pytest.mark.unit
def test_tp_complete_lookback_scenario_window_and_frozen_stamp():
    tp_row, complete_row = get_tp_complete_lookback_scenario(proc_num=1, days_ago=5)

    assert tp_row["ProcStatus"] == PROC_STATUS_TP
    assert complete_row["ProcStatus"] == PROC_STATUS_COMPLETE
    assert tp_row["DateTStamp"] == complete_row["DateTStamp"]
    assert complete_row["DateComplete"] == tp_row["ProcDate"]
    assert complete_row["ProcDate"] >= date.today() - timedelta(days=30)
    assert float(complete_row["ProcFee"]) == 160.0
