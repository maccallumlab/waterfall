import numpy as np
import waterfall
from hypothesis import given, example, assume
import hypothesis.strategies as st
import pytest
import math


@given(
    log_weight=st.floats(
        allow_nan=False, allow_infinity=False, min_value=-4, max_value=4
    ),
    log_average_weight=st.floats(
        allow_nan=False, allow_infinity=False, min_value=-4, max_value=4
    ),
)
def test_log_weight_should_be_average_for_non_terminal_stage(
    log_weight, log_average_weight
):
    copies, new_log_weight = waterfall.get_copies(
        log_weight=log_weight, log_average_weight=log_average_weight
    )
    assert new_log_weight == pytest.approx(log_average_weight)


def test_return_correct_copies_for_non_terminal_stage():
    # run 1000 trials as this is a stochastic test
    results = []
    for i in range(1000):
        copies, new_log_weight = waterfall.get_copies(
            log_weight=math.log(1.25), log_average_weight=math.log(1.0)
        )
        results.append(copies)

    for result in results:
        assert (result == 1) or (result == 2)

    mean = np.mean(results)
    expected_mean = 1.25
    lower_bound = expected_mean - 0.05
    upper_bound = expected_mean + 0.05
    assert mean > lower_bound and mean < upper_bound


@given(
    max_queued=st.integers(min_value=10, max_value=200),
    current_queued=st.integers(min_value=0, max_value=100),
    requested_copies=st.integers(min_value=1, max_value=100),
)
def test_should_be_normal_when_max_queue_not_exceeded(
    max_queued, current_queued, requested_copies
):
    assume(current_queued + requested_copies <= max_queued)

    normal_copies, merged_copies = waterfall.get_merged_copies(
        max_queued, current_queued, requested_copies
    )

    assert normal_copies == requested_copies
    assert merged_copies == 0


@given(
    max_queued=st.integers(min_value=10, max_value=200),
    current_queued=st.integers(min_value=0, max_value=100),
    requested_copies=st.integers(min_value=1, max_value=100),
)
def test_excess_should_be_in_merged_copy(max_queued, current_queued, requested_copies):
    assume(current_queued + requested_copies > max_queued)

    normal_copies, merged_copies = waterfall.get_merged_copies(
        max_queued, current_queued, requested_copies
    )

    assert normal_copies + merged_copies == requested_copies
    assert merged_copies > 0
