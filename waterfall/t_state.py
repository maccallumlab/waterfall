from waterfall import state, waterfall_runner
import numpy as np
from hypothesis import given, example
import hypothesis.strategies as st
import pytest
import contextlib
import os
import math
import tempfile
from collections import Counter

N_STAGES = 20


@contextlib.contextmanager
def in_temp():
    cwd = os.getcwd()
    temp = tempfile.mkdtemp()
    os.chdir(temp)
    yield
    os.chdir(cwd)


@given(
    weights=st.lists(
        st.floats(
            allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
        ),
        min_size=1,
    ),
    stage=st.integers(min_value=0, max_value=N_STAGES - 1),
)
@example(weights=[1e-99], stage=19)
def test_log_average_weight_single_state(weights, stage):
    with in_temp():
        s = state.State(N_STAGES)
        mean_weight = np.mean(weights)
        log_mean_weight = math.log(mean_weight)
        for w in weights:
            with s.transact():
                s.add_log_weight_observation(stage, math.log(w))

        with s.transact():
            log_average_weight = s.get_log_average_weight(stage)

        assert log_average_weight == pytest.approx(log_mean_weight)


@given(
    weights1=st.lists(
        st.floats(
            allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
        ),
        min_size=1,
    ),
    weights2=st.lists(
        st.floats(
            allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
        ),
        min_size=1,
    ),
    weights3=st.lists(
        st.floats(
            allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
        ),
        min_size=1,
    ),
    stage=st.integers(min_value=0, max_value=N_STAGES - 1),
)
def test_log_average_weight_multiple_states(weights1, weights2, weights3, stage):
    with in_temp():
        s1 = state.State(N_STAGES)
        s2 = state.State(N_STAGES)
        s3 = state.State(N_STAGES)
        mean_weight = np.mean(weights1 + weights2 + weights3)
        log_mean_weight = math.log(mean_weight)

        for w in weights1:
            with s1.transact():
                s1.add_log_weight_observation(stage, math.log(w))

        for w in weights2:
            with s2.transact():
                s2.add_log_weight_observation(stage, math.log(w))
        for w in weights3:
            with s3.transact():
                s3.add_log_weight_observation(stage, math.log(w))

        with s1.transact():
            log_average_weight1 = s1.get_log_average_weight(stage)

        with s2.transact():
            log_average_weight2 = s2.get_log_average_weight(stage)

        with s3.transact():
            log_average_weight3 = s3.get_log_average_weight(stage)

        assert log_average_weight1 == pytest.approx(log_mean_weight)
        assert log_average_weight2 == pytest.approx(log_mean_weight)
        assert log_average_weight3 == pytest.approx(log_mean_weight)
        assert log_average_weight1 == pytest.approx(log_average_weight2)
        assert log_average_weight1 == pytest.approx(log_average_weight3)
        assert log_average_weight2 == pytest.approx(log_average_weight3)


@given(
    weights=st.lists(
        st.floats(
            allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
        ),
        min_size=1,
    ),
    stage=st.integers(min_value=0, max_value=N_STAGES - 1),
    multiplier=st.integers(min_value=1, max_value=10),
)
def test_get_copies_and_log_average_weight(weights, stage, multiplier):
    with in_temp():
        s = state.State(N_STAGES)
        mean_weight = np.mean(weights)
        expected_log_mean_weight = math.log(mean_weight)
        test_weight = multiplier * mean_weight
        for w in weights:
            with s.transact():
                s.add_log_weight_observation(stage, math.log(w))

        with s.transact():
            copies, log_mean_weight = s.get_copies_and_log_weight(
                stage, math.log(test_weight)
            )

        if stage == N_STAGES - 1:
            assert log_mean_weight == pytest.approx(math.log(test_weight))
            assert copies == 0
        else:
            assert log_mean_weight == pytest.approx(expected_log_mean_weight)
            assert copies == multiplier


@given(
    weights=st.lists(
        st.floats(
            allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
        ),
        min_size=1,
    ),
    stage=st.integers(min_value=0, max_value=N_STAGES - 1),
)
def test_get_copies_and_log_average_weight_fractional(weights, stage):
    with in_temp():
        s = state.State(N_STAGES)
        mean_weight = np.mean(weights)
        expected_log_mean_weight = math.log(mean_weight)
        # Our test weight is 1.5 times the average,
        # so we expect to get 1 or 2 copies, each
        # with p=0.5.
        test_weight = 1.5 * mean_weight
        for w in weights:
            with s.transact():
                s.add_log_weight_observation(stage, math.log(w))

        # We run 100 copies to gather statistics, as this is a
        # stochastic test.
        copies = []
        logs = []
        for _ in range(100):
            with s.transact():
                copy, log_mean_weight = s.get_copies_and_log_weight(
                    stage, math.log(test_weight)
                )
                copies.append(copy)
                logs.append(log_mean_weight)

        if stage == N_STAGES - 1:
            for copy in copies:
                assert copy == 0
            for log in logs:
                assert log == pytest.approx(math.log(test_weight))
        else:
            for log in logs:
                assert log == pytest.approx(expected_log_mean_weight)
            c = Counter(copies)
            # We should get a mix of 1 or 2 copies.
            assert len(c) == 2
            # We should get each option 1/2 of the time.
            # +/- 25 should be a 3 sigma deviation, so
            # this should almost never fail.
            assert c[1] > 35 and c[1] < 65
            assert c[2] > 35 and c[2] < 65


@given(
    copies=st.integers(min_value=1),
    total_queued=st.integers(min_value=0),
    max_queue_size=st.integers(min_value=1),
)
def test_calculate_merged_copies(copies, total_queued, max_queue_size):
    normal_copies, merged, merged_mult = waterfall_runner._calculate_merged_copies(
        copies, total_queued, max_queue_size
    )
    if copies + total_queued <= max_queue_size:
        assert normal_copies == copies
        assert merged is False

    if copies + total_queued > max_queue_size:
        assert merged is True
        assert merged_mult == copies + total_queued - max_queue_size
        assert normal_copies + merged_mult == copies


