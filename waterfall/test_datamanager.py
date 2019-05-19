import numpy as np
from waterfall import datamanager
import contextlib
import os
import tempfile
from hypothesis import given, example
import hypothesis.strategies as st
import pytest
import time
import math


N_STAGES = 20


@contextlib.contextmanager
def in_temp():
    cwd = os.getcwd()
    temp = tempfile.mkdtemp()
    os.chdir(temp)
    yield
    os.chdir(cwd)


#
# Task queuing
#
def test_task_queue_single_clinet():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d = datamanager.DataManager.activate()
        assert d.n_queued_tasks == 0
        assert d.get_random_task() is None

        task = "foo"
        d.queue_task(task)
        assert d.n_queued_tasks == 1
        retrieved_task = d.get_random_task()
        assert retrieved_task == task
        assert d.n_queued_tasks == 0


def test_task_queue_multiple_clients():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d1 = datamanager.DataManager.activate()
        d2 = datamanager.DataManager.activate()

        assert d1.n_queued_tasks == 0
        assert d2.n_queued_tasks == 0
        assert d1.get_random_task() is None
        assert d2.get_random_task() is None

        task1 = "foo1"
        task2 = "foo2"
        d1.queue_task(task1)
        d2.queue_task(task2)
        assert d1.n_queued_tasks == 2
        assert d2.n_queued_tasks == 2

        r1 = d1.get_random_task()
        r2 = d1.get_random_task()
        assert d1.n_queued_tasks == 0
        assert d2.n_queued_tasks == 0
        assert d1.get_random_task() is None
        assert d2.get_random_task() is None
        assert (r1 == task1) or (r1 == task2)
        assert (r2 == task1) or (r2 == task2)


#
# In progress
#
def test_add_remove_in_progress():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d = datamanager.DataManager.activate()
        assert d.n_in_progress == 0
        assert d.get_orphan_task() is None

        task = "foo"
        in_progress_id = d.add_in_progress(task, 60)
        assert d.n_in_progress == 1

        d.remove_in_progress(in_progress_id)
        assert d.n_in_progress == 0
        assert d.get_orphan_task() is None


def test_does_not_find_orphan_before_timeout():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d = datamanager.DataManager.activate()
        task = "foo"
        d.add_in_progress(task, 60)
        assert d.get_orphan_task() is None


def test_finds_orphan_after_timeout():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d = datamanager.DataManager.activate()
        task = "foo"
        d.add_in_progress(task, 0.5)
        # pause for 1 second so we have exceeded deadline
        time.sleep(1)
        assert d.get_orphan_task() == task
        assert d.n_in_progress == 0
        assert d.get_orphan_task() is None


def test_finds_orphan_after_timeout_with_multiple_clients():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d1 = datamanager.DataManager.activate()
        d2 = datamanager.DataManager.activate()
        task = "foo"
        d1.add_in_progress(task, 1)
        assert d1.get_orphan_task() is None
        assert d2.get_orphan_task() is None
        # pause for 2 seconds so we have exceeded deadline
        time.sleep(2)
        assert d2.get_orphan_task() == task
        assert d1.get_orphan_task() is None
        assert d2.get_orphan_task() is None
        assert d1.n_in_progress == 0
        assert d2.n_in_progress == 0


@given(
    weights=st.lists(
        st.floats(
            allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
        ),
        min_size=1,
    ),
    stage=st.integers(min_value=0, max_value=N_STAGES - 2),
)
def test_average_weights_single_client(weights, stage):
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d = datamanager.DataManager.activate()
        for w in weights:
            d.log_average[stage].update_weight(math.log(w), 1)
        log_average = d.log_average[stage].log_average_weight()
        assert log_average == pytest.approx(math.log(np.mean(weights)))


@given(
    weight_mults=st.lists(
        st.tuples(
            st.floats(
                allow_nan=False, allow_infinity=False, min_value=1e-99, max_value=1e99
            ),
            st.integers(min_value=1, max_value=1000),
        ),
        min_size=1,
    ),
    stage=st.integers(min_value=0, max_value=N_STAGES - 2),
)
def test_average_weights_with_multiplicity(weight_mults, stage):
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d = datamanager.DataManager.activate()
        weights = []
        for w, mult in weight_mults:
            for _ in range(mult):
                weights.append(w)
            d.log_average[stage].update_weight(math.log(w), mult)
        log_average = d.log_average[stage].log_average_weight()
        assert log_average == pytest.approx(math.log(np.mean(weights)))


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
    stage=st.integers(min_value=0, max_value=N_STAGES - 2),
)
def test_average_weights_multiple_client(weights1, weights2, stage):
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d1 = datamanager.DataManager.activate()
        d2 = datamanager.DataManager.activate()

        for w in weights1:
            d1.log_average[stage].update_weight(math.log(w), 1)
        for w in weights2:
            d2.log_average[stage].update_weight(math.log(w), 1)
        log_average1 = d1.log_average[stage].log_average_weight()
        log_average2 = d2.log_average[stage].log_average_weight()
        assert log_average1 == pytest.approx(math.log(np.mean(weights1 + weights2)))
        assert log_average2 == pytest.approx(math.log(np.mean(weights1 + weights2)))
        assert log_average1 == log_average2


#
# Structures
#
def test_can_save_and_load_structure_single_client():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d1 = datamanager.DataManager.activate()
        s1 = np.random.normal(size=(100, 3))
        s2 = np.random.normal(size=(100, 3))
        id1 = d1.save_structure(s1)
        id2 = d1.save_structure(s2)
        e1 = d1.load_structure(id1)
        e2 = d1.load_structure(id2)
        assert np.all(e1 == s1)
        assert np.all(e2 == s2)
        assert len(d1.get_structure_ids()) == 2


def test_can_save_and_load_structure_multiple_client():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d1 = datamanager.DataManager.activate()
        d2 = datamanager.DataManager.activate()
        s1 = np.random.normal(size=(100, 3))
        s2 = np.random.normal(size=(100, 3))
        id1 = d1.save_structure(s1)
        id2 = d2.save_structure(s2)
        e1 = d2.load_structure(id1)
        e2 = d1.load_structure(id2)
        assert np.all(e1 == s1)
        assert np.all(e2 == s2)
        assert len(d1.get_structure_ids()) == 2
        assert len(d2.get_structure_ids()) == 2


#
# Provenances
#
def test_can_add_provenances_single_client():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d1 = datamanager.DataManager.activate()

        id = datamanager.gen_random_id()
        lineage = datamanager.gen_random_id()
        parent = None
        stage = 0
        s1 = np.random.normal(size=(100, 3))
        s2 = np.random.normal(size=(100, 3))
        id1 = d1.save_structure(s1)
        id2 = d1.save_structure(s2)
        w1 = 1.0
        w2 = 2.0
        mult = 1
        copies = 1
        prov = datamanager.Provenance(
            id, lineage, parent, stage, id1, w1, id2, w2, mult, copies
        )
        d1.save_provenance(prov)
        provs = d1.get_provenances()
        assert len(provs) == 1
        assert provs[0] == prov


def test_can_add_provenances_multiple_client():
    with in_temp():
        datamanager.DataManager.initialize(N_STAGES)
        d1 = datamanager.DataManager.activate()
        d2 = datamanager.DataManager.activate()

        id1 = datamanager.gen_random_id()
        id2 = datamanager.gen_random_id()

        lineage = datamanager.gen_random_id()

        parent1 = None
        stage1 = 0
        parent2 = id1
        stage2 = 1

        s1 = np.random.normal(size=(100, 3))
        s2 = np.random.normal(size=(100, 3))
        s3 = np.random.normal(size=(100, 3))
        sid1 = d1.save_structure(s1)
        sid2 = d1.save_structure(s2)
        sid3 = d1.save_structure(s3)
        w1 = 1.0
        w2 = 2.0
        w3 = 3.0

        mult = 1
        copies = 1

        prov1 = datamanager.Provenance(
            id1, lineage, parent1, stage1, sid1, w1, sid2, w2, mult, copies
        )
        d1.save_provenance(prov1)
        prov2 = datamanager.Provenance(
            id2, lineage, parent2, stage2, sid2, w2, sid3, w3, mult, copies
        )
        d2.save_provenance(prov2)

        provs1 = d1.get_provenances()
        provs2 = d2.get_provenances()
        assert len(provs1) == 2
        assert len(provs2) == 2
        assert provs1[0] == prov1 or provs1[0] == prov2
        assert provs1[1] == prov1 or provs1[1] == prov2
        assert provs2[0] == prov1 or provs2[0] == prov2
        assert provs2[1] == prov1 or provs2[1] == prov2
