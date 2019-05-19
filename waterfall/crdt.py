"""Conflict-free Replicated Data Types

This module contains an implmentation of several CRDTs
sufficient for use in Waterfall. It's not meant to be
a general purpose library.
"""

from collections import namedtuple
import math


AverageEntry = namedtuple("AverageEntry", "count sum")
LogAverageEntry = namedtuple("LogAverageEntry", "log_sum count")
TombstoneEntry = namedtuple("TombstoneEntry", "inserted removed")
UUIDMapItem = namedtuple("UUIDMapItem", "key value")


class GCounter:
    """A grow-only counter

    Parameters
    ----------
    client_id : string
                a unique identifier for this client, usually a UUID
    """

    def __init__(self, client_id):
        self.client_id = client_id
        self._data = {self.client_id: 0}

    def increment(self, val=1):
        """Increment the counter
        """
        assert val > 0
        self._data[self.client_id] += val

    @property
    def value(self):
        """Return the total count
        """
        count = 0
        for _, value in self._data.items():
            count += value
        return count

    @property
    def payload(self):
        """The payload containing all information observed

        Returns
        -------
        dict
            each key represents the observations from a single client
        """
        return self._data

    def merge(self, x):
        """Merge the observations into `self`

        Parameters
        ----------
        x : GCounter
            observations from `x` will be merged into `self`
        """
        assert isinstance(x, GCounter)
        for key, value in x.payload.items():
            if key in self._data:
                if value > self._data[key]:
                    self._data[key] = value
            else:
                self._data[key] = value

    def __getstate__(self):
        # only pickle our own observations, not all observations
        # this is a performance optimization
        state = {
            "client_id": self.client_id,
            "_data": {self.client_id: self._data[self.client_id]},
        }
        return state


def log_sum_exp(log_w1, log_w2):
    "Return ln(exp(log_w1) + exp(log_w2)) avoiding overflow."
    log_max = max(log_w1, log_w2)
    return log_max + math.log(math.exp(log_w1 - log_max) + math.exp(log_w2 - log_max))


class LogAverageWeight:
    def __init__(self, client_id):
        self.client_id = client_id
        self._data = {self.client_id: LogAverageEntry(-math.inf, 0)}

    def add_observation(self, log_weight):
        """Add an observation

        Parameters
        ----------
        log_weight : float
              the observation to add
        """
        old_log_sum, old_count = self._data[self.client_id]
        self._data[self.client_id] = LogAverageEntry(
            log_sum_exp(old_log_sum, log_weight), old_count + 1
        )

    @property
    def value(self):
        """The log average value

        Returns
        -------
        float
            the log average value across all clients
        """
        count = 0
        sum = -math.inf
        for _, value in self._data.items():
            count += value.count
            sum = log_sum_exp(sum, value.log_sum)

        if count == 0:
            raise ValueError("Tried to get value of empty GAverage.")

        return sum - math.log(count)

    @property
    def count(self):
        """The total count of observations

        Returns
        -------
        int
            the total number of observations across all clients
        """
        count = 0
        for _, value in self._data.items():
            count += value.count
        return count

    @property
    def payload(self):
        """The payload containing all information observed

        Returns
        -------
        dict
            each key represents the observations from a single client
        """
        return self._data

    def merge(self, x):
        """Merge the observations into `self`

        Parameters
        ----------
        x : GAverage
            observations from `x` will be merged into `self`
        """
        assert isinstance(x, LogAverageWeight)
        for key, value in x.payload.items():
            if key in self._data:
                if value.count > self._data[key].count:
                    self._data[key] = value
            else:
                self._data[key] = value

    def __getstate__(self):
        # only pickle our own observations, not all observations
        state = {
            "client_id": self.client_id,
            "_data": {self.client_id: self._data[self.client_id]},
        }
        return state


class GAverage:
    """A grow-only average CRDT

    This class keeps track of the average of a series of
    values, where observations can only ever be added,
    but not removed.

    Parameters
    ----------
    client_id : string
                a unique identifier for this client, usually a UUID
    """

    def __init__(self, client_id):
        self.client_id = client_id
        self._data = {self.client_id: AverageEntry(0, 0.0)}

    def add_observation(self, obs):
        """Add an observation to a GAverage

        Parameters
        ----------
        obs : float
              the observation to add
        """
        old_count, old_sum = self._data[self.client_id]
        self._data[self.client_id] = AverageEntry(old_count + 1, old_sum + obs)

    @property
    def value(self):
        """The average value

        Returns
        -------
        float
            the average value across all clients
        """
        count = 0
        sum = 0.0
        for _, value in self._data.items():
            count += value.count
            sum += value.sum

        if count == 0:
            raise ValueError("Tried to get value of empty GAverage.")

        return sum / count

    @property
    def count(self):
        """The total count of observations

        Returns
        -------
        int
            the total number of observations across all clients
        """
        count = 0
        for _, value in self._data.items():
            count += value.count
        return count

    @property
    def payload(self):
        """The payload containing all information observed

        Returns
        -------
        dict
            each key represents the observations from a single client
        """
        return self._data

    def merge(self, x):
        """Merge the observations into `self`

        Parameters
        ----------
        x : GAverage
            observations from `x` will be merged into `self`
        """
        assert isinstance(x, GAverage)
        for key, value in x.payload.items():
            if key in self._data:
                if value.count > self._data[key].count:
                    self._data[key] = value
            else:
                self._data[key] = value

    def __getstate__(self):
        # only pickle our own observations, not all observations
        state = {
            "client_id": self.client_id,
            "_data": {self.client_id: self._data[self.client_id]},
        }
        return state


class GSet:
    """A grow only set

    Parameters
    ----------
    client_id : string
                a unique identifier for this client, usually a UUID
    """

    def __init__(self, client_id):
        self.client_id = client_id
        self._data = set()
        self._mine = set()

    def add(self, item):
        """Insert an item"""
        self._data.add(item)
        self._mine.add(item)

    @property
    def items(self):
        """List of items

        Returns
        -------
        list
            a list of all items inserted
        """
        return list(self._data)

    @property
    def payload(self):
        """The payload containing all information observed

        Returns
        -------
        set
            set of items inserted
        """
        return self._data

    def merge(self, x):
        """Merge the observations into `self`

        Parameters
        ----------
        x : GSet
            observations from `x` will be merged into `self`
        """
        assert isinstance(x, GSet)
        for item in x.payload:
            self._data.add(item)

    def __getstate__(self):
        # only pickle our own observations, not all observations
        state = {"client_id": self.client_id, "_mine": self._mine}
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._data = self._mine.copy()


class TombstoneSet:
    """A set that can grow or shrink

    `remove` always wins, so that once an item has been
    removed, it can never be re-added.

    Parameters
    ----------
    client_id : string
                a unique identifier for this client, usually a UUID
    """

    def __init__(self, client_id):
        self.client_id = client_id
        self._inserted = set()
        self._removed = set()
        self._my_inserted = set()
        self._my_removed = set()

    def add(self, item):
        """Insert an item

        `remove` always wins, so that once an item has been
        removed, it can never be re-`add`ed. 
        """
        self._inserted.add(item)
        self._my_inserted.add(item)

    def remove(self, item):
        """Remove an item

        `remove` always wins, so that once an item has been
        removed, it can never be re-`add`ed. 
        """
        self._removed.add(item)
        self._my_removed.add(item)

    @property
    def items(self):
        """List of active items

        Returns
        -------
        list
            a list of all items that have been inserted, but not removed
        """
        alive = self._inserted.difference(self._removed)
        return list(alive)

    @property
    def payload(self):
        """The payload containing all information observed

        Returns
        -------
        TomstoneEntry
            the sets of items inserted and removed
        """
        return TombstoneEntry(self._inserted, self._removed)

    def merge(self, x):
        """Merge the observations into `self`

        Parameters
        ----------
        x : TombstoneSet
            observations from `x` will be merged into `self`
        """
        assert isinstance(x, TombstoneSet)
        self._inserted.update(x.payload.inserted)
        self._removed.update(x.payload.removed)

    def __getstate__(self):
        # only pickle our own observations, not all observations
        state = {
            "client_id": self.client_id,
            "_my_inserted": self._my_inserted,
            "_my_removed": self._my_removed,
        }
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._inserted = self._my_inserted.copy()
        self._removed = self._my_removed.copy()


class UUIDMap:
    """A grow-only map of UUIDs to values

    This is a mapping between UUIDs and values. The correct
    operation of this class assumes that a particular key will
    only ever be set once. We assume that different nodes will
    never try to set the same key, because they are using UUIDs
    or hashes, so there is no risk of collision.

    Parameters
    ----------
    client_id : string
                a unique identifier for this client, usually a UUID
    """

    def __init__(self, client_id):
        self.client_id = client_id
        self._data = GSet(client_id)
        self._dict = {}

    def __getitem__(self, key):
        """Get an item"""
        return self._dict[key]

    def __setitem__(self, key, value):
        """Set an item

        Note that once set, an item cannot be changed
        """
        if key in self._dict:
            raise RuntimeError("Entries in a UUIDMap cannot be re-written")
        self._dict[key] = value
        self._data.add(UUIDMapItem(key, value))

    def __contains__(self, key):
        return key in self._dict

    @property
    def payload(self):
        """The payload containing all information observed

        Returns
        -------
        GSet
            A GSet containing UUIDMapItem(key, value) pairs
        """
        return self._data

    def merge(self, x):
        """Merge the observations into `self`

        Parameters
        ----------
        x : UUIDMap
            observations from `x` will be merged into `self`
        """
        assert isinstance(x, UUIDMap)
        self._data.merge(x.payload)
        for item in self._data.items:
            self._dict[item.key] = item.value
