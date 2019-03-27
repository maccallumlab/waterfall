"""Conflict-free Replicated Data Types

This module contains an implmentation of several CRDTs
sufficient for use in Waterfall. It's not meant to be
a general purpose library.
"""

from collections import namedtuple


AverageEntry = namedtuple("AverageEntry", "count sum")
TombstoneEntry = namedtuple("TombstoneEntry", "inserted removed")
UUIDMapItem = namedtuple("UUIDDictItem", "key value")


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

    def add(self, item):
        """Insert an item"""
        self._data.add(item)

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

    def add(self, item):
        """Insert an item

        `remove` always wins, so that once an item has been
        removed, it can never be re-`add`ed. 
        """
        self._inserted.add(item)

    def remove(self, item):
        """Remove an item

        `remove` always wins, so that once an item has been
        removed, it can never be re-`add`ed. 
        """
        self._removed.add(item)

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
