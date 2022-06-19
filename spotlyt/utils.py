from sqlitedict import SqliteDict
from datetime import datetime
from spotlyt.tools import get_uuid

class Table:
    """Defines a new tables to store content in."""

    def __init__(self, fpath, table):
        """Initialize a new table."""
        self.table = SqliteDict(fpath, tablename=table, autocommit=True)

    def set(self, data, uid=None):
        """Set a new value in the table.

        Args:
            data (dict): The data to set.
        """

        if type(data) != dict:
            raise ValueError("Expected `data` of type `dict")
        
        if not uid:
            uid = get_uuid()

        data["_meta_timestamp"] = str(datetime.now().isoformat()) 

        self.table[uid] = data

    def get(self, key, default=None):
        """"Get a value from the table.
        
        Args:
            key (str): The key to get.
            default (any): The default value to return if the key is not found.
        """

        val = self.table.get(key)
        return val if val else default

    def items(self, sort=False):
        """Get all items in the table.
        
        Args:
            sort (bool): Whether to sort the items by the time
                         it was added.
        """

        data = list(self.table.items())
        if sort:
            data = sorted(data, key=lambda x: datetime.fromisoformat(x[1]["_meta_timestamp"]), reverse=True)
        return data
    
    def delete(self, uid):
        """Delete a value from the table.

        Args:
            uid (str): The uid/key of the value to delete.
        """
        del self.table[uid]
    
    def ids(self):
        """Get all uids in the table."""

        return self.table.keys()
