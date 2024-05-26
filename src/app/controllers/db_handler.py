from abc import ABC, abstractmethod


class DatabaseHandler(ABC):
    @abstractmethod
    def get(self, *args, **kwargs):
        """
        Retrieve data from the database.
        """
        pass

    @abstractmethod
    def insert(self, data):
        """
        Insert data into the database.
        """
        pass

    @abstractmethod
    def get_or_insert(self, data, *args, **kwargs):
        """
        Retrieve data from the database if it exists,
        otherwise insert it and return the inserted data.
        """
        pass
