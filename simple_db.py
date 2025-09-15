import os

from b_tree import BTree


class SimpleDatabase:
    def __init__(self):
        # before an actual table is loaded, class members are set to None

        # a header is a list of column names
        # e.g., ['name', 'id', 'grade']
        self.header = None

        # map column name to column index in the header
        self.columns = None

        # None if table is not loaded
        # otherwise list b-tree indices corresponding to columns
        self.b_trees = None

        # rows contains actual data; this is a list of lists
        # specifically, this is a list of rows, where each row
        # is a list of values for each column
        # e.g., if a table with the above header has two rows
        # self.rows can be [['Alice', 'a1234', 'HD'], ['Bob', 'a7654', 'D']]
        self.rows = None

        # name of the loaded table
        self.table_name = None

    def get_table_name(self):
        return self.table_name

    def load_table(self, table_name, file_name):
        # note our DBMS only supports loading one table at a time
        # as we load new table, the old one will be lost
        print(f"loading {table_name} from {file_name} ...")

        if not os.path.isfile(file_name):
            print("File not found")
            return

        # note, you could use a CSV module here, also we don't check
        # correctness of file
        with open(file_name) as f:
            self.header = f.readline().rstrip().split(",")
            self.rows = [line.rstrip().split(",") for line in f]
        self.table_name = table_name

        self.columns = {}
        for i, column_name in enumerate(self.header):
            self.columns[column_name] = i

        self.b_trees = [None] * len(self.header)
        print("... done!")

    def select_rows(self, table_name, column_name, column_value):
        # modify this code such that row selection uses index if it exists
        # note that our DBMS only supports loading one table at a time
        if table_name != self.table_name:
            # no such table
            return [], []

        if column_name not in self.columns:
            # no such column
            return self.header, []

        col_id = self.columns[column_name]

        selected_rows = []
        for row in self.rows:
            if row[col_id] == column_value:
                selected_rows.append(row)

        return self.header, selected_rows

    def create_index(self, c):
        if c not in self.columns:
            return (False, "Error: column '" + c + "' does not exist.")

        cid = self.columns[c]
        if self.b_trees[cid] is not None:
            return (False, "Error: index already exists on '" + c + "'.")

        index_map = {}
        for i, row in enumerate(self.rows):
            key = row[cid]
            if key not in index_map:
                index_map[key] = []
            index_map[key].append(i)

        self.b_trees[cid] = index_map
        return (True, "Index created on '" + c + "'.")

    def drop_index(self, c):
        if c not in self.columns:
            return (False, "Error: column '" + c + "' does not exist.")

        cid = self.columns[c]
        if self.b_trees[cid] is None:
            return (False, "Error: no index exists on '" + c + "'.")

        self.b_trees[cid] = None
        return (True, "Index dropped on '" + c + "'.")

    def select_rows(self, t, c, v):
        if t != self.table_name:
            return [], []

        if c not in self.columns:
            return self.header, []

        cid = self.columns[c]

        # if index exists, just use it
        if self.b_trees and self.b_trees[cid] is not None:
            index_map = self.b_trees[cid]
            ids = index_map.get(v, [])
            rows_found = [self.rows[i] for i in ids]
            return self.header, rows_found

        # otherwise do full scan
        rows_found = []
        for row in self.rows:
            if row[cid] == v:
                rows_found.append(row)
        return self.header, rows_found
