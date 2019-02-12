import datetime
import sqlite3
import urllib.request


"""
Creates a table from a CSV file of item URLs to be fetched and then fetches those items  
"""


class ItemHarvester:
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/35.0.1916.47 Safari/537.36 '

    def __init__(self, db):
        self.conn = sqlite3.connect(db)

    def run(self, tbl_name, items_csv, resume=True):
        if not resume or not self.tableExists(tbl_name):
            # Start from scratch, drop and recreate tables OR tables don't already exist
            self.recreateTable(tbl_name, items_csv, resume)

        if not self.tableExists('logs'):
            self.createLogsTable()

        self.fetchItems(tbl_name)

    def cleanup(self):
        if self.conn:
            self.conn.close()

    def tableExists(self, tbl_name):
        # query to check whether the table already exists
        qry = ''' SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'; ''' % tbl_name
        with self.conn:
            cursor = self.conn.cursor()
            results = cursor.execute(qry).fetchone()[0]

        if results == 0:
            print("Table `%s` does not exist. Creating..." % tbl_name)
            return False
        else:
            return True

    def populateDatabase(self, tbl_name, file):
        # Read values from file into values
        with open(file) as f:
            f.seek(0) # seek to start of file, probably unnecessary
            lines = f.read()
            values = [(i, None) for i in lines.splitlines()]

        insert_stmt = "INSERT INTO %s (url, data) VALUES (?, ?);" % tbl_name

        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.executemany(insert_stmt, values)
        except sqlite3.Error as e:
            print('Failed to populate table `%s` with values from %s: %s' % (e, file, tbl_name))

        print("Populated table `%s` with %s values from %s" % (tbl_name, len(values), file))

    def getUnfetchedUrls(self, tbl_name):
        items = []
        select_stmt = '''SELECT url FROM %s WHERE data is null''' % tbl_name
        with self.conn:
            cursor = self.conn.cursor()
            results = cursor.execute(select_stmt).fetchall()
            for r in results:
                items.append(r[0])
        return items

    def getData(self, tbl_name):
        items = []
        select_stmt = '''SELECT data FROM %s WHERE data is not null''' % tbl_name
        with self.conn:
            cursor = self.conn.cursor()
            results = cursor.execute(select_stmt).fetchall()
            for r in results:
                items.append(r[0])
        return items

    def getRowCount(self, tbl_name):
        select_stmt = '''SELECT count(*) FROM %s ''' % tbl_name
        with self.conn:
            cursor = self.conn.cursor()
            results = cursor.execute(select_stmt).fetchone()[0]
            return results

    def fetchItems(self, tbl_name):
        total_items = self.getRowCount(tbl_name)
        unfetched_items = self.getUnfetchedUrls(tbl_name)

        print('There are %s total URLs and %s URLs remaining to be fetched from `%s`' %
              (total_items, len(unfetched_items), tbl_name))

        update_tbl = "UPDATE %s " % tbl_name

        while len(unfetched_items) > 0:
            for item in unfetched_items:
                try:
                    # Make request
                    request = urllib.request.Request(
                        item,
                        data=None,
                        headers={'User-Agent': self.user_agent}
                    )
                    print("Requesting %s" % request.full_url)

                    import timeit
                    import time

                    start_timer = timeit.default_timer()
                    start_time = datetime.datetime.now()

                    response = urllib.request.urlopen(request).read()

                    end_timer = timeit.default_timer()
                    end_time = datetime.datetime.now()

                    time = end_timer - start_timer

                    self.updateRow(tbl_name, (response, item))
                    self.logRequest(request.full_url, start_time, end_time, time)

                # if request failed record url with no data
                except urllib.request.HTTPError as http_error:
                    # Remove rows where the error is 4xx, because those will never succeed and will cause an
                    # infinite loop
                    if 400 <= http_error.code <= 499:  # FIXME kludgey -- find a better way
                        print('Http error %s when requesting %s -- item will not be retried' % (http_error.code, item))
                        self.deleteRow(item, tbl_name)
                    else:
                        print('Error requesting %s: %s' % (item, http_error))
                        self.updateRow(tbl_name, (None, item))

                except Exception as e:
                    print('Error requesting %s: %s' % (item, e))
                    self.updateRow(tbl_name, (None, item))

            # Get an updated set of unfetched pages
            unfetched_items = self.getUnfetchedUrls(tbl_name)

    def updateRow(self, tbl_name, values):
        update_stmt = "UPDATE %s SET data = ? WHERE url = ?" % tbl_name
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(update_stmt, values)
        except sqlite3.Error as e:
            print('Error updating row %s in table `%s`: %s' % (values[0],tbl_name, e))

    def deleteRow(self, url, tbl_name):
        delete_stmt = '''DELETE FROM %s ''' % tbl_name
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(delete_stmt + " WHERE url = ?", (url,))
        except sqlite3.Error as e:
            print('Error deleting row %s from table `%s`: %s' % (url, tbl_name, e))

    def recreateTable(self, tbl_name, items_csv, resume):
        # Drop existing table and recreate table and populate with values from CSV file
        try:
            with self.conn:
                self.dropTable(tbl_name)
                self.createTable(tbl_name)
                self.populateDatabase(tbl_name, items_csv)
        except sqlite3.Error as e:
            print('Failed database setup: %s' % e)

    def dropTable(self, tbl_name):
        drop_tbl_stmt = '''DROP TABLE IF EXISTS %s''' % tbl_name
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(drop_tbl_stmt)
                print("Created table `%s`" % tbl_name)
        except sqlite3.Error as e:
            print('Failed to drop table `%s`: %s' % (e, tbl_name))

    def createTable(self, tbl_name):
        create_tbl_stmt = '''CREATE TABLE %s ('url','data')''' % tbl_name
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(create_tbl_stmt)
                print("Created table `%s`" % tbl_name)
        except sqlite3.Error as e:
            print('Failed to create table `%s`: %s' % (e, tbl_name))

    def createLogsTable(self):
        create_tbl_stmt = '''CREATE TABLE logs ('url','start_time','end_time','total_time')'''
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(create_tbl_stmt)
                print("Created `logs` table")
        except sqlite3.Error as e:
            print('Failed to create `logs` table: %s' % e)

    def logRequest(self, full_url, start, end, time):
        insert_stmt = "INSERT INTO logs (url, start_time, end_time, total_time) VALUES (?, ?, ?, ?);"

        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.executemany(insert_stmt, [(full_url, start, end, time)])
        except sqlite3.Error as e:
            print('Failed to add log row: %s' % e)
