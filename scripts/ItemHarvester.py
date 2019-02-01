

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
        self.setupDatabase()

    def cleanup(self):
        if self.conn:
            self.conn.close()

    def run(self, items_csv):
        # populate the `items` table with the values in the CSV file
        self.populateDatabase(items_csv)

        # Log database population results
        with self.conn:
            cursor = self.conn.cursor()
            cnt = cursor.execute('''SELECT count(*) FROM items WHERE url is not null''').fetchone()[0]
            print('Read %s item urls from %s' % (cnt, items_csv))

        # Fetch items
        self.fetchItems()

    def populateDatabase(self, items_csv):
        with open(items_csv) as f:
            lines = f.read().splitlines()
            to_db = [(i, None) for i in lines]

        with self.conn:
            cursor = self.conn.cursor()
            cursor.executemany("INSERT INTO items (url, data) VALUES (?, ?);", to_db)

    def getUnfetchedItems(self):
        items = []
        with self.conn:
            cursor = self.conn.cursor()
            results = cursor.execute('''SELECT url FROM items WHERE data is null''').fetchall()
            for r in results:
                items.append(r[0])
        return items

    def fetchItems(self):
        unfetched_items = self.getUnfetchedItems()
        while len(unfetched_items) > 0:
            for item in unfetched_items:
                try:
                    # Make request
                    request = urllib.request.Request(
                        item,
                        data=None,
                        headers={'User-Agent': self.user_agent}
                    )
                    response = urllib.request.urlopen(request).read()

                    try:
                        with self.conn:
                            cursor = self.conn.cursor()
                            cursor.execute("UPDATE items SET data = ? WHERE url = ?", (response, item))
                    except sqlite3.Error as e:
                        print('Error update page data for %s: %s' % (item, e))

                # if request failed record url with no data
                except urllib.request.HTTPError as http_error:
                    # Remove rows where the error is 4xx, because those will never succeed and will cause an
                    # infinite loop
                    if 400 <= http_error.code <= 499:  # FIXME kludgey -- find a better way
                        print('Http error %s when requesting %s -- item will not be retried' % (http_error.code, item))
                        self.deleteRow(item)
                    else:
                        print('Error requesting %s: %s' % (item, http_error))
                        with self.conn:
                            cursor = self.conn.cursor()
                            cursor.execute("UPDATE items SET data = ? WHERE url = ?", (None, item))

                except Exception as e:
                    print('Error requesting %s: %s' % (item, e))
                    with self.conn:
                        cursor = self.conn.cursor()
                        cursor.execute("UPDATE items SET data = ? WHERE url = ?", (None, item))

            # Get an updated set of unfetched pages
            unfetched_items = self.getUnfetchedItems()

    def deleteRow(self, url):
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM items WHERE url = ?", (url,))

    def setupDatabase(self):
        # Drop existing table and recreate
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute('''DROP TABLE IF EXISTS items''')
                cursor.execute('''CREATE TABLE items ('url','data')''')
        except sqlite3.Error as e:
            print('Failed database setup: %s' % e)
