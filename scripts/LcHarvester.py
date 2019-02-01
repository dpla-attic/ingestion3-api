import csv
import sqlite3
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from scripts.ItemHarvester import ItemHarvester

"""
Library of Congress harvester 

    - Drops and recreates all database tables at runtime 
    - Fetches collection sitemaps for list of collection pages to be fetch and retries on failure
    - Fetches collection pages [and indefinitely re-tries on failure] to retrieve a list of item 
        URLs to harvest. Writes item URLs out as to a CSV file 
    - ItemHarvester.py reads item URLs to be harvested from CSV file
    - Fetches item metadata from URLs in CSV file. Indefinitely retries on failure except for 4xx errors
    - Harvested item metadata is stored in an `items` table in the sqlite3 database   
          
"""


class LcHarvester:
    sitemap_ns = 'http://www.sitemaps.org/schemas/sitemap/0.9'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/35.0.1916.47 Safari/537.36 '

    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.setupDatabase()

    def cleanup(self):
        if self.conn:
            self.conn.close()

    def run(self, collections):
        # if exists, drop and recreate the `collections` table in database file
        self.setupDatabase()

        # for each collection name, fetch the paginated sitemap files
        for collection in collections:
            print('Fetching sitemap.xml for %s' % collection)

            # TODO fetching the sitemaps can fail and should be retryable
            coll_sitemap_xml = self.fetchCollectionSitemapXml(collection)
            coll_pages = self.parseCollectionSitemapXml(coll_sitemap_xml)

            print('%s has %s pages' % (collection, len(coll_pages)))

            # Insert collection pages into `collections` table
            self.addPagesToCollectionsTable(coll_pages)

            # Fetch pages from `collections` table
            self.fetchCollectionPages()

    def fetchCollectionPages(self):
        unfetched_pages = self.getUnfetchedPages()
        while len(unfetched_pages) > 0:
            for page in unfetched_pages:
                print('Requesting %s' % page)
                try:
                    # Make request
                    request = urllib.request.Request(
                        page,
                        data=None,
                        headers={'User-Agent': self.user_agent}
                    )
                    response = urllib.request.urlopen(request).read()
                    results = self.parseItemsFromResponse(response)

                    try:
                        with self.conn:
                            cursor = self.conn.cursor()
                            cursor.execute("UPDATE collections SET data = ? WHERE url = ?", (response, page))
                    except sqlite3.Error as e:
                        print('Error update page data for %s: %s' % (page, e))

                    try:
                        params = {'fo': 'json', 'at': 'item'}
                        to_db = [(i + '?' + urllib.parse.urlencode(params), None) for i in results]
                        with self.conn:
                            cursor = self.conn.cursor()
                            cursor.executemany("INSERT INTO items (url, data) VALUES (?, ?);", to_db)
                    except sqlite3.Error as e:
                        print('Error inserting item urls into `items`: %s' % e)

                # if request failed record url with no data
                except Exception as e:
                    print('Error requesting %s: %s' % (page, e))
            # Get an updated set of unfetched pages
            unfetched_pages = self.getUnfetchedPages()

    def getUnfetchedPages(self):
        pages = []
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute('''SELECT collections.url as 'url' FROM collections WHERE data is null''')
            for i in cursor.fetchall():
                pages.append(i[0])

        return pages

    def addPagesToCollectionsTable(self, pages):
        # FIXME there is a more efficient way to add multiple rows to a table
        for page in pages:
            try:
                with self.conn:
                    cursor = self.conn.cursor()
                    cursor.execute('''INSERT INTO 'collections' ('url','data') VALUES (?,?)''', (page, None))
            except sqlite3.Error as e:
                print("Error adding %s to `collections` table: %s" % (page, e))

    def fetchCollectionSitemapXml(self, collection):
        sitemap_url = 'https://www.loc.gov/collections/%s/sitemap.xml' % collection
        try:
            request = urllib.request.Request(
                sitemap_url,
                data=None,
                headers={
                    'User-Agent': self.user_agent
                }
            )
            response = urllib.request.urlopen(request).read()
            return ET.ElementTree(ET.fromstring(response))
        except Exception as e:
            print('Error requesting sitemap %s' % sitemap_url)

    def parseItemsFromResponse(self, rsp):
        items = []
        xml = ET.ElementTree(ET.fromstring(rsp))
        for i in xml.getroot().findall('.//{%s}loc' % self.sitemap_ns):
            items.append(i.text)
        return items

    def parseCollectionSitemapXml(self, coll_sitemap_xml):
        coll_pages = []
        for i in coll_sitemap_xml.getroot().findall('.//{%s}loc' % self.sitemap_ns):
            coll_pages.append(i.text)
        return coll_pages

    def setupDatabase(self):
        # Drop existing table and recreate
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute('''DROP TABLE IF EXISTS collections''')
                cursor.execute('''CREATE TABLE collections ('url','data')''')

                cursor.execute('''DROP TABLE IF EXISTS items''')
                cursor.execute('''CREATE TABLE items ('url','data')''')
        except sqlite3.Error as e:
            print('Failed database setup: %s' % e)

    def printSummary(self):
        with self.conn:
            cursor = self.conn.cursor()
            pageCount = cursor.execute('''SELECT count(*) FROM collections WHERE data is not null''').fetchone()[0]
            itemCount = cursor.execute('''SELECT count(*) FROM items WHERE url LIKE '%http://www.loc.gov/item%' ''').fetchone()[
                    0]
        print('Fetched %s collection sitemap pages\n'
              'Fetched %s loc.gov/item/ urls' % (pageCount, itemCount))

    def writeItems(self, items_csv):
        csvWriter = csv.writer(open(items_csv, "w"))

        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute('''SELECT url FROM items WHERE url LIKE '%http://www.loc.gov/item%' ''')
            rows = cursor.fetchall()
            csvWriter.writerows(rows)
        print('`items.url` written to %s' % items_csv)


def main():
    # FIXME paths and collection names should not be hardcoded
    db = r"./db/lc-harvest.db"
    items_csv = "./csv/lc-items.csv"
    collections = [
        'civil-war-maps',
        'american-revolutionary-war-maps',
        'panoramic-maps',
        'bain',
        'detroit-publishing-company',
        'fsa-owi-color-photographs',
        'harris-ewing',
        'national-photo-company'
    ]

    # Perform collection harvest
    collection_harvest(db, collections, items_csv)

    # Perform item harvest
    item_harvest(db, items_csv)


def collection_harvest(db, collections, items_csv):
    """
    Runs the harvest of collection pages which will generate a list of item URLs to be fetched

    :param db: Path to database
    :param collections: List of collection names
    :param items_csv: Path to write out item URLs
    :return:
    """
    lc = LcHarvester(db)

    # harvest collection pages and item urls
    lc.run(collections)

    # print a summary
    lc.printSummary()

    # write items urls out to CSV file
    lc.writeItems(items_csv)

    # close db connection
    lc.cleanup()


def item_harvest(db, items_csv):
    """
    Runs the harvest of item URLs
    :param db: Path to database
    :param items_csv: Path to item URL CSV
    :return:
    """
    item_harvester = ItemHarvester(db)

    item_harvester.run(items_csv)

    item_harvester.cleanup()


if __name__ == '__main__':
    main()
