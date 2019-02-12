import sys
import csv
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

    def __init__(self):
        pass
        # self.conn = sqlite3.connect(db)

    def generateCollectionSitemapUrls(self, collections):
        sitemap_urls = []
        for collection in collections:
            sitemap_urls.append(['https://www.loc.gov/collections/%s/sitemap.xml' % collection])
        return sitemap_urls

    def getCollectionPageUrls(self, coll_sitemap_data):
        collection_pages = []
        for page in coll_sitemap_data:
            for p in self.parseCollectionSitemapXml(page):
                collection_pages.append([p])
        return collection_pages

    def getItemUrls(self, collection_page_data):
        item_pages = []
        for page in collection_page_data:
            for p in self.parseItemsFromResponse(page):
                item_pages.append([p])
        return item_pages

    def parseCollectionSitemapXml(self, rsp):
        coll_pages = []
        coll_sitemap_xml = ET.ElementTree(ET.fromstring(rsp))
        for i in coll_sitemap_xml.getroot().findall('.//{%s}loc' % self.sitemap_ns):
            coll_pages.append(i.text)
        return coll_pages

    def parseItemsFromResponse(self, rsp):
        items = []
        xml = ET.ElementTree(ET.fromstring(rsp))
        for i in xml.getroot().findall('.//{%s}loc' % self.sitemap_ns):
            if i.text.startswith('http://www.loc.gov/item/'):  # filter out non-items
                items.append(i.text + "?fo=json&at=item")  # append parameters
        return items

    def writeUrlsToCsv(self, file, data):
        fos = open(file, "w")
        with fos:
            csv_writer = csv.writer(fos)
            csv_writer.writerows(data)


def main(args):
    if len(args) != 2:
        print("Not enough arugments passed")
        sys.exit(-1)

    base_dir = args[1]
    print("Root directory: %s" % base_dir)

    # LC co
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

    # lc database
    db = base_dir + "/db/lc-harvest.db"

    # table names
    sitemap_tbl = 'sitemap'
    collection_tbl = 'collection'
    item_tbl = 'item'

    # CSV output files
    sitemap_csv = base_dir + "/csv/lc-sitemaps.csv"
    collection_page_csv = base_dir + "/csv/lc-pages.csv"
    items_csv = base_dir + "/csv/lc-items.csv"

    lc = LcHarvester()
    item_harvester = ItemHarvester(db)

    """ SITEMAP """
    # Generate sitemap.xml URLs to request
    sitemap_urls = lc.generateCollectionSitemapUrls(collections)

    # write sitemap URLs to CSV file
    lc.writeUrlsToCsv(sitemap_csv, sitemap_urls)

    # Request all collection sitemap URLs in sitemap CSV
    item_harvester.run(sitemap_tbl, sitemap_csv)

    # get collection pages from sitemap responses
    collection_sitemap_data = item_harvester.getData(sitemap_tbl)

    # parse the sitemap data and extract the collection page URLs to fetch
    collection_pages = lc.getCollectionPageUrls(collection_sitemap_data)

    # write collection page URLs to CSV file
    lc.writeUrlsToCsv(collection_page_csv, collection_pages)

    print('%s collection page URLs written to %s' % (len(collection_pages), collection_page_csv))

    """ COLLECTION PAGES """
    # request collection pages via ItemHarvester
    item_harvester.run(collection_tbl, collection_page_csv)

    # get the harvested collection page data
    collection_page_data = item_harvester.getData(collection_tbl)

    # Extract item urls to harvest from collection_page_data
    item_urls = lc.getItemUrls(collection_page_data)

    # Write the item urls to a CSV file
    lc.writeUrlsToCsv(items_csv, item_urls)

    print('%s item URLs written to %s' % (len(item_urls), items_csv))

    """ ITEM PAGES """
    # harvest item pages
    item_harvester.run(item_tbl, items_csv)

    # TODO Write item JSON out to individual files
    item_data = item_harvester.getData(item_tbl)

    print("Harvested %s item records" % len(item_data))

    item_harvester.cleanup()


if __name__ == '__main__':
    main(sys.argv)
