import sys
import csv
import xml.etree.ElementTree as ET

from ItemHarvester import ItemHarvester
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


class LcSitemapHarvester:
    sitemap_ns = 'http://www.sitemaps.org/schemas/sitemap/0.9'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/35.0.1916.47 Safari/537.36 '

    def generate_collection_sitemap_urls(self, collections):
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

    def parseCollectionSitemapXml(self, rsp):
        coll_pages = []
        coll_sitemap_xml = ET.ElementTree(ET.fromstring(rsp))
        for i in coll_sitemap_xml.getroot().findall('.//{%s}loc' % self.sitemap_ns):
            coll_pages.append(i.text)
        return coll_pages

    def writeUrlsToCsv(self, file, data):
        fos = open(file, "w")
        with fos:
            csv_writer = csv.writer(fos)
            csv_writer.writerows(data)

    def harvestSitemaps(self, db, tbl, collections, input, output):
        """

        :param db:
        :param tbl:
        :param collections:
        :param input:
        :param output:
        :return:
        """

        item_harvester = ItemHarvester(db)

        # Generate sitemap.xml URLs to request
        sitemap_urls = self.generate_collection_sitemap_urls(collections)

        # write sitemap URLs to CSV file
        self.writeUrlsToCsv(input, sitemap_urls)

        # Request all collection sitemap URLs in sitemap CSV
        item_harvester.run(tbl, input)

        # get collection pages from sitemap responses
        collection_sitemap_data = item_harvester.getData(tbl)

        # parse the sitemap data and extract the collection page URLs to fetch
        collection_pages = self.getCollectionPageUrls(collection_sitemap_data)

        # write collection page URLs to CSV file
        self.writeUrlsToCsv(output, collection_pages)

        item_harvester.cleanup()

        print('%s collection page URLs written to %s' % (len(collection_pages), output))


def main(args):

    if len(args) != 6:
        print("Not enough arguments passed. Expecting: input data CSV [page URLs], output CSV [item URLs], "
              "database path, table name, collections [comma separated]")
        sys.exit(-1)

    input_csv = args[1]
    print("Input CSV: %s" % input_csv)

    output_csv = args[2]
    print("Output CSV: %s" % output_csv)

    db = args[3]
    print("DB: %s" % db)

    tbl = args[4]
    print("Table name: %s" % db)

    collections = args[5].split(',')
    print("Collections to harvest: %s" % collections)

    lc = LcSitemapHarvester()

    lc.harvestSitemaps(db, tbl, collections, input_csv, output_csv)


if __name__ == '__main__':
    main(sys.argv)
