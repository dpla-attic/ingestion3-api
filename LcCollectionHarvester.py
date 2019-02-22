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


class LcCollectionHarvester:
    sitemap_ns = 'http://www.sitemaps.org/schemas/sitemap/0.9'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/35.0.1916.47 Safari/537.36 '

    def getItemUrls(self, collection_page_data):
        item_pages = []
        for page in collection_page_data:
            for p in self.parse_items_from_response(page):
                item_pages.append([p])
        return item_pages

    def parse_items_from_response(self, rsp):
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

    def harvestCollections(self, db, tbl, input_csv, output_csv):
        """

        :param db:
        :param tbl:
        :param input_csv:
        :param output:
        :return:
        """

        item_harvester = ItemHarvester(db)

        # request collection pages via ItemHarvester
        item_harvester.run(tbl, input_csv)

        # get the harvested collection page data
        collection_page_data = item_harvester.getData(tbl)

        # Extract item urls to harvest from collection_page_data
        item_urls = self.getItemUrls(collection_page_data)

        # Write the item urls to a CSV file
        self.writeUrlsToCsv(output_csv, item_urls)

        print('%s item URLs written to %s' % (len(item_urls), output_csv))

        item_harvester.cleanup()


def main(args):
    if len(args) != 5:
        print("Not enough arguments passed. Expecting: input data CSV [page URLs], output CSV [item URLs], "
              "database path, table name")
        sys.exit(-1)

    input_csv = args[1]
    print("Input CSV: %s" % input_csv)

    output_csv = args[2]
    print("Output CSV: %s" % output_csv)

    db = args[3]
    print("DB: %s" % db)

    tbl = args[4]
    print("Table name: %s" % db)

    lc = LcCollectionHarvester()

    lc.harvestCollections(db, tbl, input_csv, output_csv)


if __name__ == '__main__':
    main(sys.argv)
