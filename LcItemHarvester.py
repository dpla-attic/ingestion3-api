import sys

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


class LcItemHarvester:
    sitemap_ns = 'http://www.sitemaps.org/schemas/sitemap/0.9'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/35.0.1916.47 Safari/537.36 '

    def harvestItems(self, db, tbl, input_csv):
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
        item_data = item_harvester.getData(tbl)

        print('%s item URLs harvested' % (len(item_data)))

        item_harvester.cleanup()


def main(args):
    if len(args) != 4:
        print("Not enough arguments passed. Expecting: CSV file with URLs, database, table name")
        sys.exit(-1)

    # CSV input file
    input_csv = args[1]
    print("Source data csv: %s" % input_csv)

    db = args[2]
    print("DB: %s" % db)

    tbl = args[3]
    print("Table: %s" % db)

    lc = LcItemHarvester()

    lc.harvestItems(db, tbl, input_csv)


if __name__ == '__main__':
    main(sys.argv)
