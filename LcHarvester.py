import sys

from ItemHarvester import ItemHarvester
from LcCollectionHarvester import LcCollectionHarvester
from LcItemHarvester import LcItemHarvester
from LcSitemapHarvester import LcSitemapHarvester

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


def main(args):
    if len(args) != 4:
        print("Not enough arguments passed, expecting: root path for CSV files, path to database, collections [comma "
              "separated]")
        sys.exit(-1)

    base_dir = args[1]
    print("Root directory to save CSV files: %s" % base_dir)

    db = args[2]
    print("DB: %s" % db)

    collections = args[3].split(',')
    print("Collections to harvest: %s" % collections)

    # table names
    sitemap_tbl = 'sitemap'
    collection_tbl = 'collection'
    item_tbl = 'item'

    # CSV output files
    sitemap_csv = base_dir + "/csv/lc-sitemaps.csv"
    collection_page_csv = base_dir + "/csv/lc-pages.csv"
    items_csv = base_dir + "/csv/lc-items.csv"

    item_harvester = ItemHarvester(db)

    lc_sitemap_harvester = LcSitemapHarvester()
    lc_collection_harvester = LcCollectionHarvester()
    lc_item_harvester = LcItemHarvester()

    """ SITEMAP """
    lc_sitemap_harvester.harvestSitemaps(db=db, tbl=sitemap_tbl, collections=collections, input=sitemap_csv, output=collection_page_csv)

    """ COLLECTION PAGES """
    lc_collection_harvester.harvestCollections(db=db, tbl=collection_tbl, input_csv=collection_page_csv, output_csv=items_csv)

    """ ITEM PAGES """
    lc_item_harvester.harvestItems(db=db, tbl=item_tbl, input_csv=items_csv)

    # TODO Write item JSON out to individual files
    item_data = item_harvester.getData(item_tbl)

    print("Harvested %s item records" % len(item_data))

    item_harvester.cleanup()


if __name__ == '__main__':
    main(sys.argv)
