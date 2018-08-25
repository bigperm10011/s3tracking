from scrapy.crawler import CrawlerProcess
from s3tracking.spiders import tracking
from scrapy.utils.project import get_project_settings
#from top_post_emailer import email_last_scraped_date

if __name__ == '__main__':
    #logging.basicConfig(level=logging.INFO)
    #logger = logging.getLogger(__name__)

    # only run on saturdays (once a week)


    settings = get_project_settings()
    crawler = CrawlerProcess(settings)

    crawler.crawl('tracking')
    crawler.start() # the script will block here until the crawling is finished
