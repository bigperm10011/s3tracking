import scrapy
import re
import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table, desc
from sqlalchemy.engine.url import URL
import sqlalchemy
from s3tracking import settings
from sqlalchemy.orm import mapper, sessionmaker
from s3tracking.items import S3TrackingItem
from helpers import load_tables, remove_html_markup, clean_string, score_name, find_city, list2string, zone1, zone2, zone3a
#################### Spider Description ####################
#grabs 5 leavers sorted by the last time they were scraped
#uses their linkedin profile link as a google search term
#scrapes relevant details
############################################################
class QuotesSpider(scrapy.Spider):
    name = "tracking"
    sesh, Suspect, Leaver = load_tables()
    fresh_lvr = sesh.query(Leaver).filter_by(result='Tracking', inprosshell='Yes', lasttracked=None).limit(5).all()
    lvr = sesh.query(Leaver).filter_by(result='Tracking').order_by(Leaver.lasttracked).limit(5).all()
    print('------> Number of First Time Tracks: ', len(fresh_lvr))
    print('------> Number of Re-Tracks: ', len(lvr))

    def start_requests(self, sesh=sesh, Leaver=Leaver, lvr=lvr, fresh_lvr=fresh_lvr):
        print('*********** Leavers To Be Tracked **********')
        if len(fresh_lvr) > 0:
            for l in fresh_lvr:
                print(l.name)
                lid = l.id
                try:
                    old_firm_full = l.prosfirm
                    old_firm_list = old_firm_full.split()
                    oldfirm = old_firm_list[0]
                    string = str('https://www.google.com/search?q=' + l.name + ' ' + oldfirm + ' ' + 'site:www.linkedin.com/in/')
                    url = string
                    l.lasttracked = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    sesh.commit()

                    yield scrapy.Request(url=url, callback=self.parse, meta={'lid': l.id, 'name': l.name, 'truelink': l.link})
                except:
                    string = str('https://www.google.com/search?q=' + l.name + ' ' + 'site:www.linkedin.com/in/')
                    url = string
                    l.lasttracked = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    sesh.commit()

                    yield scrapy.Request(url=url, callback=self.parse, meta={'lid': l.id, 'name': l.name, 'truelink': l.link})
        else:
            for l in lvr:
                print(l.name)
                lid = l.id
                try:
                    old_firm_full = l.prosfirm
                    old_firm_list = old_firm_full.split()
                    oldfirm = old_firm_list[0]
                    string = str('https://www.google.com/search?q=' + l.name + ' ' + oldfirm + ' ' + 'site:www.linkedin.com/in/')
                    url = string
                    l.lasttracked = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    sesh.commit()

                    yield scrapy.Request(url=url, callback=self.parse, meta={'lid': l.id, 'name': l.name, 'truelink': l.link})
                except:
                    string = str('https://www.google.com/search?q=' + l.name + ' ' + 'site:www.linkedin.com/in/')
                    url = string
                    l.lasttracked = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    sesh.commit()

                    yield scrapy.Request(url=url, callback=self.parse, meta={'lid': l.id, 'name': l.name, 'truelink': l.link})

    def parse(self, response):
        db_name = response.meta['name']
        truelink = response.meta['truelink']
        print('***')
        print('***')
        print('***')
        print('Parsing: ', db_name)
        for i in response.xpath("//div[@class='g']"):
            raw_lnk = str(i.xpath(".//cite").extract())
            clink = zone2(raw_lnk)
            if 'https://www.linkedin.com/in/' in clink and clink == truelink:
                print('Links Matched. Proceeding...')
                print('DB Link: ', truelink)
                print('Scraped Link: ', clink)
                h3a = i.xpath(".//h3/a").extract()
                name, role1, firm1 = zone1(h3a)

                name_test = score_name(name, db_name)
                if name_test > 80:
                    print('Passing Sore: ', name_test)
                    slp_xtract = i.xpath(".//div[contains(@class, 'slp')]/descendant::text()").extract()
                    print('Raw SLP Xtract: ', slp_xtract)
                    print('LENGTH of SLP Xtract: ', len(slp_xtract))

                    if len(slp_xtract) > 0:
                        txt = str(slp_xtract)
                        print('length of slp: ', len(txt))
                        print('slp class detected. Running Zone3a Analysis...')
                        city, role, firm = zone3a(txt)
                        print('results from zone3a analysis: ')
                        item = S3TrackingItem()
                        item['name'] = name
                        item['link'] = clink
                        item['ident'] = response.meta['lid']
                        item['location'] = city
                        if role1 == None:
                            item['role'] = role
                        else:
                            item['role'] = role1
                        if firm1 == None:
                            item['firm'] = firm
                        else:
                            item['firm'] = firm1

                        yield item

                    else:
                        print('no slp class found.  salvaging text')
                        st_class = i.xpath(".//span[contains(@class, 'st')]/descendant::text()").extract()
                        print('ST Text Extracted: ', st_class)
                        salvage_string = list2string(st_class)
                        cleaned_str = clean_string(salvage_string, name)
                        item = S3TrackingItem()
                        item['name'] = name
                        item['link'] = clink
                        item['location'] = None
                        item['ident'] = response.meta['lid']
                        if role1 == None:
                            item['role'] = None
                        else:
                            item['role'] = role1
                        if firm1 == None:
                            salvage_text = cleaned_str.strip()
                            print('length of salvaged text: ', len(salvage_text))
                            if len(salvage_text) < 100:
                                item['firm'] = salvage_text
                            else:
                                try:
                                    item['firm'] = salvage_text[:98]
                                except:
                                    item['firm'] = None
                        else:
                            item['firm'] = firm1
                        yield item

                else:
                    print('Failing Score: ', name_test)
                    yield None
            else:
                print("Links Don't Match: ")
                print("DB Link: ", truelink)
                print('Scraped Link: ', clink)
                yield None
