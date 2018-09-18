from s3tracking import settings
#from datetime import datetime, timezone, date
import datetime
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker

from helpers import send_mail, load_tables, gen_html, htmldos

class S3TrackingPipeline(object):
    def process_item(self, item, spider):
        print('***** Pipeline Processing Started ******')
        sesh = spider.sesh
        lvr = sesh.query(spider.Leaver).filter_by(id=item['ident']).one()
        print('Matching Links....')
        print('Link on File: ', lvr.link)
        print('Link Found: ', item['link'])
        if lvr.link == item['link']:
            print('Match!!!')
            lvr.trackfirm = item['firm']
            lvr.tracklocation = item['location']
            lvr.trackrole = item['role']
            try:
                sesh.commit()
                print('DB Updated With the following: ')
                print('For: ', lvr.name)
                print('Tracking Firm: ', lvr.trackfirm)
                print('Tracking Location: ', lvr.tracklocation)
                print('Tracking Role: ', lvr.trackrole)
                print('Tracking TimeStamp: ', lvr.lasttracked)
                print('Result Saved. DB Updated')
                print('.')
                print('.')
                print('.')
                print('.')
                print('.')
            except IntegrityError:
                print('except....', item['name'])
            print('***** Pipeline Processing Complete ******')
            return item
        else:
            print("Link Doesn't Match!")
            return None

    def close_spider(self, spider):
        sesh = spider.sesh
        lvrs = sesh.query(spider.Leaver).filter_by(inprosshell='Yes').all()
        today = datetime.date.today()
        checked = []
        changed = []
        for l in lvrs:
            timestamp = l.lasttracked
            try:
                date = timestamp.date()
                if date == today:
                    checked.append(l)
                    if l.lasttracked != None and l.leaverrole != l.trackrole:
                        l.result = 'TrackAlert'
                        l.datetimeresult = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        l.trackend = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        changed.append(l)
                        sesh.commit()
                        print('!!! Role Change !!!', l.name)
                    elif l.lasttracked != None and l.leaverfirm != l.trackfirm:
                        l.result = 'TrackAlert'
                        l.datetimeresult = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        l.trackend = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        changed.append(l)
                        sesh.commit()
                        print('!!! Firm Change !!!', l.name)
            except:
                pass

        try:
            if len(changed) > 0:
                html2 = htmldos(changed)
                resp_code2 = send_mail(html2)
                print(resp_code2)
            if len(checked) > 0:
                html = gen_html(checked)
                resp_code = send_mail(html)
                print(resp_code)
        except:
            print('Emails Not Sent')
