from s3tracking import settings
#from datetime import datetime, timezone, date
import datetime
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker

from helpers import send_mail, load_tables, gen_html, htmldos, compare_score, track_alert

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
        tracking = sesh.query(spider.Leaver).filter_by(result='Tracking').all()
        today = datetime.date.today()
        checked = []
        changed = []
        for t in tracking:
            print('>>>>>>>', t.name)
            timestamp = t.lasttracked.date()
            if timestamp == today:
                checked.append(t)

            role_score = compare_score(t.leaverrole, t.trackrole)
            if role_score == 'No Data':
                print('Role Has no Data to Compare')
            elif role_score == 'Update':
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Change Detected: %s and %s' %(t.leaverrole,t.trackrole))
                changed.append(track_alert(t, sesh))
            elif role_score < 50:
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Change Detected: %s and %s' %(t.leaverrole,t.trackrole))
                changed.append(track_alert(t, sesh))
            else:
                print('No Change to Role')

            firm_score = compare_score(t.leaverfirm, t.trackfirm)
            if firm_score == 'No Data':
                print('Firm Has no Data to Compare')
            elif firm_score == 'Update':
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Change Detected: %s and %s' %(t.leaverfirm,t.trackfirm))
                changed.append(track_alert(t, sesh))
            elif firm_score < 50:
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Change Detected: %s and %s' %(t.leaverfirm,t.trackfirm))
                changed.append(track_alert(t, sesh))
            else:
                print('>>No Change to Firm')

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
