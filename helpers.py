from s3tracking.settings import get_con_string
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.ext.automap import automap_base
import sendgrid
import os
from sendgrid.helpers.mail import *
# ******* Track Upgrade Imports ************
from fuzzywuzzy import fuzz
from geotext import GeoText
import re
import html


def send_mail(body):
    sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
    from_email = Email("jdbuhrman@gmail.com")
    subject = "Updated Tracking infromation from SAR"
    to_email = Email("jbuhrman2@bloomberg.net")
    content = Content("text/html", body)
    mail = Mail(from_email, subject, to_email, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    return response.status_code

#check
def load_tables():
    """"""
    cs = get_con_string()
    # automap base
    Base = automap_base()

    # pre-declare User for the 'user' table
    class Leaver(Base):
        __tablename__ = 'leaver'

    # reflect
    engine = create_engine(cs)
    Base.prepare(engine, reflect=True)

    # we still have Address generated from the tablename "address",
    # but User is the same as Base.classes.User now

    Suspect = Base.classes.suspect
    Session = sessionmaker(bind=engine)
    session = Session()
    return session, Suspect, Leaver


def remove_html_markup(s):
    tag = False
    quote = False
    out = ""

    for c in s:
            if c == '<' and not quote:
                tag = True
            elif c == '>' and not quote:
                tag = False
            elif (c == '"' or c == "'") and tag:
                quote = not quote
            elif not tag:
                out = out + c

    return out

def htmldos(changed):
    html2 = """\
        <!DOCTYPE html><html lang="en"><head>SAR Leaver Found! </head><body><table border='1'>
        <thead><tr><th>Name</th><th>Old Firm</th><th>New Firm</th><th>Old Role</th><th>New Role</th><th>Location</th><th>LinkedIn</th></tr></thead>"""

    for c in changed:
        html2 = html2 + "<tr>"
        html2 = html2 + "<td>" + c.name + "</td>"
        try:
            html2 = html2 + "<td>" + c.lfirm + "</td>"
        except:
            html2 = html2 + "<td>None</td>"
        try:
            html2 = html2 + "<td>" + c.track_firm + "</td>"
        except:
            html2 = html2 + "<td>None</td>"
        try:
            html2 = html2 + "<td>" + c.lrole + "</td>"
        except:
            html2 = html2 + "<td>None</td>"
        try:
            html2 = html2 + "<td>" + c.track_role + "</td>"
        except:
            html2 = html2 + "<td>None</td>"
        try:
            html2 = html2 + "<td>" + c.llocation + "</td>"
        except:
            html2 = html2 + "<td>None</td>"
        try:
            html2 = html2 + '<td><a target="_blank" href="'+ c.llink + ' ">LinkedIn</a></td></tr>'
        except:
            html2 = html2 + '<td><a target="_blank" href=None">None</a></td></tr>'

    html2 = html2 + "</table></body></html>"
    return html2

def gen_html(checked):
    html = """\
        <!DOCTYPE html><html lang="en"><head>SAR Leavers Checked Today</head><body><table border='1'>
        <thead><tr><th>Name</th><th>Firm</th><th>Role</th><th>Location</th><th>Link</th></tr></thead>"""

    for l in checked:
        html = html + "<tr>"
        html = html + "<td>" + l.name + "</td>"
        try:
            html = html + "<td>" + l.track_firm + "</td>"
        except:
            html = html + "<td>None</td>"
        try:
            html = html + "<td>" + l.track_role + "</td>"
        except:
            html = html + "<td>None</td>"
        try:
            html = html + "<td>" + l.track_location + "</td>"
        except:
            html = html + "<td>None</td>"
        try:
            html = html + '<td><a target="_blank" href="'+ l.llink + ' ">LinkedIn</a></td></tr>'
        except:
            html = html + '<td><a target="_blank" href=None">None</a></td></tr>'
    html = html + "</table></body></html>"
    return html

# *************** Track Upgrade Helpers ******************



def clean_string(string, name):
    print('******Clean String Started*******')
    first = name.split(' ')[0].lower()
    name = name.lower()
    name_w_s = name + 's'
    print('state of string before filtering: ', string)
    view_stmt = 'view ' + name + 's full profile'
    view1_stmt = 'view ' + name + 's profile'
    view2_stmt = 'view ' + name + 's professional profile on linkedin'
    generic_stmt = 'on linkedin the worlds largest professional community'
    dif_stmt = 'find a different ' + name
    join_stmt = 'join linkedin to see ' + first + 's skills endorsements and full profile'
    find_stmt = 'find a different ' + name
    complete_stmt = 'see the complete profile on linkedin and'
    dsc2_stmt = 'discover ' + first +  's connections and jobs at similar companies'
    dsc3_stmt = 'discover ' + first +  's connections and jobs at similar'
    partial_stmt = 'see the complete profile on'
    lang_stmt = 'view this profile in another language'
    lang2_stmt = 'view this profile in another  language'
    slang_stmt = 'languages'
    lnkd_stmt = 'linkedin is the worlds largest business network'
    lnkd2_stmt = 'linkedin and'
    lnkd3_stmt = 'linkedin'
    dsc_stmt = 'discover inside'
    help_stmt = 'helping professionals like ' + name
    conn_stmt = 'connections to recommended job candidates'
    activity_stmt = name + 's activity'
    world_stmt = 'the worlds largest professional'
    name_string = name
    on_stmt = 'on their profile'
    trash_list = [world_stmt, activity_stmt, dsc3_stmt, dsc2_stmt, lang2_stmt, conn_stmt, view_stmt, view1_stmt, view2_stmt, lnkd_stmt, dif_stmt, help_stmt, dsc_stmt, join_stmt, find_stmt, complete_stmt, partial_stmt, generic_stmt, lang_stmt, slang_stmt, name_w_s, name_string, lnkd3_stmt, lnkd2_stmt, on_stmt]

    for i in trash_list:
        if i in string:
            string = string.replace(i, '')
            print('Found and Removed: ', i)
    print('Salvaged Text FINAL: ', string)
    print('******Clean String Closing*******')
    return string

def score_name(rez_name, db_name):
    print('***Starting Score_Name***')
    ratio = fuzz.ratio(rez_name.lower(), db_name.lower())
    print('Names:')
    print(rez_name, db_name)
    print('ratio is: ', ratio)
    print('*** Score_Name Complete ***')
    return ratio

def find_city(text):
    print('**Find City Started**')
    print('text delivered to find_city: ', text)
    places = GeoText(text)
    print('places result: ', places)
    city = places.cities
    print('**City Found: ', city)
    print('**Closing find_city**')
    try:
        return city
    except:
        return None

def list2string(strung):
    print('****** list2string STARTING *****')
    help_list = []
    for s in strung:
        s = str(s)
        s = re.sub(r'[^a-zA-Z0-9\s]', '', s)
        s = s.strip().strip('[').strip(']')
        s = s.replace('\xa0', '')
        s = s.replace('\n', '')
        s = s.lower()
        help_list.append(s)
    lststring = ", ".join(help_list)
    out = re.sub(r'[^a-zA-Z0-9\s]', '', lststring)
    print('final version of string: ', out)

    print('****** list2string COMPLETE *****')
    return out

def zone1(h3):
    print('Content as delivered: ', h3)
    lnkd1 = ' | Professional Profile - LinkedIn'
    lnkd2 = ' | Berufsprofil - LinkedIn'
    lnkd3 = ' | LinkedIn'
    lnkd4 = ' - LinkedIn'
    print('**** Processing Zone 1...')

    regex = re.sub('<[^>]*>', '', str(h3))
    print('regex Phase 1: ', regex)

    regex = regex.strip('[').strip(']').strip("'")
    print('regex Phase 2: ', regex)

    #regex = regex.strip(lnkd1).strip(lnkd2).strip(lnkd3)
    if lnkd1 in regex:
        print(lnkd1)
        regex = regex.replace(lnkd1, '')
    elif lnkd2 in regex:
        print(lnkd2)
        regex = regex.replace(lnkd2, '')
    elif lnkd3 in regex:
        print(lnkd3)
        regex = regex.replace(lnkd3, '')
    elif lnkd4 in regex:
        print(lnkd4)
        regex = regex.replace(lnkd4, '')
    print('regex Phase 3: ', regex)

    regex = html.unescape(regex)
    print('regex Phase 4: ', regex)

    regex = regex.strip('.')
    print('regex Phase 5: ', regex)

    dsh_list = regex.split('-')
    print('dash list: ', dsh_list)

    if len(dsh_list) == 3:
        name = dsh_list[0].strip()
        role = dsh_list[1].strip()
        firm = dsh_list[2].strip()
        print('Zone1 Results: Name/Role/Firm Found')
        print(name)
        print(role)
        print(firm)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    elif len(dsh_list) == 4:
        name = dsh_list[0].strip()
        role = dsh_list[1] + ' ' + dsh_list[2].strip()
        firm = dsh_list[3].strip()
        print('Zone1 Results: Name/Role+Role/Firm Found')
        print(name)
        print(role)
        print(firm)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    elif len(dsh_list) == 1:
        name = dsh_list[0].strip()
        role = None
        firm = None
        print('Zone1 Results: name found')
        print(name)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    elif len(dsh_list) == 2:
        name = dsh_list[0].strip()
        role = dsh_list[1].strip()
        firm = None
        print('Zone1 Results: name/role found')
        print(name)
        print(role)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    else:
        name = None
        role = None
        firm = None
        print('Zone1 Results: Nothing Found')
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm

def zone2(raw_lnk):
    print('*** Starting Zone2 Analysis...')
    print('Content As Delivered: ', raw_lnk)
    clean_link = remove_html_markup(raw_lnk).strip('[').strip(']').strip("\'")
    clean_link = clean_link.strip()
    print('Zone2 Cleaned Link: ', clean_link)
    print('*** Zone2 Analysis Complete ***')
    return clean_link

def zone3a(slp):
    print('*** Starting Zone3 Analysis ***')
    print('Content as Delivered: ', slp)
    slp = slp.strip('[').strip(']').strip("'")
    print('stripped brackets: ', slp)
    slp = slp.replace(u'\\xa0', u' ')
    print('Replaced \\xa0: ', slp)
    slp_lst = slp.split('-')
    print('Split Content into List: ', slp_lst)

    if len(slp_lst) == 2:
        print('slp_list is 2')
        cty = slp_lst[0].strip()
        print('possible city found. checking: ', cty)
        cty_result = find_city(cty)
        print('length of city_find result: ', len(cty_result))
        if cty_result:
            print('city verified: ', cty_result)
            city = cty_result
            role = None
            firm = slp_lst[1].strip()
            print('city/firm found')
            print(city)
            print(firm)
            print('*** Zone3a Analysis Complete ***')
            return city, role, firm

        else:
            city = cty.strip()
            role = None
            firm = slp_lst[1].strip()
            print('city/firm found')
            print(city)
            print(firm)
            print('*** Zone3a Analysis Complete ***')
            return city, role, firm

    elif len(slp_lst) == 3:
        print('slp_list is 3')
        cty = slp_lst[0].strip()
        print('possible city found. checking: ', cty)
        cty_result = find_city(cty)
        if cty_result:
            print('city verified: ', cty_result)
            city = cty_result[0]
            role = slp_lst[1].strip()
            firm = slp_lst[2].strip()
            print('city/role/firm found')
            print(city)
            print(role)
            print(firm)
            print('*** Zone3a Analysis Complete ***')
            return city, role, firm

        else:
            city = cty.strip()
            role = slp_lst[1].strip()
            firm = slp_lst[2].strip()
            print('city/role/firm found')
            print(city)
            print(role)
            print(firm)
            print('*** Zone3a Analysis Complete ***')
            return city, role, firm

    elif len(slp_lst) == 4:
        print('slp_list is 4')
        cty = slp_lst[0].strip()
        print('possible city found. checking: ', cty)
        cty_result = find_city(cty)
        if cty_result:
            print('city verified: ', cty_result)
            city = cty_result[0]
            role = slp_lst[1].strip() + ' ' + slp_lst[2].strip()
            firm = slp_lst[3].strip()
            print('city/role/firm found')
            print(city)
            print(role)
            print(firm)
            print('*** Zone3a Analysis Complete ***')
            return city, role, firm
        else:
            print('Warning: City Not Found: ', cty_result)
            city = cty.strip()
            role = slp_lst[1].strip() + ' ' + slp_lst[2].strip()
            firm = slp_lst[3].strip()
            print('city/role/firm found')
            print(city)
            print(role)
            print(firm)
            print('*** Zone3a Analysis Complete ***')
            return city, role, firm


    else:
        print('slp_list is None')
        print('No Usable Information Found')
        print('*** Zone3a Analysis Complete ***')
        city = None
        role = None
        firm = None
        return city, role, firm
