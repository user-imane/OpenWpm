import sqlite3
import pandas as pd
import os
import sys
from tld import get_tld
from urlparse import urlparse, parse_qs
import re
from timeit import default_timer as timer

output_dir = sys.argv[1]
data_dir = sys.argv[2]
nbr_db = sys.argv[3]

db = os.path.join(data_dir, 'crawl-data.sqlite')
conn = sqlite3.connect(db)
cur = conn.cursor()
urls_tp_file = os.path.join(output_dir, 'urls_tp_' + nbr_db)
referrer_matching = open(os.path.join(output_dir, 'matching1db/referrer_matching_' + nbr_db), 'a')
referrer_results = open(os.path.join(output_dir, 'matching1db/referrer_results_' + nbr_db), 'a')
request_matching = open(os.path.join(output_dir, 'matching1db/request_matching_' + nbr_db), 'a')
request_results = open(os.path.join(output_dir, 'matching1db/request_results_' + nbr_db), 'a')
domains = os.path.join(output_dir, 'matching1db/domains_ref_req_' + nbr_db)

PERFECT_MATCH_REF = 0
VALUE_MATCH_REF = 0
VALUE_AS_KEY_REF = 0
CONCATENATE_REF = 0
PERFECT_MATCH_REQ = 0
VALUE_MATCH_REQ = 0
VALUE_AS_KEY_REQ = 0
CONCATENATE_REQ = 0
NB_COOKIE_SYN_REF = 0
NB_COOKIE_SYN_REQ = 0
NB_TP_REQ = 0
false_postif = ['homepage', 'eur' 'false', 'true', 'yes', 'null']
domain_sending_req = {}
domain_sending_ref = {}
domain_rec_req = {}
domain_rec_ref = {}
cookies_domain = {}


def tld(urls):
    top_domain = urlparse(urls).hostname
    top_domain = get_tld('http://' + top_domain)
    return top_domain


def matching(name_cookie, value_cookie, name_param, value_param):
    if name_param == name_cookie and value_param == value_cookie:
        return "perfect_matching"
    elif value_param == value_cookie:
        return "value_matching"

    else:
        return ""


def add_to_dict(attr, dictio):
    if attr not in dictio:
        dictio[attr] = 1
    else:
        dictio[attr] += 1


conn3 = sqlite3.connect(os.path.join(output_dir, 'matching_tables/matching' + nbr_db + '.sqlite'))

cur3 = conn3.cursor()

cur3.execute('CREATE TABLE IF NOT EXISTS ref_matching (site_id INTEGER NOT NULL, link_id INTEGER NOT NULL, '
             'resp_id INTEGER NOT NULL, send_ref TEXT, rec_ref TEXT, PRIMARY KEY(site_id, link_id, resp_id))')
cur3.execute('CREATE TABLE IF NOT EXISTS req_matching (site_id INTEGER NOT NULL, link_id INTEGER NOT NULL, '
             'resp_id INTEGER NOT NULL, send_req TEXT, rec_req TEXT, PRIMARY KEY(site_id, link_id, resp_id))')
query = 'SELECT name, value, site_id, raw_host, link_id from cookies group by name, value '
df = pd.read_sql_query(query, conn)
with open(urls_tp_file, 'r') as tp_urls:
    for line in tp_urls:
        line = line.rstrip()
        line_pixel = line.split(" pixel:")
        line = str(line_pixel[0])
        pixel = line_pixel[1]
        if str(pixel) == str(1):
            NB_TP_REQ += 1
            COOKIE_SYN_REF = False
            COOKIE_SYN_REQ = False
            PERFECT_MATCH_REF_BO = False
            VALUE_MATCH_REF_BO = False
            VALUE_AS_KEY_REF_BO = False
            CONCATENATE_REF_BO = False
            PERFECT_MATCH_REQ_BO = False
            VALUE_MATCH_REQ_BO = False
            VALUE_AS_KEY_REQ_BO = False
            CONCATENATE_REQ_BO = False
            values_synced_req = []
            values_synced_ref = []
            line_split = line.split(" ")
            site_id = line_split[0]
            link_id = line_split[1]
            resp_id = line_split[2]
            print site_id, link_id
            url = line_split[3]
            referrer = line_split[4]
            domain = tld(referrer)
            t1 = timer()

            cookies = df[
                (df['site_id'] == int(site_id)) & (df['raw_host'] == str(domain)) & (df['link_id'] < int(link_id) + 1)]
            t2 = timer()
            print "db  ", t2 - t1
            ref = urlparse(referrer)
            query = parse_qs(ref.query)
            req = urlparse(url)
            query2 = parse_qs(req.query)
            vv = {x for v in query.values() for x in v}
            cc = {x for v in query2.values() for x in v}
            t3 = timer()
            for ind, cookie in cookies.iterrows():
                try:
                    if len(str(cookie[1])) > 2 and (str(cookie[1])).lower() not in false_postif and str(
                            cookie[1]) in str(query) or str(cookie[1]) in str(query2):
                        if str(cookie[0]) in query and str(cookie[1]) in query[str(cookie[0])]:
                            PERFECT_MATCH_REF_BO = True
                            COOKIE_SYN_REF = True
                        elif str(cookie[1]) in vv:
                            VALUE_MATCH_REF_BO = True
                            COOKIE_SYN_REF = True

                        if str(cookie[0]) in query2 and str(cookie[1]) in query2[str(cookie[0])]:
                            PERFECT_MATCH_REQ_BO = True
                            COOKIE_SYN_REQ = True
                        elif str(cookie[1]) in cc:
                            VALUE_MATCH_REQ_BO = True
                            COOKIE_SYN_REQ = True


                except:
                    print "error"
            t4 = timer()
            print "matching  ", t4 - t3
            if PERFECT_MATCH_REF_BO:
                PERFECT_MATCH_REF += 1
            if VALUE_MATCH_REF_BO:
                VALUE_MATCH_REF += 1

            if PERFECT_MATCH_REQ_BO:
                PERFECT_MATCH_REQ += 1
            if VALUE_MATCH_REQ_BO:
                VALUE_MATCH_REQ += 1

            if COOKIE_SYN_REF is True:
                cur3.execute(
                    "insert into ref_matching (site_id , link_id , resp_id, send_ref , rec_ref) Values(?,?,?,?,?)",
                    (site_id, link_id, resp_id, tld(referrer), tld(url)))
                NB_COOKIE_SYN_REF += 1
                add_to_dict(tld(referrer), domain_sending_ref)
                add_to_dict(tld(url), domain_rec_ref)
                referrer_matching.write(tld(url) + ' ' + tld(referrer) + ' ' + pixel + "\n")
            if COOKIE_SYN_REQ is True:
                cur3.execute(
                    "insert into req_matching (site_id , link_id , resp_id, send_req , rec_req) Values(?,?,?,?,?)",
                    (site_id, link_id, resp_id, tld(referrer), tld(url)))
                NB_COOKIE_SYN_REQ += 1
                add_to_dict(tld(referrer), domain_sending_req)
                add_to_dict(tld(url), domain_rec_req)
                request_matching.write(tld(url) + ' ' + tld(referrer) + ' ' + pixel + "\n")
conn3.commit()
conn3.close()
referrer_results.write(
    "nbr_tp_req : " + str(NB_TP_REQ) + '\n' + "nbr_ref_sync : " + str(NB_COOKIE_SYN_REF) + '\n' + "perfect_matching : "
    + str(PERFECT_MATCH_REF) + '\n' + "value_matching :" + str(VALUE_MATCH_REF)
    + '\n'
    + "value_as_key :" + str(VALUE_AS_KEY_REF) + '\n' + "concatenate : " + str(CONCATENATE_REF))
request_results.write(
    "nbr_tp_req : " + str(NB_TP_REQ) + '\n' + "nbr_req_sync : " + str(NB_COOKIE_SYN_REQ) + '\n' + "perfect_matching : "
    + str(PERFECT_MATCH_REQ) + '\n' + "value_matching :" + str(VALUE_MATCH_REQ)
    + '\n'
    + "value_as_key :" + str(VALUE_AS_KEY_REQ) + '\n' + "concatenate : " + str(CONCATENATE_REQ))

with open(domains, 'w') as ff:
    ff.write("senders_ref = " + str(domain_sending_ref) + '\n')
    ff.write("rec_ref = " + str(domain_rec_ref) + '\n')
    ff.write("senders_req = " + str(domain_sending_req) + '\n')
    ff.write("rec_req = " + str(domain_rec_req) + '\n')
referrer_matching.close()
referrer_results.close()
request_matching.close()
referrer_results.close()
