import sys
import pandas as pd
import sqlite3
import os


db1=sys.argv[1]
print db1

conn = sqlite3.connect(db1)
curr = conn.cursor()
mixed = curr.execute('select rec from matching  where rec  in ( select rec from matching where cookies_req != "" or cookies_res != "") '
                     'and rec in (select rec from matching where cookies_req = "" and cookies_res = "") group by rec').fetchall()

#print mixed
print "mixed",len(mixed)
analytics = curr.execute('select rec from matching  where rec not in ( select rec from matching where cookies_req != "" or cookies_res != "") group by rec').fetchall()

print "analytics",len(analytics)
tracker = curr.execute('select rec from matching where rec not in ( select rec from matching where '
                       'cookies_req = "" and cookies_res = "") group by rec').fetchall()

print "tracker",len(tracker)
total = curr.execute('select rec from matching group by rec ').fetchall()
print "total", len(total)



curr.execute('Create table if not exists categorie (id INTEGER PRIMARY KEY, domain TEXT, categorie TEXT, '
             'color TEXT)')
for mixed in mixed:
    curr.execute('insert into categorie (domain , categorie, color) Values (?,?,?)', (mixed[0],"mixte","g"))
for analytics in analytics:
    curr.execute('insert into categorie (domain , categorie, color) Values (?,?,?)', (analytics[0],"analytics","r"))
for tracker in tracker:
    curr.execute('insert into categorie (domain , categorie, color) Values (?,?,?)', (tracker[0],"tracker","b"))
conn.commit()
conn.close()
