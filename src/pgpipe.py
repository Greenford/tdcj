import sys
from pymongo import MongoClient
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2 as pg2


def run_pipe(commit_count=1000):
    client = MongoClient('localhost', 27017)
    inmates = client.tdcj.inmates
    unassigned = client.tdcj.unassigned

    results = inmates.find({'_id': {'$exists':'true'}})
    conn = pg2.connect(dbname='tdcj', host='localhost', port=5435, user='postgres')
    cur = conn.cursor()

    count = 0
    for inmate in result:
        insert_offender(cur, inmate)
        count += 1
        if count % commit_count == 0:
            conn.commit()
            print(f'{count} documents cleared pipe')

    conn.commit()
    cur.close()
    conn.close()


def _reset_tdcj_pgdb():
    conn = pg2.connect(host='localhost', port=5435, user='postgres')
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute('DROP DATABASE IF EXISTS tdcj')
    cur.execute('CREATE DATABASE tdcj')

    cur.close()
    conn.close()


def _create_tables():
    conn = pg2.connect(dbname='tdcj', host='localhost', port=5435, user='postgres')
    cur = conn.cursor()

    commands = (
        '''
        CREATE TABLE offenders (
            sid_number INTEGER UNIQUE,
            tdcj_number INTEGER PRIMARY KEY,
            name VARCHAR(30) NOT NULL,
            race CHAR(1) NOT NULL,
            gender BOOLEAN NOT NULL,
            date_of_birth DATE NOT NULL,
            max_sentence_date DATE,
            msd_category SMALLINT,
            current_facility VARCHAR(30) NOT NULL,
            projected_release_date DATE,
            parole_eligibility_date DATE,
            visitation_eligible VARCHAR(4),
            last_accessed TIMESTAMP NOT NULL
        )
        ''',
        '''
        CREATE TABLE offenses (
            tdcj_number INTEGER,
            offense_number SERIAL,
            offense_date DATE NOT NULL,
            offense VARCHAR(32) NOT NULL,
            sentence_date DATE NOT NULL,
            county VARCHAR(13) NOT NULL,
            case_number VARCHAR(18),
            sentence INTERVAL NOT NULL,
            PRIMARY KEY (tdcj_number, offense_number),
            FOREIGN KEY (tdcj_number)
                REFERENCES offenders (tdcj_number)
        )
        '''
    )

    for c in commands:
        cur.execute(c)

    conn.commit()
    cur.close()
    conn.close()

def prep_offender_data(entry):
    offense_dict = entry.pop('offensetable')
    tdcj_num = entry['_id']
    entry['Maximum Sentence Date'], entry['MSD_cat'] = split_msd_cat(\
        entry['Maximum Sentence Date'])
    if abs(entry['MSD_cat']) > 1:
        entry['Projected Release Date'] = None
        if abs(entry['MSD_cat']) > 2:
            entry['Parole Eligibility Date'] = None
    if entry['Parole Eligibility Date'] == 'NOT AVAILABLE':
        entry['Parole Eligibility Date'] = None
    entry['Gender'] = entry['Gender'] == 'F'

    offenses = [\
        {k:offensedict[k][i] for k in offensedict.keys()}\
        for i in offensedict['Offense'].keys()\
    ]
    for offense in offenses:
        offense['tdcj_num'] = tdcj_num
        offense['Sentence'] = sentence_str_to_timedelta(\
            offense.pop('Sentence (YY-MM-DD)'))

    return entry, offenses

def insert_offender(cur, entry):
    offender_info, offenses = prep_offender_data(entry)
    try:
        cur.execute(
            """
            INSERT INTO offenders (
                sid_number, 
                tdcj_number, 
                name, 
                race,
                gender,
                date_of_birth,
                max_sentence_date,
                msd_category,
                current_facility, 
                projected_release_date,
                parole_eligibility_date,
                visitation_eligible, 
                last_accessed        
            )
            VALUES (
                %(SID Number)s, 
                %(_id)s, 
                %(Name)s, 
                %(Race)s,
                %(Gender)s,
                %(DOB)s,
                %(Maximum Sentence Date)s,
                %(MSD_cat)s,
                %(Current Facility)s,
                %(Projected Release Date)s,
                %(Parole Eligibility Date)s,
                %(Offender Visitation Eligible)s,
                %(accessed)s
            );
            """,offender_info)
    except Exception as e:
        raise type(e)(f'{str(e)} tdcj_num={tdcj_num}')\
            .with_traceback(sys.exc_info()[2])

    insert_offenses(cur, offenses)


def insert_offenses(cur, offenses):
    for offense in offenses:
        cur.execute(
            """
            INSERT INTO offenses (
                tdcj_number,
                offense_date,
                offense,
                sentence_date,
                county,
                case_number,
                sentence      
            )
            VALUES (
                %(tdcj_num)s, 
                %(Offense Date)s, 
                %(Offense)s, 
                %(Sentence Date)s,
                %(County)s,
                %(Case No)s,
                %(Sentence)s
            );
            """, offense)


def split_msd_cat(msd):
    mode = 1
    if msd.endswith('CUMULATIVE OFFENSES'):
        cumulative = -1
        msd = msd[:-19].strip()
    if msd == 'LIFE SENTENCE':
        return (None, 2*mode)
    elif msd == 'LIFE WITHOUT PAROLE':
        return (None, 3*mode)
    elif msd == 'NOT AVAILABLE':
        return (None, 4*mode)
    elif msd == 'DEATH ROW':
        return (None, 5*mode)
    else: 
        try:
            return (datetime.strptime(msd, '%Y-%m-%d'), mode)
            
        except ValueError as e:
            raise type(e)(f'{str(e)} value: msd={msd}')\
                .with_traceback(sys.exc_info()[2])
        

def sentence_str_to_timedelta(string):
    if string.endswith('Days'):
        return timedelta(days=int(string[:-4]))
    vals = [int(i) for i in string.split('-')]
    return timedelta(weeks=vals[0]*52+vals[0]*4, days=vals[2])

         
