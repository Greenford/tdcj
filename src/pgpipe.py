import sys
from pymongo import MongoClient
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2 as pg2
from psycopg2.errors import UniqueViolation


def run_pipe(print_count=1000):
    """
    Converts a mongoDB open on localhost:27017 to a postgreSQL 
    DB open on localhost:5432.

    Args:
        print_count: prints progress after every number of this inserts

    Raises:
        any exception during insertion with extra information to identify the troublesome record
    """
    #initialize mongo connection
    client = MongoClient('localhost', 27017)
    inmates = client.tdcj.inmates
    unassigned = client.tdcj.unassigned

    #query mongo
    results = inmates.find({'_id': {'$exists':'true'}})

    #initalize postgres connection
    conn = pg2.connect(dbname='tdcj', host='localhost', port=5432, user='postgres')
    cur = conn.cursor()
    
    #insert every inmate into the postgres DB
    try:
        count = 0
        for inmate in results:
            insert_offender(conn, cur, inmate)
            count += 1
            if count % print_count == 0:
                print(f'{count} documents cleared pipe')
    except Exception as e:
        raise type(e)(f'{str(e)} problematic entry: {inmate}')\
            .with_traceback(sys.exc_info()[2])
    cur.close()
    conn.close()


def _reset_tdcj_pgdb():
    """
    Deletes and recreates the tdcj SQL database.
    """
    conn = pg2.connect(host='localhost', port=5432, user='postgres')
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute('DROP DATABASE IF EXISTS tdcj')
    cur.execute('CREATE DATABASE tdcj')

    cur.close()
    conn.close()


def _create_tables():
    """
    Creates the 3 tables for the postgres DB.
    """
    conn = pg2.connect(dbname='tdcj', host='localhost', port=5432, user='postgres')
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
            sentence INTEGER NOT NULL,
            PRIMARY KEY (tdcj_number, offense_number),
            FOREIGN KEY (tdcj_number)
                REFERENCES offenders (tdcj_number)
        )
        ''',
        '''
        CREATE TABLE offender_pipe_err (
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
        '''
    )

    for c in commands:
        cur.execute(c)

    conn.commit()
    cur.close()
    conn.close()

def prep_offender_data(entry):
    """
    Cleans data to insert into SQL database.

    Args:
        entry: 1 offender's info and offense history

    Returns: 
        tuple: (cleaned offender info dict, cleaned offense history array)
    """
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
    if entry['Projected Release Date'] == 'NOT AVAILABLE':
        entry['Projected Release Date'] = None
    entry['Gender'] = entry['Gender'] == 'F'

    offenses = [\
        {k:offense_dict[k][i] for k in offense_dict.keys()}\
        for i in offense_dict['Offense'].keys()\
    ]
    for offense in offenses:
        offense['tdcj_num'] = tdcj_num
        offense['Sentence'] = sentence_str_to_days_int(\
            offense.pop('Sentence (YY-MM-DD)'))

    return entry, offenses

def insert_offender(conn, cur, entry):
    """
    Cleans and inserts the information and offense history of a single offender.

    Args:
        conn: connection to the postgres DB
        cur: cursor for the postgres DB
        entry: entry to clean and insert

    Raises: 
        Any exception that occurs during insertion with extra info 
        to assist identifying the problematic record.
    """

    #clean data
    offender_info, offenses = prep_offender_data(entry)
    
    #query template
    command=\
        """
        INSERT INTO {} (
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
        """
    #insert a record
    try:
        cur.execute(command.format('offenders'), offender_info)
        conn.commit()

    #for the SID or TDCJ number, insert the record in an error table instead.
    except UniqueViolation:
        conn.rollback()
        cur.execute(command.format('offender_pipe_err'), offender_info)
        return

    #for a different exception, re-raise it with information about the problematic record.
    except Exception as e:
        raise type(e)(f'{str(e)} tdcj_num={offender_info["_id"]}')\
            .with_traceback(sys.exc_info()[2])

    #insert the offender's offense history
    insert_offenses(cur, offenses)


def insert_offenses(cur, offenses):
    """
    Inserts a list of dicts containing cleaned offense history into the SQL DB.

    Args:
        cur: cursor to the SQL DB
        offenses: a list of cleaned offense dicts
    """
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
    """
    Splits the date portion and codifies occasional accompanying text
    into a tuple

    Args:
        msd: maximum sentence date

    Raises:
        ValueError for unhandled value cases
    """
    mode = 1
    if msd.endswith('CUMULATIVE OFFENSES'):
        mode = -1
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
        

def sentence_str_to_days_int(string):
    """
    Turns two formats of sentence lengths into a timedelta object in order to 
    be cast as an INTERVAL type in the future.

    Args:
        string: string containing the length of the sentence in either
        'Y-M-D' or 'DDD Days' formats

    Returns:
        timedelta object
    """
    if string.endswith('Days'):
        return int(string[:-4])
    vals = [int(i) for i in string.split('-')]
    return vals[0]*365 + vals[1]*30 + vals[2]

        
if __name__ == '__main__':
    _reset_tdcj_pgdb()
    _create_tables()
    run_pipe()
