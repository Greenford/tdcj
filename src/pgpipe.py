import sys
from pymongo import MongoClient
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2 as pg2
from psycopg2.errors import UniqueViolation
PGPORT = 5432
#normally we wouldn't do this, but no one will have access to this DB
PG_PW='password'

OFFENDER_MAP_DICT = {
    '_id':'tdcj_num', 
    'Race':'race',
    'Gender':'gender',
    'birth_year':'birth_year',
    'Maximum Sentence Date':'max_sentence_date',
    'MSD_cat':'sentence_category',
    'Current Facility':'current_facility',
    'Projected Release Date':'projected_release_date',
    'Parole Eligibility Date':'parole_eligibility_date',
    'Offender Visitation Eligible':'visition_eligible',
    'released':'released',
    'valid_start':'valid_start',
    'valid_end':'valid_end',
    'period':'period'
}
INSERT_OFFENDER = (
    'INSERT INTO {} ('
        ', '.join(OFFENDER_MAP_DICT.values)
    ') VALUES ('
        ', '.join(list(map(lambda col: f'%({col})s', OFFENDER_MAP_DICT.values())))
    ');'
)
OFFENSE_MAP_DICT = {
    'tdcj_num':'tdcj_num', 
    'Offense Date':'offense_date', 
    'Offense':'offense', 
    'Sentence Date':'sentence_date',
    'County':'County',
    'Case No':'case_num',
    'Sentence':'sentence'
}
INSERT_OFFENSES = (
    'INSERT INTO offenses ('
        ', '.join(OFFENSE_MAP_DICT.values())
    ') VALUES ('
        ', '.join(list(map(lambda col: f'%({col})s', OFFENSE_MAP_DICT.values())))
    ');'
)
PII_MAP_DICT = {
    '_id':'tdcj_num',
    'SID Number':'sid_num',
    'DOB':'date_of_birth',
    'Name':'name'
}
INSERT_PII = (
    'INSERT INTO pii ('
        ', '.join(PII_MAP_DICT.values())
    ') VALUES ('
        ', '.join(list(map(lambda col: f'%({col})s', PII_MAP_DICT.values())))
    ');'
)
INSERT_UNASSIGNED = """
    INSERT INTO unmapped_tdcj_num (
        tdcj_num,
        valid_start,
        valid_end,
        period
    ) VALUES (
        %(tdcj_num)s,
        %(valid_start)s,
        %(valid_end)s,
        %(period)s
    );
"""

INSERT_REDIRECT = """
    INSERT INTO redirect_tdcj_num (
        from_tdcj,
        to_tdcj
    ) VALUES (
        %(from_tdcj)s,
        %(to_tdcj)s
    );
"""

def run_pipe(print_count=1000):
    """
    Converts a mongoDB open on localhost:27017 to a postgreSQL 
    DB open on localhost:PGPORT.

    Args:
        print_count: prints progress after every number of this inserts

    Raises:
        any exception during insertion with extra information to identify the troublesome record
    """
    # initialize mongo connection
    client = MongoClient("localhost", 27017)
    inmates = client.tdcj.inmates
    unassigned = client.tdcj.unassigned

    # query mongo
    results = inmates.find({"_id": {"$exists": "true"}})

    # initalize postgres connection
    conn = pg2.connect(dbname="tdcj", host="localhost", port=PGPORT, user="postgres", password=PG_PW)
    cur = conn.cursor()

    # insert every inmate into the postgres DB
    try:
        count = 0
        for inmate in results:
            insert_offender(conn, cur, inmate)
            count += 1
            if count % print_count == 0:
                print(f"{count} documents cleared pipe")
    except Exception as e:
        raise type(e)(f"{str(e)} problematic entry: {inmate}").with_traceback(
            sys.exc_info()[2]
        )
    cur.close()
    conn.close()


def _reset_tdcj_pgdb():
    """
    Deletes and recreates the tdcj SQL database.
    """
    conn = pg2.connect(host="localhost", port=PGPORT, user="postgres", password=PG_PW)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS tdcj")
    cur.execute("CREATE DATABASE tdcj")

    cur.close()
    conn.close()


def _create_tables():
    """
    Creates the tables for the postgres DB.
    """
    conn = pg2.connect(dbname="tdcj", host="localhost", port=PGPORT, user="postgres", password=PG_PW)
    cur = conn.cursor()

    commands = (
        """
        CREATE TABLE offender (
            tdcj_num INTEGER PRIMARY KEY,
            race CHAR(1) NOT NULL,
            gender BOOLEAN NOT NULL,
            birth_year SMALLINT NOT NULL,
            max_sentence_date DATE,
            sentence_category SMALLINT,
            current_facility VARCHAR(30) NOT NULL,
            projected_release_date DATE,
            parole_eligibility_date DATE,
            visitation_eligible VARCHAR(4),
            released BOOLEAN NOT NULL,
            valid_start DATE NOT NULL,
            valid_end DATE NOT NULL,
            period SMALLINT
        )
        """,
        """
        CREATE TABLE pii (
            tdcj_num INTEGER PRIMARY KEY,
            sid_num INTEGER UNIQUE,
            date_of_birth DATE NOT NULL,
            name VARCHAR(30) NOT NULL,

            FOREIGN KEY (tdcj_num) REFERENCES offender (tdcj_num)
        )
        """,
        """
        CREATE TABLE offender_change (
            tdcj_num INTEGER,
            change_num SERIAL,
            old_info JSONB NOT NULL,
            valid_start DATE NOT NULL,
            valid_end DATE,
            period_checked SMALLINT,

            PRIMARY KEY (tdcj_num, change_num),
            FOREIGN KEY (tdcj_num) REFERENCES offender (tdcj_num)
        )
        """,
        """
        CREATE TABLE offenses (
            tdcj_num INTEGER,
            offense_num SERIAL,
            offense_date DATE NOT NULL,
            offense VARCHAR(32) NOT NULL,
            sentence_date DATE NOT NULL,
            county VARCHAR(13) NOT NULL,
            case_num VARCHAR(18),
            sentence INTEGER NOT NULL,

            PRIMARY KEY (tdcj_num, offense_num),
            FOREIGN KEY (tdcj_num)
                REFERENCES offender (tdcj_num)
        )
        """,
        """
        CREATE TABLE redirect_tdcj_num (
            tdcj_num_old INTEGER PRIMARY KEY,
            tdcj_num_new INTEGER
        )
        """,
        """
        CREATE TABLE unmapped_tdcj_num (
            tdcj_num INTEGER PRIMARY KEY,
            valid_start DATE NOT NULL,
            valid_end DATE NOT NULL,
            period_checked SMALLINT
        )
        """,
        """
        CREATE TABLE offender_pipe_err (
            tdcj_num INTEGER PRIMARY KEY,
            race CHAR(1) NOT NULL,
            gender BOOLEAN NOT NULL,
            birth_year SMALLINT NOT NULL,
            max_sentence_date DATE,
            sentence_category SMALLINT,
            current_facility VARCHAR(30) NOT NULL,
            projected_release_date DATE,
            parole_eligibility_date DATE,
            visitation_eligible VARCHAR(4),
            released BOOLEAN NOT NULL,
            valid_start DATE NOT NULL,
            valid_end DATE NOT NULL,
            period_checked SMALLINT
        )
        """,
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
        tuple: (cleaned offender info dict, cleaned offense history array, pii dict)
    """
    #split maximum sentence date category
    entry["max_sentence_date"], entry["sentence_category"] = split_msd_cat(
        entry.pop("Maximum Sentence Date")
    )
    #adjust fields based on the MSD category
    if abs(entry["sentence_category"]) > 1:
        entry["Projected Release Date"] = None
        if abs(entry["sentence_category"]) > 2:
            entry["Parole Eligibility Date"] = None
    if entry["Parole Eligibility Date"] == "NOT AVAILABLE":
        entry["Parole Eligibility Date"] = None
    if entry["Projected Release Date"] == "NOT AVAILABLE":
        entry["Projected Release Date"] = None

    entry["gender"] = entry.pop("Gender") == "F"
    entry["birth_year"] = int(entry['DOB'][0:4])
    entry['valid_start'] = entry['valid_end'] = datetime.strptim(entry['accessed'][:8], '%Y%m%d')
    #TODO add different behaviour for this one
    entry['period'] = None
    entry['released'] = False
   
    #format offenses for insertion
    offense_dict = entry.pop("offensetable")
    offenses = [
        {k: offense_dict[k][i] for k in offense_dict.keys()}
        for i in offense_dict["Offense"].keys()
    ]
    for offense in offenses:
        offense["tdcj_num"] = entry['_id']
        offense["sentence"] = sentence_str_to_days_int(
            offense.pop("Sentence (YY-MM-DD)")
        )

    #personally identifiable information
    
    pii = {
        'name': entry.pop('Name'),
        'date_of_birth': entry.pop('DOB'),
        'sid_num': entry.pop('SID Number'),
        'tdcj_num':entry['_id']
    }

    #name cleanup
    for old_key in entry.keys():
        entry[OFFENDER_MAP_DICT[old_key]] = entry.pop(old_key)
    for offense in offenses: 
        for old_key in offense.keys():
            offense[OFFENSE_MAP_DICT[old_key]] = offense.pop(old_key)

    return entry, offenses, pii


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

    # clean data
    offender_info, offenses, pii = prep_offender_data(entry)

    # insert a record
    try:
        cur.execute(INSERT_PII, pii)

    # for the TDCJ number, insert the record in an error table instead.
    except UniqueViolation as u:
        conn.rollback()
        cur.execute(
            'SELECT * FROM offender'
            f'WHERE sid_num="{pii["sid_num"]}";'
        )
        try:
            result = cur.fetchone()
            handle_multimapped_record(conn, cur, offender_info, offenses, pii, result)
            return
        #not tdcj unique violation
        except ProgrammingError:
            raise u.with_traceback(sys.exc_info()[2])

    # for a different exception, re-raise it with information about the problematic record.
    except Exception as e:
        raise type(e)(f'{str(e)} tdcj_num={offender_info["_id"]}').with_traceback(
            sys.exc_info()[2]
        )
    
    cur.execute(INSERT_OFFENDER.format("offender"), offender_info)
    # insert the offender's offense history
    for offense in offenses:
        cur.execute(INSERT_OFFENSES, offense)
    conn.commit()


def handle_multimapped_record(conn, cur, offender, offenses, pii, result):
    



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
    if msd.endswith("CUMULATIVE OFFENSES"):
        mode = -1
        msd = msd[:-19].strip()
    if msd == "LIFE SENTENCE":
        return (None, 2 * mode)
    elif msd == "LIFE WITHOUT PAROLE":
        return (None, 3 * mode)
    elif msd == "NOT AVAILABLE":
        return (None, 4 * mode)
    elif msd == "DEATH ROW":
        return (None, 5 * mode)
    else:
        try:
            return (datetime.strptime(msd, "%Y-%m-%d"), mode)

        except ValueError as e:
            raise type(e)(f"{str(e)} value: msd={msd}").with_traceback(
                sys.exc_info()[2]
            )


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
    if string.endswith("Days"):
        return int(string[:-4])
    vals = [int(i) for i in string.split("-")]
    return vals[0] * 365 + vals[1] * 30 + vals[2]


if __name__ == "__main__":
    _reset_tdcj_pgdb()
    _create_tables()
    run_pipe()
