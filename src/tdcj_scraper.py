from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
import time, json, os, sys
import numpy as np
import pandas as pd
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from pymongo.errors import DuplicateKeyError
from datetime import datetime


class Scraper:
    
    def __init__(self, headless = True):
        '''
        Constructs the scraper: starts a webdriver instance 
        and connects to the mongoDB

        Args:
            headless: boolean controlling if the webdriver runs
        '''
        wd_path = f'{os.getcwd()}/src/chromedriver'
        opt = Options()
        opt.headless = headless
        self.driver = Chrome(executable_path=wd_path, options=opt)
        self.db = MongoClient('localhost', 27017).tdcj

    def scrape_TDCJ_array(self, arr):
        '''
        Scrapes an array of TDCJ numbers.
        
        Args:
            arr: array or generator of TDCJ numbers to be scraped

        Returns: 
            List of inmate data for valid TDCJ numbers 
            and False values for invalid TDCJ numbers
        '''
        ret_list = []
        for tdcjnum in arr:
            ret_list.append(self.scrape_inmate(tdcjnum))
        return ret_list


    def scrape_inmate(self, tdcjnum):
        '''
        Scrapes information related to the input number

        Args:
            tdcjnum: int representing a TDCJ number to be scraped.

        Returns: 
            a dictionary of inmate information if the number is valid
            else the value False
        '''
        self.driver.get('https://offender.tdcj.texas.gov/OffenderSearch/start')
        time.sleep(1.5)

        #the form wants an 8-digit number padded on the left with 0s
        qstring = str(tdcjnum)
        qstring =''.join(['0']*(8-len(qstring))) + qstring
        
        #type qstring and hit search
        tdcj_num_field = self.driver.find_element_by_name('tdcj')
        tdcj_num_field.send_keys(qstring)
        self.driver.find_element_by_name('btnSearch')\
            .click()
        time.sleep(1.5)

        try:
            self.driver.find_element_by_class_name('tdcj_table') \
                .find_element_by_tag_name('a')\
                .click()

        #this happens for unassigned tdcj numbers... 
        # or TODO for if the page has loaded too quickly
        except NoSuchElementException:
            return None
        time.sleep(1)

        #we found an inmate!
        #get admin data into a dict
        entry = dict()
        admin_info = self.driver.find_element_by_id('content_right')\
            .find_elements_by_tag_name('p')[1]\
            .text\
            .split('\n\n')
        for row in map(lambda s: s.split(':'),admin_info):
            entry[row[0].strip()] = row[1].strip()

        #add offenses data to the return dict
        offense_table_html = self.driver.find_element_by_class_name('tdcj_table')\
            .get_attribute('outerHTML')
        offenses = json.loads(pd.read_html(offense_table_html)[0].to_json())
        entry['offensetable'] = offenses

        #add the timestamp for when it was pulled
        entry['accessed'] = datetime.now().strftime('%Y%m%d_%H%M')

        #some data cleaning for Mongo
        entry['offensetable']['Case No'] = entry['offensetable'].pop('Case No.')
        entry['_id'] = entry.pop('TDCJ Number')
        return entry

    def scrape_range_to_db(self, scrape_range, print_mode = 0):
        unassigned_count = 0
        for tdcj_num in scrape_range:
            try:
                #scrape single number
                datum = self.scrape_inmate(tdcj_num)

                #for invalid tdcj numbers
                if datum == None:
                    self.db.unassigned.insert_one({'_id':tdcj_num})
                    unassigned_count += 1

                #for valid tdcj numbers
                else:
                    self.db.inmates.insert_one(d)
                    if print_mode >= 2:
                        print(f'{i} added to inmates')
                        print(f'         {unassigned_count} invalid TDCJ numbers prev')
                    unassigned_count = 0
            
            #TDCJ number already in mongo
            except DuplicateKeyError:
                if print_mode >= 1:
                    print(f'Duplicate tdcj number ignored: {i}')
                continue

if __name__ == '__main__':
    s = Scraper()
    scraper_instance = sys.argv[1]
    n=100
    pmode = 2

    bounds = s.db.admin.find({'_id':scraper_instance}, {'bounds':'true'}).next()['bounds']
    while len(bounds) > 0:
        s.db.admin.update_one({'_id':scraper_instance}, {'$pop':{'bounds':-1}})
        
        segment = bounds[0]
        start = int(segment[0])
        end = int(segment[1])
        if start > end:
            start, end = end, start
        subsegment_length = (end-start)//n
        if(subsegment_length*n != (end-start)):
            raise ValueError(f'invalid range {segment} not divisible by {n}')
        for i in range(n):
            s.scrape_range_to_db(range(start+i*subsegment_length,\
                start+(i+1)*subsegment_length), pmode)
            print(f'{100*(i+1)/n}% finished with segment [{start},{end})')
        bounds = s.db.admin.find({'_id':scraper_instance}, {'bounds', 'true'}).next()['bounds']

