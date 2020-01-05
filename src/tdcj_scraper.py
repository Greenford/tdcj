from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
import time, json
import numpy as np
import pandas as pd
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from pymongo.errors import DuplicateKeyError
from datetime import datetime
import os


class Scraper:
    
    def __init__(self, headless = True):
        wd_path = f'{os.getcwd()}/src/chromedriver'
        opt = Options()
        opt.headless = headless
        self.driver = Chrome(executable_path=wd_path, options=opt)
        self.mongoclient = MongoClient('localhost', 27017)
        #take 1
        #self.nlist=np.concatenate((np.arange(2500000),np.arange(99900000, 100000000)), axis=None)
        #np.random.shuffle(self.nlist)
        #gonne move this outside of the init into main

    def scrape_inmates(self, arr):
        ret_list = []
        for tdcjnum in arr:
            ret_list.append(self.scrape_inmate(tdcjnum))
        return ret_list


    def scrape_inmate(self, tdcjnum):
        #TODO how to make sure I'm not trying to pull from the webpage too fast?
        self.driver.get('https://offender.tdcj.texas.gov/OffenderSearch/start')
        time.sleep(1.5)

        #the form wants an 8-digit number padded on the left with 0s
        qstring = str(tdcjnum)
        qstring =''.join(['0']*(8-len(qstring))) + qstring
        
        #type number and hit search
        tdcj_num_field = self.driver.find_element_by_name('tdcj')
        tdcj_num_field.send_keys(qstring)
        self.driver.find_element_by_name('btnSearch').click()
        time.sleep(1.5)

        try:
            self.driver.find_element_by_class_name('tdcj_table') \
                .find_element_by_tag_name('a').click()
        #this happens for unassigned tdcj numbers... 
        # or TODO for if the page has loaded too quickly
        except NoSuchElementException:
            return False
        time.sleep(1)

        #we found an inmate!
        #get admin data into a dict
        t = self.driver.find_element_by_id('content_right') \
                .find_elements_by_tag_name('p')[1].text
        t = list(map(lambda s: s.split(':'),t.split('\n\n')))
        entry = dict()
        for row in t:
            entry[row[0].strip()] = row[1].strip()

        #add offenses data to the dict
        t = self.driver.find_element_by_class_name('tdcj_table').get_attribute('outerHTML')
        offenses = json.loads(pd.read_html(t)[0].to_json())
        entry['offensetable'] = offenses

        #add the timestamp for when it was pulled
        entry['accessed'] = datetime.now().strftime('%Y%m%d_%H%M')

        #some data cleaning for Mongo
        entry['offensetable']['Case No'] = entry['offensetable'].pop('Case No.')
        entry['_id'] = entry.pop('TDCJ Number')
        return entry

if __name__ == '__main__':
    s = Scraper()
    #TODO make something for unassigned TDCJ numbers
    
    #2nd iteration
    #ngen = range(2230000,100000,-1)

    #3rd iteration ascending
    #start = 2250000
    #increment = 10000
    #checkrange = 500

    #4th iteration ascending
    #start = 2277968
    #increment = 1000
    #checkrange = 200

    #5th iteration, descending
    #start = 2133500
    #end = 100000

    #6th iteration, remote ascending
    #start = 100000
    #end =   1000000

    #7th iteration, remote ascending
    #start = 100001
    #end =   1000001

    #8th iteration descending, continuing 5th iteration
    #start = 2123441
    #end   = 1000000

    #9th iteration ascending, continuing 4th iteration
    #start = 2286080
    #end   = 2500000
    #inc   = 1
    
    #10-14th iterations
    inc = -5
    #start = 2109955
    end =   1900000

    #10
    start=2107280

    #11
    #start = 2108556

    #12
    #start = 2100712

    #13
    #start 2106233

    #14 
    #start = 2106899

    ngen = range(start, end, inc)

    unassigned = s.mongoclient.tdcj.unassigned
    inmates = s.mongoclient.tdcj.inmates
    #while True:
    unassigned_count = 0
    for i in ngen:
        try:
            d = s.scrape_inmate(i)
            if d == False:
                unassigned.insert_one({'_id':i})
                unassigned_count += 1
            else:
                inmates.insert_one(d)
                print(f'{i} added to inmates')
                print(f'         {unassigned_count} invalid TDCJ numbers prev')
                unassigned_count = 0
        except DuplicateKeyError:
            print(f'Duplicate tdcj number ignored: {i}')
            continue
        except Exception as e:
            print(f'Exited. Current tdcj scraping: {i}')
            raise e
        '''
        #get count of inmates in last checkrange tdcj numbers
        #if there has been 1 new inmate, change start and increment and continue
        r = np.array([int(i['_id']) for i in inmates.find(\
            {'_id': {'$exists':'true'}}, {'_id':'true'})])
        r=r[r >= start+increment-checkrange]
        r=r[r < start+increment]
        print(f'@@@@@ Valid TDCJ numbers in the last {checkrange}: {len(r)} @@@@@')
        if len(r) > 0:
            start = start + increment
            ngen = range(start, start + increment)
        else:
            break #while
        '''

