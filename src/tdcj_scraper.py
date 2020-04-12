from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
import time, json, os, sys, asyncio, signal
import numpy as np
import pandas as pd
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from pymongo.errors import DuplicateKeyError
from datetime import datetime

class Scraper:
    def __init__(self, headless, workersleeptime, mgrsleeptime, pmode, numworkers, batchsize):
        """
        Constructs the scraper: starts a webdriver instance 
        and connects to the mongoDB

        Args:
            headless (bool): controlling if the webdriver runs
            sleeptime (float): worker sleep time in seconds
            pmode (int): print mode. Higher the number, the more is printed
        """
        # TODO headless?
        self.db = MongoClient("localhost", 27017).tdcj
        self.mgrsleeptime = mgrsleeptime
        self.pmode = pmode
        self.batchsize = batchsize 
        self.q = asyncio.Queue()
        self.workers = [ScraperWorker(self.q, self.db, headless, workersleeptime, pmode) for i in range(numworkers)]

    async def tailmanager(self):
        """
        Populates the queue to scrape with unchecked potential TDCJ numbers in 
        descending order. 
        """
        tailmax = int(self.db.admin.find_one({"_id": "tail"})["value"])
        while tailmax >= 99999 + self.batchsize:
            if self.q.qsize() < self.batchsize:
                for i in range(tailmax, tailmax - self.batchsize, -1):
                    self.q.put_nowait(i)
                tailmax -= self.batchsize
                self.db.admin.update_one({"_id": "tail"}, {"$set": {"value": tailmax}})
            await asyncio.sleep(self.mgrsleeptime)

    async def deathrowMGR(self):
        """
        Populates the queue with active DR tdcj numbers.
        """
        pass

    async def headMGR(self):
        """
        Populates the queue with potential recently-assigned tdcj numbers.
        """
        pass
    
    async def recidivismMGR(self):
        """
        Populates the queue with confirmed unassigned tdcj numbers.
        """
        pass

    async def releaseMGR(self):
        """
        Populates the queue with potentially released or paroled tdcj numbers.
        """
        pass

class ScraperWorker: 
    def __init__(self, q, db, headless, sleeptime, pmode):
        """
        Constructs the worker with own webdriver. 

        Args:
            headless (bool): controlling if the webdriver runs
            sleeptime (float): worker sleep time in seconds
            pmode (int): print mode. Higher the number, the more is printed
        """
        wd_path = f"{os.getcwd()}/src/chromedriver"
        opt = Options()
        opt.headless = headless
        
        self.driver = Chrome(executable_path=wd_path, options=opt)
        self.db = db
        self.pmode = pmode
        self.sleeptime = sleeptime
        self.q = q

    async def scrape_inmate(self, tdcjnum):
        """
        Scrapes information related to the input number

        Args:
            tdcjnum: int representing a TDCJ number to be scraped.

        Returns: 
            a dictionary of inmate information if the number is valid
            else the value False
        """
        self.driver.get("https://offender.tdcj.texas.gov/OffenderSearch/start")
        await asyncio.sleep(self.sleeptime)

        # the form wants an 8-digit number padded on the left with 0s
        qstring = str(tdcjnum)
        qstring = "".join(["0"] * (8 - len(qstring))) + qstring

        # type qstring and hit search
        tdcj_num_field = self.driver.find_element_by_name("tdcj")
        tdcj_num_field.send_keys(qstring)
        self.driver.find_element_by_name("btnSearch").click()
        await asyncio.sleep(self.sleeptime)

        try:
            self.driver.find_element_by_class_name(
                "tdcj_table"
            ).find_element_by_tag_name("a").click()

        # this happens for unassigned tdcj numbers...
        # or TODO for if the page has loaded too quickly
        except NoSuchElementException:
            return tdcjnum
        await asyncio.sleep(self.sleeptime)

        # we found an inmate!
        # get admin data into a dict
        entry = dict()
        admin_info = (
            self.driver.find_element_by_id("content_right")
            .find_elements_by_tag_name("p")[1]
            .text.split("\n\n")
        )
        for row in map(lambda s: s.split(":"), admin_info):
            entry[row[0].strip()] = row[1].strip()

        # add offenses data to the return dict
        offense_table_html = self.driver.find_element_by_class_name(
            "tdcj_table"
        ).get_attribute("outerHTML")
        offenses = json.loads(pd.read_html(offense_table_html)[0].to_json())
        entry["offensetable"] = offenses

        # add the timestamp for when it was pulled
        entry["accessed"] = datetime.now().strftime("%Y%m%d_%H%M")

        # some data cleaning for Mongo
        entry["offensetable"]["Case No"] = entry["offensetable"].pop("Case No.")
        entry["_id"] = entry.pop("TDCJ Number")
        return entry

    async def store_idata(self, idata):
        """
        Asyncronously inserts the scraped data of an inmate into the mongodb.

        Args: 
            idata (dict or int): int if no data for tdch number, dict if otherwise
            print_mode (int): how much to print. The higher the number, the more printed.
        
        Returns: None, but inserts into inmates or unassigned.
        """
        try:
            # for invalid tdcj numbers
            if type(idata) == int:
                self.db.unassigned.insert_one({"_id": idata})
                if self.pmode >= 2:
                    print(f"{idata} added to unassigned.")
            # for valid tdcj numbers
            else:
                self.db.inmates.insert_one(idata)
                if self.pmode >= 2:
                    print(f"{idata['_id']} added to inmates")

        # TDCJ number already in mongo
        except DuplicateKeyError:
            if self.pmode >= 1:
                if type(idata) == dict:
                    idata = idata['_id']
                print(f"Duplicate tdcj number ignored: {idata}")

    async def work(self):
        """
        Worker co-routine for scraping. Scrapes tdcj numbers from queue 
        populated by manager.

        Args:
            q (asyncio.Queue): tdcj numbers to scrape

        Returns: None. Ends when the queue is empty.
        """
        while not self.q.empty():
            tdcjnum = await self.q.get()
            idata = await self.scrape_inmate(tdcjnum)
            await self.store_idata(idata)
            self.q.task_done()

def main(args):
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s)))
        loop.set_exception_handler(handle_exception)
    
    scr = Scraper(**args)
    try: 
        loop.create_task(scr.tailmanager())
        [loop.create_task(w.work()) for w in scr.workers]
        loop.run_forever()
    finally:
        loop.close()

def handle_exception(loop, context):
    msg = context.get('exception', context['message'])
    print(f"Caught exception: {msg}")
    asyncio.create_task(shutdown(loop))

async def shutdown(loop, signal=None):
    if signal:
        print(f'Received exit signal {signal.name}...')
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop() 


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workersleeptime", type=float, default=1.5)
    parser.add_argument("-m", "--mgrsleeptime", type=float, default=5)
    parser.add_argument("-p", "--pmode", type=int, default=1)
    parser.add_argument("-b", "--batchsize", type=int, default=50)
    parser.add_argument("-n", "--numworkers", type=int, default=3)
    parser.add_argument("-v", dest="headless", action='store_false')
    parser.set_defaults(headless=True)
    args = parser.parse_args()
    
    main(args.__dict__)
