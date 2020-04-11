from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
import time, json, os, sys, asyncio
import numpy as np
import pandas as pd
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from pymongo.errors import DuplicateKeyError
from datetime import datetime


class Scraper:
    def __init__(self, headless=True, sleeptime=1, pmode=0):
        """
        Constructs the scraper: starts a webdriver instance 
        and connects to the mongoDB

        Args:
            headless: boolean controlling if the webdriver runs
        """
        wd_path = f"{os.getcwd()}/src/chromedriver"
        opt = Options()
        opt.headless = headless
        self.driver = Chrome(executable_path=wd_path, options=opt)
        self.db = MongoClient("localhost", 27017).tdcj
        self.sleeptime = sleeptime
        self.pmode = pmode

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

    async def store_idata(self, idata, print_mode=0):
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
                if print_mode >= 2:
                    print(f"{idata} added to unassigned.")
            # for valid tdcj numbers
            else:
                self.db.inmates.insert_one(idata)
                if print_mode >= 2:
                    print(f"{idata['_id']} added to inmates")

        # TDCJ number already in mongo
        except DuplicateKeyError:
            if print_mode >= 1:
                print(f"Duplicate tdcj number ignored: {idata}")

    async def worker(self, q):
        """
        Worker co-routine for scraping. Scrapes tdcj numbers from queue 
        populated by manager.

        Args:
            q (asyncio.Queue): tdcj numbers to scrape

        Returns: None. Ends when the queue is empty.
        """
        while not q.empty():
            tdcjnum = await q.get()
            idata = await self.scrape_inmate(tdcjnum)
            await self.store_idata(idata, self.pmode)
            q.task_done()

    async def tailmanager(self, q, sleeptime, batchsize):
        """
        Populates the queue to scrape with unchecked potential TDCJ numbers in 
        descending order. 

        Args:
            q (asyncio.Queue): queue of tdcj numbers to scrape
            sleeptime (float): sleep time for the manager
            batchsize (int): how many tdcj numbers to add to the queue at once
        """
        tailmax = int(self.db.admin.find_one({"_id": "tail"})["value"])
        while tailmax >= 99999 + batchsize:
            if q.qsize() < batchsize:
                for i in range(tailmax, tailmax - batchsize, -1):
                    q.put_nowait(i)
                tailmax -= batchsize
                self.db.admin.update_one({"_id": "tail"}, {"$set": {"value": tailmax}})
            await asyncio.sleep(sleeptime)


async def main(workersleeptime=1, numworkers=2, mgrsleeptime=5, pmode=0, batchsize=50):
    s = Scraper(sleeptime=workersleeptime, pmode=pmode)
    q = asyncio.Queue()
    workers = [asyncio.create_task(s.worker(q)) for i in range(numworkers)]
    await s.tailmanager(q, mgrsleeptime, batchsize)
    await asyncio.gather(*workers)

    for w in workers:
        w.cancel()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workersleeptime", type=float)
    parser.add_argument("-m", "--mgrsleeptime", type=float)
    parser.add_argument("-p", "--pmode", type=int)
    parser.add_argument("-b", "--batchsize", type=int)
    parser.add_argument("-n", "--numworkers", type=int)
    args = parser.parse_args()

    asyncio.run(main(**args.__dict__))
