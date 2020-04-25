from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
    TimeoutException,
)
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import time, json, os, asyncio, signal
import numpy as np
import pandas as pd
from datetime import datetime


class Scraper:
    def __init__(
        self, headless, workersleeptime, mgrsleeptime, pmode, numworkers, batchsize
    ):
        """
        Constructs the scraper: starts a webdriver instance 
        and connects to the mongoDB

        Args:
            headless (bool): controlling if the webdriver runs
            workersleeptime (float): worker sleep time in seconds
            mgrsleeptime (float): manager sleep time in seconds
            pmode (int): print mode. Higher the number, the more is printed
            numworkers (int): number of workers to initiate
            batchsize (int): number of tdcj numbers for manager to load at a time
        """
        self.db = MongoClient("localhost", 27017).tdcj
        self.mgrsleeptime = mgrsleeptime
        self.pmode = pmode
        self.batchsize = batchsize
        self.q = asyncio.Queue()
        self.workers = [
            ScraperWorker(self.q, self.db, headless, workersleeptime, pmode)
            for i in range(numworkers)
        ]

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
                if self.pmode >= 1:
                    print(f"Added tail tasks {tailmax}..{tailmax-self.batchsize}")
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
            q (asyncio.Queue): queue to contain scraping tasks
            db (pymongo.database.Database): relevant database to write to
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
        self.sleepmult = 1
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
        await self.search_by_number(tdcjnum)
        try:
            self.driver.find_element_by_class_name(
                "tdcj_table"
            ).find_element_by_tag_name("a").click()

        # this happens for unassigned tdcj numbers...
        except NoSuchElementException:
            return tdcjnum

        await asyncio.sleep(self.sleeptime)
        await self.wait_until_present(By.ID, "content_right")
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

    async def search_by_number(self, tdcjnum, retry=False):
        """
        Searches the tdcj website for a possible inmate number.

        Args:
            tdcjnum (int): possible tdcj number
            retry (bool): for recursive usage; True if call has failed once
        """
        self.driver.get("https://offender.tdcj.texas.gov/OffenderSearch/start")
        # the form wants an 8-digit number padded on the left with 0s
        qstring = str(tdcjnum)
        qstring = "".join(["0"] * (8 - len(qstring))) + qstring
        await asyncio.sleep(self.sleeptime)
        await self.wait_until_present(By.NAME, "tdcj")

        # type qstring and hit search
        tdcj_num_field = self.driver.find_element_by_name("tdcj")
        tdcj_num_field.send_keys(qstring)
        self.driver.find_element_by_name("btnSearch").click()
        await asyncio.sleep(self.sleeptime)

        #retries the call once if it fails
        try:
            await self.wait_until_present(By.ID, "content_right")
        except TimeoutException as e:
            if retry:
                raise e
            else:
                self.search_by_number(tdcjnum, True)

    async def wait_until_present(self, by, label):
        """
        Convenience method for waiting until an element is present. Also makes 
        waittime for elements elastic for changing network latency.

        Args:
            timeout (float): time to wait in seconds
            by (selenium.webdriver..by): selector type
            label (str): label of element
        """
        try:
            WebDriverWait(self.driver, self.sleeptime * self.sleepmult).until(
                EC.presence_of_element_located((by, label))
            )
            #decrease wait time
            if self.sleepmult > 1:
                self.sleepmult -= 0.1
        except TimeoutException as e:
            #wait longer times
            if self.sleepmult < 3:
                self.sleepmult += 1
                print(f'Sleeptime increased to {self.sleepmult * self.sleeptime} seconds.')
                await self.wait_until_present(by, label)
            else:
                raise e

    async def store_idata(self, idata):
        """
        Asyncronously inserts the scraped data of an inmate into the mongodb.

        Args: 
            idata (dict or int): int if no data for tdch number, dict if otherwise
        
        Returns: None, but inserts into inmates or unassigned.
        """
        try:
            # for invalid tdcj numbers
            if type(idata) == int:
                self.db.unassigned.insert_one({"_id": idata})
                if self.pmode >= 3:
                    print(f"{idata} added to unassigned.")
            # for valid tdcj numbers
            else:
                self.db.inmates.insert_one(idata)
                if self.pmode >= 3:
                    print(f"{idata['_id']} added to inmates")

        # TDCJ number already in mongo
        except DuplicateKeyError:
            if self.pmode >= 2:

                # TODO need split behavior for cases:
                # 1. tdcj number is valid but already in unassigned collection (reincarcerated)
                # 2. tdcj number is valid but already in inmates collection (update)
                # 3. tdcj number is invalid but in inmates collection (release)
                if type(idata) == dict:
                    idata = idata["_id"]
                print(f"Duplicate tdcj number ignored: {idata}")

    async def work(self):
        """
        Worker co-routine for scraping. Scrapes tdcj numbers from queue 
        populated by manager.

        Returns: None. Ends when the queue is empty.
        """
        while not self.q.empty():
            tdcjnum = await self.q.get()
            idata = await self.scrape_inmate(tdcjnum)
            await self.store_idata(idata)
            self.q.task_done()


def main(args):
    """
    Sets up async environment and runs the scraper. 

    Args:
        args (dict): parameters for Scraper()
    """
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
        )
        loop.set_exception_handler(handle_exception)

    scr = Scraper(**args)
    try:
        loop.create_task(scr.tailmanager())
        [loop.create_task(w.work()) for w in scr.workers]
        loop.run_forever()
    finally:
        loop.close()


def handle_exception(loop, context):
    """
    Simple async exception handler that just prints the exception.

    Args:
        loop (asyncio event loop): event loop
        context (dict): asyncio error context 
    """
    # context doesn't always have an exception
    e = context.get("exception", context)
    if not isinstance(e, Exception):
        import pprint

        print(f"Caught exception without object:")
        pprint.pprint(e)
        asyncio.create_task(shutdown(loop))
    else:
        asyncio.create_task(shutdown(loop))
        print("Caught exception:")
        raise e


async def shutdown(loop, signal=None):
    """
    Defines shutdown behavior for the event loop.

    Args:
        loop (asyncio event loop): event loop
        signal: signal received.
    """
    if signal:
        print(f"Received exit signal {signal.name}...")
    print(datetime.now().strftime("%Y%m%d_%H%M"), "Shutting down...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workersleeptime", type=float, default=1.0)
    parser.add_argument("-m", "--mgrsleeptime", type=float, default=5)
    parser.add_argument("-p", "--pmode", type=int, default=1)
    parser.add_argument("-b", "--batchsize", type=int, default=50)
    parser.add_argument("-n", "--numworkers", type=int, default=3)
    parser.add_argument("-v", dest="headless", action="store_false")
    parser.set_defaults(headless=True)
    args = parser.parse_args()

    main(args.__dict__)
