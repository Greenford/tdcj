import numpy as np
from pymongo import MongoClient

class TDCJ_report:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        inmates = client.tdcj.inmates
        unassigned = client.tdcj.unassigned
        self.inmate_nums = np.array([int(i['_id']) for i in inmates.find(\
            {'_id': {'$exists':'true'}}, {'_id':'true'})])
        self.unassigned_nums = np.array([int(i['_id']) for i in unassigned.find(\
            {'_id': {'$exists':'true'}}, {'_id':'true'})])

    def last_scraped(self):
        digits = [(i,i+5) for i in range(5)]
        idata = self.inmate_nums 

        for pair in digits:
            min_result = np.min(idata[ (idata - (idata//10)*10 == pair[0])\
                | (idata - (idata//10)*10 == pair[1]) ])
            print(f'Min result for inmate TDCJ numbers ending with {pair}:')
            print(f'    {min_result}')
    
    def num_scraped(self):
        print(f'{len(self.inmate_nums)} valid TDCJ numbers scraped')
        print(f'{len(self.unassigned_nums)} unassigned TDCJ numbers scraped')
    
if __name__ == '__main__':
    r = TDCJ_report()
    r.num_scraped()
