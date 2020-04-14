from pymongo import MongoClient

    
def num_scraped():
    db = MongoClient().tdcj
    print(f'{db.inmates.estimated_document_count()} valid TDCJ numbers scraped')
    print(f'{db.unassigned.estimated_document_count()} unassigned TDCJ numbers scraped')
    
if __name__ == '__main__':
    num_scraped()
