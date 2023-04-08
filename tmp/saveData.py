from connectDatabase import connectDatabase
from extractData import extractData
import sys



if __name__ == "__main__":
    db_name = sys.argv[1]
    db = connectDatabase(db_name)

    url = sys.argv[2]
    comment_collection = db['comments'] 
    items = extractData(url) 

    try:
        print('Insert {} documents into database.'.format(len(items)))
        comment_collection.insert_many(items)
    except:
        print('Insert unsuccessfully!')

