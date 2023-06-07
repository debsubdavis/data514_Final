from cleaning.calendar_etl import CalendarETL
from cleaning.listings_etl import ListingsETL
from cleaning.reviews_etl import ReviewsETL

import glob
import time

def main():
    print("hello world")

    start = time.time()
 
    rootdir = 'data'
    for path in glob.glob(f'{rootdir}/**/', recursive=True):
        print(f'path: {path}')
        file_list = glob.glob(f'{path}/*', recursive=True)
        #file_list  = os.listdir(path)
        print(f'file_list: {file_list}')
        split = path.split("/")

        city = split[1]
        print(f'city: {city}')

        CalETL = CalendarETL()
        ListETL = ListingsETL()
        ReviewETL = ReviewsETL()

        for file in file_list:
            if "listings" in file:
                print("process listings")
                jsonstr_listings = ListETL.clean(file, city)
            elif "calendar" in file:
                print("process calendar")
                jsonstr_cal = CalETL.clean_cal(file, city)
            elif "reviews" in file:
                print("process reviews")
                jsonstr_reviews = ReviewETL.clean(file, city)
        print(f'{city} complete @ {time.time()}!')

    end = time.time()
    
    print("The time of execution of above program is :",
        (end-start), "seconds")

if __name__ == "__main__":
    main()