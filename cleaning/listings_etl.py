import pandas as pd
import json

from utils.mongo_utils import MongoUtils
from utils.cleaning_utils import CleaningUtils

class ListingsETL:

    def clean(self, file_path, city):

        listings = pd.read_csv(file_path)

        selected_cols_listings = [
            'id',
            'name',
            'listing_url',
            'property_type',
            'room_type',
            'accommodates',
            'price',
            'amenities',
            'review_scores_rating',
            'neighbourhood_cleansed'
        ]

        row_lambda = lambda row: {
            "_id": str(row["id"]),
            "listing_name": row["name"],
            "listing_url": row["listing_url"],
            "city": row['city'],
            "property_type": row["property_type"],
            "room_type": row["room_type"],
            "accommodates": row["accommodates"],
            "nightly_price": float(row["price"].replace("$", "").replace(",", "")),  # Assuming price is a string formatted like "$120.45"
            "amenities": row["amenities"],
            #"isAvail_next_2days": False,  # Assuming this field needs to be updated later
            "rating": row["review_scores_rating"],
            "neighbourhood_cleansed": row["neighbourhood_cleansed"]
        }

        json_output_file_path = "outputs/json/" + city + "/listings.json"

        jsonstr_list = CleaningUtils.process_df(listings, selected_cols_listings, row_lambda, city, json_output_file_path)

        MongoUtils.write_to_collection(jsonstr_list, "Listings", city)
        

