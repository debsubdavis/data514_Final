import pandas as pd
import json

from utils.mongo_utils import MongoUtils
from utils.cleaning_utils import CleaningUtils

class ReviewsETL:
    # def row_to_json(self, row):

    #     return {
    #         "_id": str(row['listing_id']) + "-" + str(row['id']),
    #         "listing_id": row["listing_id"],
    #         "date": row["date"],
    #         "reviewer_id": row["reviewer_id"],
    #         "reviewer_name": row['reviewer_name'],
    #         "city": row['city'], 
    #         "comments": row["comments"],
    #     }

    # def create_doc_json(self, selected_cols, full_df, city):
    #     print("starting create_doc_json")

    #     condensed_df = CleaningUtils.clean_df(full_df, selected_cols, city)
    #     print("***printing condensed_df***")
    #     print(condensed_df.columns)

    #     #create_json_nested(condensed_df)

    #     json_data = condensed_df.apply(self.row_to_json, axis=1).tolist()
    #     print("completed apply, writing to json")
    #     # Serialize the json_data list to a JSON formatted string
    #     json_string = json.dumps(json_data, indent=2)

    #     json_output_file_path = "outputs/json/" + city + "/reviews.json"
    #     #Write the JSON string to a file
    #     with open(json_output_file_path, "w") as json_file:
    #         json_file.write(json_string)
        #MongoUtils.write_to_collection(json_string, "Listings")


    def clean(self, file_path, city):

        reviews = pd.read_csv(file_path)

        print(reviews.shape)
        print(reviews.columns)
        print(reviews.head(5))

        # selected_cols_reviews = [
        #     'listing_id',
        #     'id',
        #     'date',
        #     'reviewer_id',
        #     'reviewer_name',
        #     'comments',
        # ]

        selected_cols_reviews = [
            'listing_id',
            'date',
        ]

        
        # row_lambda = lambda row: {
        #     "_id": str(row['listing_id']) + "-" + str(row['id']),
        #     "listing_id": row["listing_id"],
        #     "date": row["date"],
        #     "reviewer_id": row["reviewer_id"],
        #     "reviewer_name": row['reviewer_name'],
        #     "city": row['city'], 
        #     "comments": row["comments"],
        # }

        row_lambda = lambda row: {
            #"_id": str(row['listing_id']) + "-" + str(row['date']),
            "listing_id": str(row["listing_id"]),
            "date": row["date"],
            "city": row['city'],
        }

        json_output_file_path = "outputs/json/" + city + "/reviews.json"

        jsonstr_review = CleaningUtils.process_df(reviews, selected_cols_reviews, row_lambda, city, json_output_file_path)

        MongoUtils.write_to_collection(jsonstr_review, "Reviews", city)


