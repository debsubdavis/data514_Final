import json

class CleaningUtils:


    def process_df(full_df, selected_cols, row_lambda, city, json_output_file_path):
        # filter to only the selected columns
        condensed_df = full_df[selected_cols]
        # drop rows with null values in any of the selected columns
        condensed_df = condensed_df.dropna(subset=selected_cols)
        # add a column with the city
        condensed_df['city'] = city

        #json_data = condensed_df.apply(row_to_json_dict, axis=1).tolist()

        json_data = condensed_df.apply(row_lambda, axis=1).tolist()

        print("completed apply, writing to json")

        # Convert to JSON
        json_string = json.dumps(json_data, indent=2)

        with open(json_output_file_path, "w") as json_file:
            json_file.write(json_string)

        print("wrote to json, now inserting into mongo collection")

        return json_string

