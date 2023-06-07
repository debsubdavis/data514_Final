import pandas as pd

portland_calendar = pd.read_csv("data/portland/calendar.csv")
print(f'Portland Calendar')
print(f'Shape: {portland_calendar.shape}')
print(f'Columns: {portland_calendar.columns}')


portland_calendar = portland_calendar.dropna(subset=['available'])

portland_calendar['available'] = portland_calendar['available'].map({'t': True, 'f': False})

portland_calendar['date'] = pd.to_datetime(portland_calendar['date'])
portland_calendar['price'] = portland_calendar['price'].replace({',': ''}, regex=True).str.replace('$', '').astype(float)

#df_listing = portland_calendar[(portland_calendar["listing_id"] == 633385) & (portland_calendar['available'] == True)]

df_listing = portland_calendar[(portland_calendar['available'] == True)]

print(f'df_listing')
print(f'Shape: {portland_calendar.shape}')
print(f'Columns: {portland_calendar.columns}')
print("Looking for null values...")
# Check for null values in columns
null_columns = df_listing.columns[df_listing.isnull().any()]

# Print the columns with null values
print(null_columns)

df_listing.sort_values(['listing_id', 'date'], inplace=True)
df_listing.reset_index(drop=True, inplace=True)

print(df_listing.head(15))

consecutive_days = 0
availability_estimate = []
start_date = None
end_date = None

for index, row in df_listing.iterrows():
    if index < len(df_listing) - 1:
        next_date = df_listing.loc[index + 1, 'date']
        curr_date = row['date']
        #print(f'next_date: {next_date}, curr_date {curr_date}')
        #if df_listing.loc[index + 1, 'date'] - row['date'] == pd.Timedelta(days=1):
        if next_date - curr_date == pd.Timedelta(days=1):
            #print(f"testing, consecutive_days before counter {consecutive_days}")
            consecutive_days += 1
            #print(f'consecutive_days after counter {consecutive_days}')
            if start_date is None:
                start_date = row['date']
        else:
            if start_date is not None:
                end_date = row['date']
                min_nights = row['minimum_nights']
                #print(f'min_nights: {min_nights}, consecutive_days: {consecutive_days}')
                if consecutive_days >= row['minimum_nights']:
                    min_nights = row['minimum_nights']
                    #print(f'consecutive days: {consecutive_days}, minimum nights: {min_nights}')
                    availability_estimate.append((start_date, end_date, row['listing_id'], start_date.month, start_date.quarter))
            consecutive_days = 0
            start_date = None
    else:
        if start_date is not None:
            end_date = row['date']
            min_nights = row['minimum_nights']
            #print(f'min_nights: {min_nights}, consecutive_days: {consecutive_days}')
            if consecutive_days >= row['minimum_nights']:
                min_nights = row['minimum_nights']
                #print(f'consecutive days: {consecutive_days}, minimum nights: {min_nights}')
                availability_estimate.append((start_date, end_date, row['listing_id'], start_date.month, start_date.quarter))
        consecutive_days = 0
        start_date = None

print("done with the loop, creating the new dataframe")
df_availability = pd.DataFrame(availability_estimate, columns=['start_date', 'end_date', 'listing_id', 'Start Date Month', 'Start Date Quarter'])
print(df_availability.head(15))
print(df_availability.shape)



# (Booking trend for Spring v/s Winter) For “Entire home/apt” type listings in Portland provide it’s availability estimate for each month of Spring and Winter this year.
# NOTE: availability estimate: Chunks of time within which booking is possible. Compare the number of minimum nights against the consecutively available days. If there is availability on Friday, but Thursday, Saturday and Sunday are not available and the minimum number of days is 2, then Friday also doesn’t qualify for booking and is not part of the final result. Also, note, the minimum nights bookable for a listing may vary based on the day of the week). 

# The seasons are defined as spring (March, April, May), summer (June, July, August), autumn (September, October, November) and winter (December, January, February).




# df_availability['season'] = df_availability['start_date'].dt.quarter.map({1: 'Winter', 2: 'Spring', 3: 'Summer', 4: 'Fall'})
# #df_availability['season'] = df_availability['season'].map({1: 'Winter', 2: 'Spring', 3: 'Summer', 4: 'Fall'})

# df_availability['start_date'] = pd.to_datetime(df_availability['start_date'])
# df_availability['end_date'] = pd.to_datetime(df_availability['end_date'])

# df_availability['date_range'] = df_availability.apply(lambda row: pd.date_range(row['start_date'], row['end_date']), axis=1)

# df_availability = df_availability.explode('date_range')

# df_availability['year_month'] = df_availability['date_range'].dt.to_period('M')

# df_availability_grouped = df_availability.groupby(['year_month', 'listing_id']).agg({'date_range': ['min', 'max']}).reset_index()

# df_availability_grouped.columns = ['_'.join(col).strip('_') for col in df_availability_grouped.columns.values]
 
# df_availability_grouped.rename(columns={'date_range_min': 'start_date', 'date_range_max': 'end_date'}, inplace=True)

# df_availability_grouped['month'] = df_availability_grouped['start_date'].dt.month
# df_availability_grouped['season'] = df_availability_grouped['start_date'].dt.quarter.map({1: 'Winter', 2: 'Spring', 3: 'Summer', 4: 'Fall'})

# Calculate average price
def calculate_average_price(row):
    mask = (portland_calendar['date'] >= row['start_date']) & (portland_calendar['date'] <= row['end_date'])
    return portland_calendar.loc[mask, 'price'].mean()

df_availability['average_price'] = df_availability.apply(calculate_average_price, axis=1)

print(df_availability.head(15))
# df_availability_grouped.to_csv("df_availability_grouped.csv", index=False)
