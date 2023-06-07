[
    {
        '$lookup': {
            'from': 'Listings', 
            'localField': 'listing_id', 
            'foreignField': '_id', 
            'as': 'listing_info'
        }
    }, {
        '$unwind': {
            'path': '$listing_info'
        }
    }, {
        '$match': {
            'listing_info.room_type': 'Entire home/apt'
        }
    }, {
        '$unwind': {
            'path': '$Months in Date Range'
        }
    }, {
        '$match': {
            'Months in Date Range': {
                '$in': [
                    12, 1, 2, 3, 4, 5
                ]
            }
        }
    }, {
        '$group': {
            '_id': {
                'listing_id': '$listing_id', 
                'city': '$city'
            }, 
            'availability': {
                '$push': {
                    'month': {
                        '$convert': {
                            'input': '$Months in Date Range', 
                            'to': 'int'
                        }
                    }, 
                    'start_date': '$start_date', 
                    'end_date': '$end_date'
                }
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'listing_id': '$_id.listing_id', 
            'city': '$_id.city', 
            'availability': '$availability'
        }
    }, {
        '$sort': {
            'listing_id': 1
        }
    }
]