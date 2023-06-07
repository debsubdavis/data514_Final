[
    {
        '$addFields': {
            'tomorrow': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'timezone': 'America/Los_Angeles', 
                    'date': {
                        '$dateAdd': {
                            'startDate': datetime.utcnow(), 
                            'unit': 'day', 
                            'amount': 1
                        }
                    }
                }
            }, 
            'day_aftr_tmrw': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'timezone': 'America/Los_Angeles', 
                    'date': {
                        '$dateAdd': {
                            'startDate': datetime.utcnow(), 
                            'unit': 'day', 
                            'amount': 2
                        }
                    }
                }
            }
        }
    }, {
        '$match': {
            '$or': [
                {
                    '$expr': {
                        '$eq': [
                            '$date', '$tomorrow'
                        ]
                    }
                }, {
                    '$expr': {
                        '$eq': [
                            '$date', '$day_aftr_tmrw'
                        ]
                    }
                }
            ]
        }
    }, {
        '$group': {
            '_id': {
                'listing_id': '$listing_id', 
                'city': '$city'
            }, 
            'isAvail': {
                '$sum': '$available'
            }
        }
    }, {
        '$match': {
            '$expr': {
                '$eq': [
                    '$isAvail', 2
                ]
            }
        }
    }, {
        '$lookup': {
            'from': 'Listings', 
            'localField': '_id.listing_id', 
            'foreignField': '_id', 
            'as': 'listing_details'
        }
    }, {
        '$project': {
            '_id': 0, 
            'listing_id': '$_id.listing_id', 
            'city': 1, 
            'Is Available for Next Two Days': 'true', 
            'listing_details': '$listing_details'
        }
    }, {
        '$unwind': {
            'path': '$listing_details'
        }
    }, {
        '$sort': {
            'listing_details.rating': -1
        }
    }
]