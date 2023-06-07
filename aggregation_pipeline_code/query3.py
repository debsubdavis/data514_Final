[
    {
        '$addFields': {
            'month_num': {
                '$month': {
                    '$dateFromString': {
                        'dateString': '$date', 
                        'format': '%Y-%m-%d'
                    }
                }
            }
        }
    }, {
        '$match': {
            'month_num': 12
        }
    }, {
        '$addFields': {
            'year': {
                '$year': {
                    '$dateFromString': {
                        'dateString': '$date', 
                        'format': '%Y-%m-%d'
                    }
                }
            }
        }
    }, {
        '$group': {
            '_id': {
                'city': '$city', 
                'month_num': '$month_num', 
                'year': '$year'
            }, 
            'count': {
                '$count': {}
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'City': '$_id.city', 
            'Year': '$_id.year', 
            'Month': '$_id.month_num', 
            'Number of Reviews': '$count'
        }
    }, {
        '$sort': {
            'City': 1, 
            'Year': 1
        }
    }
]