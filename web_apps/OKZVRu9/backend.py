import dataiku
import pandas as pd
from flask import request


# Example:
# As the Python webapp backend is a Flask app, refer to the Flask
# documentation for more information about how to adapt this
# example to your needs.
# From JavaScript, you can access the defined endpoints using
# getWebAppBackendUrl('first_api_call')

@app.route('/first_api_call')
def first_call():
    max_rows = request.args.get('max_rows') if 'max_rows' in request.args else 500

    mydataset = dataiku.Dataset("REPLACE_WITH_YOUR_DATASET_NAME")
    mydataset_df = mydataset.get_dataframe(sampling='head', limit=max_rows)

    # Pandas dataFrames are not directly JSON serializable, use to_json()
    data = mydataset_df.to_json()
    return json.dumps({"status": "ok", "data": data})


TRUSTED_GROUP = "administrators"

@app.route('/get-sensitive-data')
def get_sensitive_data():
    headers = dict(request.headers)
    # Get the auth info of the user performing the request
    auth_info = dataiku.api_client().get_auth_info_from_browser_headers(headers)
    print ("User doing the query is %s" % auth_info["authIdentifier"])
    # If the user's group is not TRUSTED_GROUP, raise an exception
    if TRUSTED_GROUP not in auth_info["groups"] :
        raise Exception("You do not belong here, go away")
    else:
        data = {"status": "ok", "you_are": auth_info["authIdentifier"]}
    return json.dumps(data)