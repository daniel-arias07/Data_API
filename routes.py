from flask import Blueprint
from flask_restful import Api
from controller import S3_Data, TS_Data, TS_Query


# Create Blueprint instance
api_bp = Blueprint('api', __name__)

api = Api(api_bp)

# Register resources
api.add_resource(S3_Data, '/v1/s3_data')
api.add_resource(TS_Data, '/v1/ts_data')
api.add_resource(TS_Query, '/v1/ts_query')