import io
import os
import json
import time
import configparser
import ast
import pickle

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


from flask_restful import Resource, reqparse
from flask import jsonify, make_response, Response
from flask import request

from datetime import datetime, timezone

from backend import Timestream_Handler, S3_Handler, Utils
from log import Log


class TS_Query(Resource, Log):
    def get(self):
        try:
            self.log("info", TS_Query.get.__name__, f"{'*'*20} TS_Query {'*'*20}")

            get_parser = reqparse.RequestParser()

            self.add_get_arguments(get_parser)

            args = get_parser.parse_args()
            
            self.log("info", TS_Query.get.__name__, f"args: {args}")

            if not self.check_args(args):
                self.log("info", TS_Query.get.__name__, f"args: {args}")
                return {"data" : f"missing required args"}, 400

            self.data, self.elapsed_time = self.query_data_from_timestream(args)
            
            query_response = Response(
                response = self.data.to_json()
                , status=200
            )

            return query_response

        except (Exception) as e:
            self.log("error", f'{TS_Query.get.__name__}', f'Exception: {e}')
            return {'data': None}, 500

    def check_args(self, args):
        try:
            required = ["query"]
            for required_param in required:
                if args.get(required_param, None) is None:
                    self.log("error", TS_Query.check_args.__name__, f"param: {required_param} is required and not found in the request")
                    return False                    
        except (Exception) as e:
            self.log("error", TS_Query.check_args.__name__, f"Exception: {e}")
            return False

        try:
            query = args.get('query', None)
            self.log("info", TS_Query.check_args.__name__, f"query: {query}")

        except (Exception) as e:
            self.log("error", TS_Query.check_args.__name__, f"Exception: {e}")
            return False

        return True

    def query_data_from_timestream(self
                                , args):
        try:
            ts_handler = Timestream_Handler()

            t = time.process_time()

            query = args.get("query", None)

            ts_data = ts_handler.control_query_data(query)

            elapsed_time = time.process_time() - t
            
            return ts_data, elapsed_time

        except (Exception) as e:
            self.log("error", TS_Query.query_data_from_timestream.__name__, f"Exception: {e}")
            return pd.DataFrame()

    def add_get_arguments(self, get_parser):
        get_parser.add_argument('query', type=str, required=True, location='args')
               
class TS_Data(Resource, Log):
    def get(self):
        try:
            self.log("info", TS_Data.get.__name__, f"{'*'*20} TS_Data GET {'*'*20}")

            ts_data = TS_Data()
            get_parser = reqparse.RequestParser()

            self.add_get_arguments(get_parser)

            args = get_parser.parse_args()
            
            self.log("info", TS_Data.get.__name__, f"args: {args}")

            if self.check_args(args) == False:
                self.log("info", TS_Data.get.__name__, f"args: {args}")


            customer = args['customer'].lower()
            building = args['building'].lower()
            first_date = args['first_date']
            last_date = args['last_date']
            model = args['model']
            config = args['config']
            suffix = args['suffix']
            frequency = args['frequency']
            frequency_q = args['frequency_q']
            merge = args.get('merge', "outer")
            predictions_model = args['predictions_model']
            forecast_offset:str = args['forecast_offset']

            s3_uri = args['s3_uri'] if args['s3_uri'] is not None else ""
            s3_upload = int(args['s3_upload']) if args['s3_upload'] is not None else 0
            self.get_local_download = int(args['local_download']) == 1

            self.original_file_name = Utils.generate_file_name(
                model=model, config=config, customer=customer, building=building, first_date=first_date, end_date=last_date, suffix=suffix)

            self.log("info", TS_Data.get.__name__, f"self.original_file_name: {self.original_file_name}")

            self.data, self.elapsed_time = self.get_data_from_timestream(customer
                                        , building
                                        , first_date
                                        , last_date
                                        , frequency
                                        , frequency_q
                                        , merge
                                        , predictions_model
                                        , forecast_offset)

            local_path = os.path.join("data", self.original_file_name)
            self.data.to_parquet(local_path)

            self.log("info", f'{TS_Data.get.__name__}', f'local file saved in: {local_path}')

            if s3_upload:
                # Save to S3
                s3_handler = S3_Handler()                

                uploaded_response = s3_handler.upload_file(
                    file=local_path, file_name=self.original_file_name, s3_path=s3_uri, extension="parquet")
                if  uploaded_response:
                    upload_s3_message = f"file saved in S3: {s3_uri}"
                    empty_data_folder_result, empty_data_folder_message = Utils.empty_data_folder()
                    if empty_data_folder_result == False:
                        self.log("error", S3_Data.post.__name__, empty_data_folder_message)
                else: 
                    upload_s3_message = f"Error trying to save upload file in S3"

                self.log("info", f'{TS_Data.get.__name__}', f'{upload_s3_message}')
            else:
                self.log("info", f'{TS_Data.get.__name__}', f's3_upload value: {s3_upload}')


            response = ts_data.make_rest_response(self.get_local_download
                                                , self.data
                                                , self.original_file_name
                                                , "parquet"
                                                , s3_uri
                                                , s3_upload
                                                , self.elapsed_time)
            return response 

        except (Exception) as e:
            self.log("error", f'{TS_Data.get.__name__}', f'Exception: {e}')
            return {'data': None}, 500

    def post(self):
        try:
            self.log("info", TS_Data.get.__name__, f"{'*'*20} TS_Data POST {'*'*20}")

            ts_handler = Timestream_Handler()

            parquet_data = request.get_data()
            
            df_post:pd.DataFrame = Utils.bytes_to_data_frame(parquet_data)

            df_post["time"] = pd.to_datetime(df_post['time'])
            df_post.set_index("time", inplace=True)

            self.log("info", f'{TS_Data.post.__name__}', f'parquet_data: {len(parquet_data)}')
            self.log("info", f'{TS_Data.post.__name__}', f'df_post: {df_post.shape}')
            self.log("info", f'{TS_Data.post.__name__}', f'index type: {type(df_post.index)}')
            
            self.log("info", f'{TS_Data.post.__name__}', f'index [:3]:\n{(df_post.index[:3])}')
            self.log("info", f'{TS_Data.post.__name__}', f'index [-3:]:\n{(df_post.index[-3:])}')

            if 'future_timestamp' in df_post.columns:
                self.log("info", f'{TS_Data.post.__name__}', f'future_timestamp[:3]:\n{(df_post[:3]["future_timestamp"])}')
                self.log("info", f'{TS_Data.post.__name__}', f'future_timestamp[-3:]:\n{(df_post[-3:]["future_timestamp"])}')

            try:
                now = datetime.now()
                df_filtered = df_post[df_post.index > now]
            except Exception as e:
                self.log("error", f'{TS_Data.post.__name__}', f'Error: {e}')
                try:
                    now = datetime.now(tz=timezone.utc)
                    df_filtered = df_post[df_post.index > now]
                except Exception as utc_e:
                    self.log("error", f'{TS_Data.post.__name__}', f'Error: {utc_e}')
                    df_filtered = pd.DataFrame()


            # Filter rows that are above or past the current datetime
            self.log("info", f'{TS_Data.post.__name__}', f'df_filtered shape: {df_filtered.shape}')
            
            if df_filtered.shape[0] > 0:
                self.log("info", f'{TS_Data.post.__name__}', f'Remove future index not valid')
                df_post = df_post[df_post.index <= now]
                self.log("info", f'{TS_Data.post.__name__}', f'index [:3]:\n{(df_post.index[:3])}')
                self.log("info", f'{TS_Data.post.__name__}', f'index [-3:]:\n{(df_post.index[-3:])}')

                if 'future_timestamp' in df_post.columns:
                    self.log("info", f'{TS_Data.post.__name__}', f'after remove future_timestamp[:3]:\n{(df_post[:3]["future_timestamp"])}')
                    self.log("info", f'{TS_Data.post.__name__}', f'after remove future_timestamp[-3:]:\n{(df_post[-3:]["future_timestamp"])}')
            
            headers = request.headers

            database = headers['database']
            measurement = headers['measurement']
            tags = ast.literal_eval(headers['tags'])

            version = int(datetime.now().timestamp())

            self.log("info", f'{TS_Data.post.__name__}', f' database: {database}')
            self.log("info", f'{TS_Data.post.__name__}', f' measurement type:{measurement}')
            self.log("info", f'{TS_Data.post.__name__}', f' version: {version}')
            self.log("info", f'{TS_Data.post.__name__}', f' tags: {tags}')
            self.log("info", f'{TS_Data.post.__name__}', f' tpye tags: {type(tags)}')

            upload_to_ts = ts_handler.upload_df(tags=tags
                                , measurement = measurement
                                , database = database
                                , df = df_post
                                , version = version)

            ts_post_response = Response()

            if upload_to_ts:
                ts_post_response.status_code = 200
                message = f'df writed in database: {database} table: {measurement}'
                ts_post_response.data = message
                self.log("info", f'{TS_Data.post.__name__}', f'message: {message}')

            else:
                ts_post_response.status_code = 500
                message = f'df could not be writed in database: {database} table: {measurement}'
                ts_post_response.data = message
                self.log("error", f'{TS_Data.post.__name__}',  f'message: {message}')

            return ts_post_response

        except (Exception) as e:
            self.log("error", TS_Data.post.__name__, f"Exception: {e}")
            return {"data": None}, 500

    def check_args(self, args):
        try:
            required = ["customer", "building", "first_date", "last_date", "model", "config", "suffix"]
            for required_param in required:
                if args.get(required_param, None) is None:
                    self.log("error", TS_Data.check_args.__name__, f"param: {required_param} is required and not found in the request")
                    return False                    
        except (Exception) as e:
            self.log("error", TS_Data.check_args.__name__, f"s3_upload Exception: {e}")
            return False

        try:
            customer = args.get('customer', None)
            building = args.get('building', None)
            first_date = args.get('first_date', None)
            last_date = args.get('last_date', None)
            model = args.get('model', None)
            config = args.get('config', None)

            s3_uri = args.get('s3_uri', None)

            s3_upload = int(args.get('s3_upload', 0))
            get_local_download = int(args.get('local_downloa', 0))
            suffix = args.get('suffix', None)
            frequency = args.get('frequency', None)
            frequency_q = args.get('frequency_q', None)
            merge = args.get('merge', None)
            predictions_model = args.get('predictions_model', None)

            self.log("info", TS_Data.check_args.__name__, f"customer: {customer}")
            self.log("info", TS_Data.check_args.__name__, f"building: {building}")
            self.log("info", TS_Data.check_args.__name__, f"first_date: {first_date}")
            self.log("info", TS_Data.check_args.__name__, f"last_date: {last_date}")
            self.log("info", TS_Data.check_args.__name__, f"model: {model}")
            self.log("info", TS_Data.check_args.__name__, f"s3_uri: {s3_uri}")
            self.log("info", TS_Data.check_args.__name__, f"config: {config}")
            self.log("info", TS_Data.check_args.__name__, f"s3_upload: {s3_upload}")
            self.log("info", TS_Data.check_args.__name__, f"get_local_download: {get_local_download}")
            self.log("info", TS_Data.check_args.__name__, f"suffix: {suffix}")
            self.log("info", TS_Data.check_args.__name__, f"frequency: {frequency}")
            self.log("info", TS_Data.check_args.__name__, f"frequency_q: {frequency_q}")
            self.log("info", TS_Data.check_args.__name__, f"merge: {merge}")
            self.log("info", TS_Data.check_args.__name__, f"predictions_model: {predictions_model}")

        except (Exception) as e:
            self.log("error", TS_Data.check_args.__name__, f"Exception: {e}")
            return False

        return True

    def get_data_from_timestream(self
                                , customer:str
                                , building:str
                                , first_date:str
                                , last_date:str
                                , frequency:str
                                , frequency_q:str
                                , merge:str
                                , predictions_model:str
                                , forecast_offset:str):
        try:
            ts_handler = Timestream_Handler()

            # get the request body as bytes
            req_data = request.get_data()

            # decode the request body using the appropriate encoding
            req_data_str = req_data.decode('utf-8')

            # parse the request body as JSON
            req_json = json.loads(req_data_str)

            t = time.process_time()

            ts_data = ts_handler.get_data(building=building, first_date=first_date,
                                            last_date=last_date, req_json=req_json
                                            , predictions_model=predictions_model
                                            , frequency=frequency
                                            , frequency_q=frequency_q
                                            , merge=merge
                                            , forecast_offset = forecast_offset
                                            )

            elapsed_time = time.process_time() - t
            
            return ts_data, elapsed_time

        except (Exception) as e:
            self.log("error", TS_Data.get_data_from_timestream.__name__, f"Exception: {e}")
            return pd.DataFrame()

    def make_rest_response(self
                        , local_download
                        , df
                        , file_name
                        , extension
                        , s3_path
                        , s3_upload
                        , elapsed_time):
        
        if local_download:
            output = Utils.data_frame_to_bytes(df)
            output_bytes = output.getvalue().to_pybytes()

            self.log("info", f'{TS_Data.make_rest_response.__name__}', f'data_frame_to_bytes')
            self.log("info", f'{TS_Data.make_rest_response.__name__}', f'file_name: {file_name}')
            
            download_response = Response(
                output_bytes,
                mimetype='application/octet-stream',
                headers={'Content-Disposition': f'attachment; filename={file_name}.{extension}'}
            )

            download_response.headers.add('file_name', file_name)
            download_response.headers.add('shape_rows', str(df.shape[0]))
            download_response.headers.add('shape_columns', str(df.shape[1]))
            download_response.headers.add('CPU_execution_time', str(elapsed_time))
            download_response.headers.add('file_extension', f'{extension}')
            
            if s3_upload:
                download_response.headers.add('s3_uri', s3_path)

            return download_response
        else:
            response_data = {
                "code": 200,
                "file_name": file_name,
                "file_extension": "parquet",
                "shape": {
                    "rows": df.shape[0],
                    "columns": df.shape[1],
                    "nan": str(df.isna().sum().sum())
                },
                "CPU Execution time": elapsed_time,
            }

            if s3_upload:
                response_data["s3_uri"] = s3_path

            get_response = make_response(jsonify(response_data))

            # Send to Teams Webhook Channel
            data_webhook = "https://netorgft5402147.webhook.office.com/webhookb2/b5422ecc-2ce9-40d4-acf4-9162b6d0982e@488cd7c2-1e2a-48d8-9c88-1a3730d54468/IncomingWebhook/ddc571789a814c50a60aa16c8bca1d39/1464f7d9-e1ef-4633-9c25-df2dee3c45ba"
            _, send_message_webhook = Utils.send_message(data_webhook, f'{response_data["file_name"]}.{response_data["file_extension"]} uploaded to s3_uri: {response_data["s3_uri"]}')
            self.log("info", f'{TS_Data.make_rest_response.__name__}', f'send_message_webhook: {send_message_webhook}')

            return get_response

    def add_get_arguments(self, get_parser):
        get_parser.add_argument('customer', type=str, required=True, location='args')
        get_parser.add_argument('building', type=str, required=True, location='args')
        get_parser.add_argument('first_date', type=str, required=True, location='args')
        get_parser.add_argument('last_date', type=str, required=True, location='args')
        get_parser.add_argument('model', type=str, required=True, location='args')
        get_parser.add_argument('config', type=str, required=True, location='args')
        get_parser.add_argument('suffix', type=str, required=True, location='args')
        

        get_parser.add_argument('frequency', type=str, required=False, location='args')
        get_parser.add_argument('frequency_q', type=str, required=False, location='args')
        get_parser.add_argument('s3_uri', type=str, required=False, location='args')
        get_parser.add_argument('local_download', type=int, required=False, location='args')
        get_parser.add_argument('s3_upload', type=int, required=False, location='args')
        get_parser.add_argument('predictions_model', type=str, required=False, location='args')
        get_parser.add_argument('forecast_offset', type=str, required=False, location='args')
               
class S3_Data(Resource, Log):
    def get(self):
        try:
            self.log("info", S3_Data.get.__name__, f"{'*'*20} S3_Data {'*'*20}")
            
            self.log("info", S3_Data.get.__name__, f"{'*'*20}")

            try:
                self.log("info", f'{S3_Data.get.__name__}', f'client_ip: {request.remote_addr}')
            except (Exception) as e:
                self.log("error", f'{S3_Data.get.__name__}', f'Exception: {e}')

            try:
                self.log("info", f'{S3_Data.get.__name__}', f'client_ip proxy: {request.environ.get("HTTP_X_FORWARDED_FOR", request.remote_addr)}')
            except (Exception) as e:
                self.log("error", f'{S3_Data.get.__name__}', f'Exception: {e}')


            s3_handler = S3_Handler()
            get_parser = reqparse.RequestParser()

            self.add_get_arguments(get_parser)

            args = get_parser.parse_args()

            self.check_args(args)

            customer  = args['customer'].lower() if args['customer'] is not None else ""
            building  = args['building'].lower() if args['customer'] is not None else ""

            s3_uri  = args['s3_uri'].lower()
            file_name = args['file_name']
            extension = args['extension']
            
            t = time.process_time()

            local_save_path = f'{os.path.join("data")}'
            self.log("info", f'{S3_Data.get.__name__}', f' local_save_path: {local_save_path}')
            
            data_dir_path = os.path.join(os.getcwd(), "data")
            self.log("info", f'{S3_Data.get.__name__}', f' data_dir_path: {data_dir_path}')
            
            if(not os.path.exists(data_dir_path)):
                os.mkdir(data_dir_path)
                self.log("info", f'{S3_Data.get.__name__}', f' mkdir: {data_dir_path}')
            else:
                self.log("info", f'{S3_Data.get.__name__}', f' already exists: {data_dir_path}')

            if s3_handler.download_file(s3_uri = s3_uri
                                        , file_name = file_name
                                        , save_file_path = local_save_path
                                        , extension = extension):
                                        
                self.log("info", f'{S3_Data.get.__name__}', f' file {file_name} exists in: {s3_uri}')
                
                self.save_full_name = f"{file_name}.{extension}"
                self.save_full_path = os.path.join("data", self.save_full_name)
                self.elapsed_time = time.process_time() - t
                
                s3_get_response = self.make_rest_response(s3_uri, file_name, extension)
                
                return s3_get_response

            else:
                self.log("error", f'{S3_Data.get.__name__}', f' file {file_name} does not exists in: {s3_uri}')                
                return {'data': None}, 500

        except (Exception) as e:
            self.log("error", f'{S3_Data.get.__name__}', f' Exception: {e}')
            return {'data': None}, 500

    def check_args(self, args):

        try:
            required = ["s3_uri", "file_name", "extension"]
            for required_param in required:
                if args.get(required_param, None) is None:
                    self.log("error", S3_Data.check_args.__name__, f"param: {required_param} is required and not found in the request")
                    return False                    
        except (Exception) as e:
            self.log("error", S3_Data.check_args.__name__, f"s3_upload Exception: {e}")
            return False                  

        try:
            customer  = args['customer'].lower() if args['customer'] is not None else ""
            building  = args['building'].lower() if args['customer'] is not None else ""
            s3_uri  = args['s3_uri'].lower()
            file_name = args['file_name']
            extension = args['extension']

            self.log("info", S3_Data.check_args.__name__, f"customer: {customer}")
            self.log("info", S3_Data.check_args.__name__, f"building: {building}")
            self.log("info", S3_Data.check_args.__name__, f"s3_uri: {s3_uri}")
            self.log("info", S3_Data.check_args.__name__, f"file_name: {file_name}")
            self.log("info", S3_Data.check_args.__name__, f"extension: {extension}")

        except:
            self.log("error", S3_Data.check_args.__name__, f"Exception: {e}")
            return False

    def post(self): 
        try:
            self.log("info", S3_Data.post.__name__, f"{'*'*20} S3_Data POST {'*'*20}")
            
            t = time.process_time()
            s3_data = S3_Data()
            s3_handler = S3_Handler()

            self.log("info", S3_Data.post.__name__, f"S3_Handler")

            headers = request.headers

            self.log("info", S3_Data.post.__name__, f"headers")

            file_name = headers['file_name']
            extension = headers['extension']
            s3_uri = headers['s3_uri']

            self.log("info", S3_Data.post.__name__, f"get_data")
            self.log("info", S3_Data.post.__name__, f"request.content_length: {request.content_length}")
            self.log("info", S3_Data.post.__name__, f"request.content_type: {request.content_type}")
            self.log("info", S3_Data.post.__name__, f"request.content_encoding: {request.content_encoding}")
            
            data = request.get_data()

            self.log("info", S3_Data.post.__name__, f"AFTER get_data")

            local_path = os.path.join("data", f"{file_name}.{extension}")
            self.log("info", S3_Data.post.__name__, f"local_path: {local_path}" )

            if extension == 'parquet':
                df_post = Utils.bytes_to_data_frame(data)
                df_post.to_parquet(local_path)
                self.log("info", S3_Data.post.__name__, f"parquet saved in: {local_path}" )

            if extension in ['pkl', 'h5', 'png', 'pdf', 'json']:
                received_data = request.data  # Get the received data from the POST request
                self.log("info", S3_Data.post.__name__, f"extension: {extension}" )
                self.log("info", S3_Data.post.__name__, f"loaded model" )
    
                with open(local_path, 'wb') as file:
                    file.write(received_data)  # Write the received data to a file
                    self.log("info", S3_Data.post.__name__, f"dump model/file" )

            if extension == 'cfg':
                file_content = data.decode('utf-8')
                config = configparser.ConfigParser()
                config.read_string(file_content)
         
                with open(local_path, 'w') as configfile:
                    config.write(configfile)

                self.log("info", S3_Data.post.__name__, f"config file saved in: {local_path}" )

            self.s3_path = s3_uri
            self.s3_file_name = f"{file_name}"

            self.log("info", S3_Data.post.__name__, f"s3_path: {self.s3_path}" )
            self.log("info", S3_Data.post.__name__, f"s3_file_name: {self.s3_file_name}" )

            # Save to S3
            uploaded_response = s3_handler.upload_file(
                file=local_path, file_name=self.s3_file_name, s3_path=self.s3_path, extension=extension)
            if  uploaded_response:
                upload_s3_message = f"file saved in S3: {self.s3_path}"
                empty_data_folder_result, empty_data_folder_message = Utils.empty_data_folder()
                if empty_data_folder_result == False:
                    self.log("error", S3_Data.post.__name__, empty_data_folder_message)

            else: 
                upload_s3_message = f"Error trying to save upload file in S3"

            self.log("info", f'{S3_Data.post.__name__}', f' {upload_s3_message}')
            self.post_elapsed_time = time.process_time() - t   

            response = {"data": upload_s3_message}
            return response, 200
        except (Exception) as e:
            self.log("error", S3_Data.post.__name__, f"Exception: {e}" )
            return {"data": None} , 500

    def make_rest_response(self
                        , s3_uri
                        , file_name
                        , extension):

        s3_response = Response()
        try:
            if extension == 'parquet':
                if os.path.exists(self.save_full_path):
                    self.data = pd.read_parquet(self.save_full_path)
                    self.log("info", f'{S3_Data.make_rest_response.__name__}', f' save_full_path: {self.save_full_path}')
                    self.log("info", f'{S3_Data.make_rest_response.__name__}', f' read_parquet: {self.data.shape}')

                output = Utils.data_frame_to_bytes(self.data)
                output_bytes = output.getvalue().to_pybytes()

                s3_response.data = output_bytes
                s3_response.mimetype= 'application/octet-stream'
                s3_response.headers={'Content-Disposition': f'attachment; filename={file_name}.{extension}'}

            if extension in ('bin', 'h5', 'pkl'):
                if os.path.exists(self.save_full_path):
                    with open(self.save_full_path, 'rb') as file:
                        output_bytes = file.read()

                s3_response.data = output_bytes
                s3_response.mimetype = 'application/octet-stream'
                s3_response.headers={'Content-Disposition': f'attachment; filename={file_name}.{extension}'}

            if extension == 'cfg' or extension == 'json':
                if os.path.exists(self.save_full_path):
                    with open(self.save_full_path, 'rb') as file:
                        output_bytes = file.read()

                self.log("info", f'{S3_Data.make_rest_response.__name__}', f' file {file_name} read bytes: {len(output_bytes)}')

                s3_response.data = output_bytes
                s3_response.mimetype= 'text/plain'
                s3_response.headers={'Content-Disposition': f'attachment; filename={file_name}.{extension}'}

            self.log("info", f'{S3_Data.make_rest_response.__name__}', f' file_name: {file_name}')
            
            s3_response.headers['file_name'] = file_name
            s3_response.headers['s3_uri'] = s3_uri
            s3_response.headers['file_extension'] = f'{extension}'
            s3_response.headers['CPU_execution_time'] = str(self.elapsed_time)

            self.log("info", f'{S3_Data.make_rest_response.__name__}', f' s3_response headers: {s3_response.headers}')

            if extension == 'parquet':
                s3_response.headers['shape_rows'] = str(self.data.shape[0])
                s3_response.headers['shape_columns'] = str(self.data.shape[1])

            return s3_response

        except Exception as e:
            self.log("error", f'{S3_Data.make_rest_response.__name__}', f'Exception: {e}')
            s3_response.status = 500
            s3_response.data = e
            return s3_response
        
    def add_get_arguments(self, get_parser):
        get_parser.add_argument('s3_uri', type=str, required=True, location='args')
        get_parser.add_argument('file_name', type=str, required=True, location='args')
        get_parser.add_argument('extension', type=str, required=True, location='args')

        get_parser.add_argument('customer', type=str, required=False, location='args')
        get_parser.add_argument('building', type=str, required=False, location='args')

    @staticmethod
    def data_frame_to_bytes(data):
        table = pa.Table.from_pandas(data)
        output = pa.BufferOutputStream()
        pq.write_table(table, output)
        return output