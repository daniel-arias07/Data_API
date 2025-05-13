import json
import os
import sys
import logging
from flask import Flask
from routes import api_bp

logging.basicConfig(filename="data_api_log.txt"
                    , level=logging.INFO
                    , format='%(asctime)s | %(levelname)s | %(message)s'
                    , datefmt='%d/%m/%Y %H:%M:%S'
                    , filemode="a")

app = Flask(__name__)
# app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.INFO)

# Register Blueprint
app.register_blueprint(api_bp, url_prefix='/api/data_api')

if __name__ == '__main__':
    with open(os.path.join("config","config.json")) as file:
        config_json_file = json.load(file)
    
    app.run(debug=False
            , host=config_json_file["host"]
            , port=config_json_file["port"])
