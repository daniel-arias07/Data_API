from flask import current_app
import pandas as pd

class Log:
    def log(self, log_level, method_name, message):
        pd.set_option('display.max_columns', None)
        
        with current_app.app_context():
            log_func = getattr(current_app.logger, log_level)
            log_func(f"{self.__class__.__name__} | {method_name} | {message}")
            print(f"{self.__class__.__name__} | {method_name} | {message}")
