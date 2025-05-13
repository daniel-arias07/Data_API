import shutil
import os

shutil.rmtree(os.path.join(os.getcwd(), 'data'), ignore_errors=True)
os.mkdir(os.path.join(os.getcwd(), 'data'))
