import json
import re
import signal
from pathlib import Path

from llama_index.core import SimpleDirectoryReader


class SignalHandler:
    shutdown_requested = False


    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, *args):
        print('Request to shutdown received, stopping')
        self.shutdown_requested = True

    def can_run(self):
        return not self.shutdown_requested


# signal_handler = SignalHandler()

# while signal_handler.can_run():

# read the manifest file first 
# then read the directory 
import json
# Opening JSON file
f = open('../data/manifest.json')
 
# returns JSON object as 

data = json.load(f)
print(data.get("user_id"))
print(data.get("company_id"))
print(data.get("company_name"))
print(data.get("data_folder"))

file_path = "..data/" + data.get("data_folder")
reader = SimpleDirectoryReader(input_dir=file_path)




