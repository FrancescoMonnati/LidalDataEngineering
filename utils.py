import json
import os

def read_json_file(file_path):
        try:        
            with open(file_path, 'r') as file:
                js = json.load(file)
                return js
        except Exception as e:
            raise Exception("{}".format(e.__class__.__name__) + ": " + str(e))
        


def get_environmental_variable(file_path):
        path = os.getcwd()         
        js = read_json_file(path, "Environmental_Variables.json")
        
        try: 
            os.environ["smtp_server"] = js["smtp_server"]                    
            os.environ["sender_email_address"] = js["sender_email_address"]
            os.environ["port"] = js["port"]
            os.environ["password"] = js["password"]
            os.environ["receiver_email_address"] = js["receiver_email_address"]

            return 
        except Exception as e:
            raise Exception("{}".format(e.__class__.__name__) + ": " + str(e) + ". Error occured in get_environmental_variable method")
