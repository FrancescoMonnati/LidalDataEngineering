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
                   
        js = read_json_file("Environmental_Variables.json")
        try:                    
            os.environ["ADHERENCE_P001"] = js["ADHERENCE"]["P001"]
            os.environ["ADHERENCE_P002"] = js["ADHERENCE"]["P002"]
            os.environ["ADHERENCE_P003"] = js["ADHERENCE"]["P003"]
            os.environ["ADHERENCE_P004"] = js["ADHERENCE"]["P004"]
            os.environ["ADHERENCE_P005"] = js["ADHERENCE"]["P005"]
            os.environ["ADHERENCE_P006"] = js["ADHERENCE"]["P006"]
            #Threshold Cumulative
            os.environ["THRESH_CUM_P001"] = js["THRESH_CUM"]["P001"]    #Hoffman parameters for fragmented pollen season https://doi.org/10.1111/all.14153 
            os.environ["THRESH_CUM_P002"] = js["THRESH_CUM"]["P002"]
            os.environ["THRESH_CUM_P003"] = js["THRESH_CUM"]["P003"]
            os.environ["THRESH_CUM_P004"] = js["THRESH_CUM"]["P004"]
            os.environ["THRESH_CUM_P005"] = js["THRESH_CUM"]["P005"]
            os.environ["THRESH_CUM_P006"] = js["THRESH_CUM"]["P006"]
            #Threshold single day
            os.environ["THRESH_SINGLE_P001"] = js["THRESH_SINGLE"]["P001"]
            os.environ["THRESH_SINGLE_P002"] = js["THRESH_SINGLE"]["P002"]
            os.environ["THRESH_SINGLE_P003"] = js["THRESH_SINGLE"]["P003"]
            os.environ["THRESH_SINGLE_P004"] = js["THRESH_SINGLE"]["P004"]
            os.environ["THRESH_SINGLE_P005"] = js["THRESH_SINGLE"]["P005"]
            os.environ["THRESH_SINGLE_P006"] = js["THRESH_SINGLE"]["P006"]

            return 
        except Exception as e:
            raise Exception("{}".format(e.__class__.__name__) + ": " + str(e) + ". Error occured in get_environmental_variable method")
