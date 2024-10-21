import json

def save_expectations(expectation_suite, file_name:str):
    with open(file_name+".json", "w") as my_file:
        my_file.write(
            json.dumps(expectation_suite.to_json_dict(), sort_keys=True, indent=4)
        )