import json
import os
import sys

json_file_path = os.getcwd() + "/oozie-repository/coordinators/projectname/appname/ParentSchedule.json"
# pig = "oozie-repository/workflows/projectname/appname/Pig/pigscripts"
# hive = "oozie-repository/workflows/projectname/appname/hive/hivescripts"


''' 
I'll parse the coordinator json and will return the hive, pig script path in dictionary
'''
def parse_json_object(data):
    workflow_dict = {}
    for items in data:
        data_item = items["fields"]["data"]
        try:
            decoded = json.loads(data_item)
            for item in decoded['workflow']['nodes']:
                if item['type'] == "pig-widget":
                    print("pig script path is: %s" %(item['properties']['script_path']))
                    workflow_dict['pig'] = str(item['properties']['script_path'])
                elif item['type'] == "hive-widget":
                    print("hive script path is: %s" %(item['properties']['script_path']))
                    workflow_dict['hive'] = str(item['properties']['script_path'])
                else:
                    continue
        except(ValueError, KeyError, TypeError):
            print("[Warning] in coordinator JSON")
    return workflow_dict


''' 
I'll return full path of workflow depending upon key
'''
def fetch_repo_path(key):
    fetch_azure_devops_variable = os.environ[key]
    if fetch_azure_devops_variable:
        return fetch_azure_devops_variable
    else:
        return False

''' 
I'll check whether the file is present on the required directory or not
'''
def get_file_name(final_dict):
    key_list = final_dict.keys()
    for key_item in key_list:
        pig_file_name = os.path.basename(final_dict[key_item])
        pig_repository_path =  fetch_repo_path(key_item)
        if pig_repository_path:
            pig_full_path =  os.getcwd()+ "/" + pig_repository_path + "/" + pig_file_name
            exists = os.path.isfile(pig_full_path)
            if exists:
                print("The workflow of %s exist in the repository" %(key_item))
            else:
                print("The workflow of %s does not exist in the repository so exiting" %(key_item))
                sys.exit(1)


def get_full_path(file_name, path):
    full_path = os.getcwd()+ "/" + path + "/" + file_name
    return full_path

''' 
I'll check whether the file is present on the required directory or not
'''
def check_workflow_exist_or_not(final_dict, pig_path, hive_path):
    key_list = final_dict.keys()
    for key_item in key_list:
        if key_item == "pig":
            pig_file_name = os.path.basename(final_dict[key_item])
            path = get_full_path(pig_file_name, pig_path)
        elif key_item == "hive":
            hive_file_name = os.path.basename(final_dict[key_item])
            path = get_full_path(hive_file_name, hive_path)
        else:
            continue
        exists = os.path.isfile(path)
        if exists:
            print("The workflow of %s exist in the repository" %(key_item))
        else:
            print("The workflow of %s does not exist in the repository so exiting" %(key_item))
            sys.exit(1)

def main():
    print('Going to parse json')
    pig_path =  sys.argv[1]
    hive_path = sys.argv[2]
    if len(sys.argv) < 3:
        print("This script accept two arguments: 1. pig-path 2. hive-path")
    else:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
            final_dict = parse_json_object(data)
            print("Consolidated dict is: %s" %(final_dict))
            check_workflow_exist_or_not(final_dict, pig_path, hive_path)

if __name__ == '__main__':
    main()
