#!/usr/bin/env python3

# Copyright (C) DATADVANCE, 2010-2021

"""A REST client for pSeven Enterprise workflow.

REST client which illustrates communication between a REST client
and the workflow. script works in an assumption that all input parameters
are defined in input parameters pane of workflow. If user change names or
types of variables in "input" dictionary, he must manually make the
same changes in input parameters pane.

"""

import json
import logging
import sys
import time
import uuid
import os
import backoff
import requests


# Configuring logging.
LOG = logging.getLogger()
LOG.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
HANDLER = logging.StreamHandler(sys.stdout)
HANDLER.setLevel(logging.DEBUG)
HANDLER.setFormatter(FORMATTER)
LOG.addHandler(HANDLER)

# Disable logging from 'urllib3'.
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

# Public configuration.
# https://pseven.online/pseven/.rest/v1/workflows/b5a2288a131c4dfa91cb17a90e806f2e
# '266c055755db472db90699789683d5ed'

WF_ID = 'da15564cdab546b8b518fafc038330c4'
API_TOKEN = '28059c2980a9e8051cc013673da60126544a2250'

PSEVEN_URL = "http://pseven.online"
WORKFLOW_LIST_URL = f"{PSEVEN_URL}/pseven/.rest/v1/workflows/"

# backoff decorator is used to retry when an exception is raised
# max_time specifies the maximum amount of total time in seconds that can elapse before giving up
# max_value - max time between retries
@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=600,
                      max_value=10,
                      jitter=None,
                      logger=LOG)
def ensure_success(response):
    """
    Raise an exception if HTTP response has a error code.
    """
    response.raise_for_status()
    return response

def update_run_parametrs(run_info, inputs):
    """
    Update inputs in a run configuration
    """
    run_info_new = run_info
    for input_name, input_value in inputs.items():
        for param_ind, param_info in enumerate(run_info):
            if param_info["name"] == input_name:
                if param_info["schemas"][0]["@type"] == "List":
                    if run_info_new[param_ind]["value"] == None:
                        raise Exception(f"'{input_name}' is not defined in input parameters panel")
                    if len(input_value) != len(run_info_new[param_ind]["value"]["value"]):
                        raise Exception(f"Lenght of the '{input_name}' is greater than the lenght of the parameter in input parameters panel")
                    for value_ind, value in enumerate(input_value):
                        run_info_new[param_ind]["value"]["value"][value_ind]["value"] = value
                elif param_info["schemas"][0]["@type"] == "Dictionary":
                    if run_info_new[param_ind]["value"] == None:
                        raise Exception(f"'{input_name}' is not defined in input parameters panel")
                    if len(input_value) != len(run_info_new[param_ind]["value"]["value"]):
                        raise Exception(f"Lenght of the '{input_name}' is greater than the lenght of the parameter in input parameters panel")
                    value_ind = 0
                    for value_name, value in input_value.items():
                        run_info_new[param_ind]["value"]["value"][value_ind]["key"] = value_name
                        run_info_new[param_ind]["value"]["value"][value_ind]["value"]["value"] = input_value[value_name]
                        value_ind = value_ind + 1
                else:
                    run_info_new[param_ind]["value"]["value"] = input_value
    return run_info_new

def get_request_headers():
    access_token = API_TOKEN
    headers = {
        "Authorization": f"Token {access_token}",
        "content-type": "application/json",
    }
    LOG.debug("Got access token: %s", access_token)
    return headers

def get_or_start_workflow_run(headers, inputs, upload_files, upload_directories):
    """
    Start workflow with specific input values, return url to workflow run.
    """
    # Trying to find a workflow run instance first.
    LOG.debug("Getting workflow by ID '%s'...", WF_ID)
    response = ensure_success(requests.get(WORKFLOW_LIST_URL, headers=headers))
    workflow_url = next(
        (workflow for workflow in response.json() if workflow["id"] == WF_ID),
        None,
    )["url"]
    if workflow_url == None:
        raise Exception("Workflow not found")
    
    workflowruns_url = workflow_url + "runs/"
    
    # Creating a new workflow run.
    LOG.debug("Creating a new workflow run...")
    response = ensure_success(requests.post(workflowruns_url, headers=headers))
    workflowrun_url = response.json()["url"]

    # Waiting for workflow run initialization.
    LOG.debug("Waiting for run 'CONFIGURATION' state...")
    while True:
        response = requests.get(workflowrun_url, headers=headers)
        LOG.debug("Pulling REST API...(%s)", response.json()["state"])
        if response.json()["state"] == "CONFIGURATION":
            break
        # out_values = get_output_values_from_results(response_data['results'], outputs)

        time.sleep(.1)

    # Get run details.
    response = requests.get(workflowrun_url, headers=headers)
    run_info = response.json()
    # Change parameter values in run configuration.
    run_info_new = run_info
    run_info_new['parameters'] = update_run_parametrs(run_info['parameters'], inputs)
    # Send changes to workflowRun.
    response = ensure_success(requests.patch(workflowrun_url, headers=headers, data=json.dumps(run_info_new)))
    # Upload files
    file_upload(upload_files, headers, workflowrun_url);
    directory_upload(upload_directories, headers, workflowrun_url);
    # Start the run.
    ensure_success(requests.post(workflowrun_url + "run/", headers=headers))

    # Wait until the run status is RUNNING
    while True:
        response = requests.get(workflowrun_url, headers=headers)
        LOG.debug("Pulling REST API...(%s)", response.json()["state"])
        if response.json()["state"] == "RUNNING":
            break
        time.sleep(2)

    return workflowrun_url

def get_output_values_from_results(data, out_ports):
    '''
    Read output values from data
    '''
    outputs = {}
    for port in out_ports:
        port_info = next(filter(lambda x: x["name"] == port, data), None)
        if port_info == None:
            outputs[port] = np.nan
        elif port_info['value']['properties']['@schema']['@type'] == 'List':
            outputs[port] = [port_info['value']['value'][i]['value'] for i in range(len(port_info['value']['value']))]
        elif port_info['value']['properties']['@schema']['@type'] == 'Dictionary':
        	outputs[port] = {port_info['value']['value'][i]['key']: port_info['value']['value'][i]['value']['value'] \
                for i in range(len(port_info['value']['value']))}		
        else:
            outputs[port] = port_info['value']['value']
    return outputs

def file_upload(upload_files, headers, run_url_str):
    headers_only_auth = {'Authorization': headers['Authorization']}
    run_upload_str = run_url_str + "upload/"
    for file_obj in upload_files:
        files_data = {
        'destination': (None, os.path.dirname(file_obj[1])),
        'file': (os.path.basename(file_obj[1]), open(file_obj[0], 'rb')),
        }
        response = requests.post(run_upload_str, files=files_data,headers = headers_only_auth)
        if (response.status_code == 200):
            LOG.debug("File " + str(file_obj[1]) + " was uploaded successfully!")
        else:
            LOG.debug("File " + str(file_obj[1]) + " was not uploaded!")
            LOG.debug("Response code: " + str(response.status_code) )

def directory_upload(upload_directories, headers, run_url_str):
    headers_only_auth = {'Authorization': headers['Authorization']}
    run_upload_str = run_url_str + "upload/"
    for dir_obj in upload_directories:
        dir_data = {
        'directory': (None, dir_obj)
        }
        response = requests.post(run_upload_str, files=dir_data,headers = headers_only_auth)
        if (response.status_code == 200):
            LOG.debug("Directory " + str(dir_obj) + " was uploaded successfully!")
        else:
            LOG.debug("Directory " + str(dir_obj) + " was not uploaded!")
            LOG.debug("Response code: " + str(response.status_code) )

def main():
    """
    Run prepared workflow with custom run parameters,
    process design points response requests, and get a result.
    """

    # Specify input values.
    inputs = {
        "a" : 8,
        "b" : 5,
        "c" : 10,
        "Бюджет" : 15
    }
    # Specify names of outputs.
    outputs = ["Модель.x", "Модель.y"]

    # Specify path to files (relative to a run directory), to be downloaded after workflow finishes.
    # If file content is a text, client outputs it to stdout.
    report_file_paths = []

    # Specify the list of files to be uploaded to the p7Ent:
    # first value in each sub-list - global path to the file on the user's machine, which you want to upload
    # second values in each sub-list - relative (to the current "Run" directory) destination paths in the p7Ent
    # Name of the uploaded file should also be specified!
    # In the example below testfile.txt file, located on the user's PC, would be uploaded
    # as first_file.dat to the .../rel_dir1/rel_dir2/ directory.
    # (where ... is https://pseven.online/pseven/.files/Users/<User>/<Wf_name>.p7wf/@Runs/#<p7runNum>p7run)
    # If specified relative directory doesn't exist it would be created.
    # All files will be uploaded to the current RUN directory (RUN should be in CONFIGURATION mode!)
    uploaded_files_paths = [["A.txt", "A.txt"]]

    # Specify the list of directories to be uploaded to the p7Ent
    # All specified user's directories will be uploaded to the current RUN directory in the p7Ent
    # All user's files inside the directories apecified list below won't be uploaded to the p7Ent:
    # this method is used for preparation of empty directories inside the current RUN directory
    uploaded_directories_paths = []



    # Get authorization headers for subsequent requests.
    headers = get_request_headers()
    # Start workflow with specific input values, return url to workflow run.
    workflowrun_url = get_or_start_workflow_run(headers, inputs, uploaded_files_paths, uploaded_directories_paths)

    LOG.debug("Waiting until running finish...")
    # Extract values from results.
    while True:
        # Stop pull the platfrom if workflow run is finished.
        # (success, fail)
        response = ensure_success(requests.get(workflowrun_url, headers=headers))
        response_data = response.json()
        if response_data["state"] in ["FINISHED", "FAILED", "INTERRUPTED"]:
            LOG.info(f"Optimization state {response_data['state']}")
            if response_data["state"] == "FAILED":
                LOG.info("Workflow has failed")
            else:
                # Get results from output ports.
                time.sleep(30) # Waiting for json results generating
                response = ensure_success(requests.get(workflowrun_url, headers=headers))
                response_data = response.json()
                out_values = get_output_values_from_results(response_data['results'], outputs)
            break

    # Write the results to stdout.
    LOG.info("Run results:")
    for name, value in out_values.items():
        LOG.info(f"{name} = {value}")

    # Download a files located in "report_file_paths". Output text content to stdout.
    for path in report_file_paths:
    	response = ensure_success(requests.get(workflowrun_url + 'download/?file=' + path, headers=headers))
    	if response.headers['content-type'] == 'text/plain':
    		LOG.info("Get report file:\n(%s)", response.text)
    	with open(os.path.basename(path), 'wb') as f:
    		f.write(response.content)

if __name__ == "__main__":
    main()