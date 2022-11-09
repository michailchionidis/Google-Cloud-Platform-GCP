#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Function that creates a Cloud Scheduler job within your GCP project

"""
import pandas as pd
import time
import json
from google.cloud import scheduler
from google.cloud import pubsub
import random
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json" #Path to the service account file of your gcp project

def set_cloud_scheduler(arguments):
    
    hour = random.randrange(0,23,1) # Random integer from 0 to 23
    minute= random.randrange(0,59,1) # Random integer from 0 to 59

    client_name = arguments['client_name'].replace(" ", "_") # Set your own arguments when calling the function
    client_url = arguments['client_url'].replace(" ", "_") # Set your own arguments when calling the function

    print(arguments)
    print(client_name)
    topic_name = 'test' # Set the name of the Pub/Sub topic
    gcp_project_id = 'pfx-social-hero' # Set your gcp project id

    try:
       client = scheduler.CloudSchedulerClient()
       # create daily public posts retrieval job
       # runs every day
       msg = {'client_name': client_name, 'client_url':client_url}
       parent = f'projects/{gcp_project_id}/locations/europe-west2'

       job = {
         'pubsub_target': {
           'topic_name': f'projects/{gcp_project_id}/topics/{topic_name}',
           'data': json.dumps(msg).encode('utf8'),
           'attributes': {'client_name': client_name, 'client_url':client_url}
         },
         'description': f'Retrieve data for the following client: {client_name}', # Simple description message of the function that will be triggered
         'name': f'{parent}/jobs/retrieve_data-{client_name}', #Set the name of the Cloud Scheduler Job that will be created
         # Use a cron expression to schedule how often the job will be executed
         'schedule': f'{minute} {hour} 1 * *', #The parameters that we set mean that the job will be executed at a random minute, hour at the 1st day of each month
         'time_zone': 'Etc/UTC',
        }

       response = client.create_job(request={'parent': parent, 'job': job})
    except Exception as e:
        print(e)
        print('An error occurred! Could not create a Cloud Scheduler Job')
        return 'fail'
        
    print('Cloud Scheduler job was created successfully')
    return 'success'
        
if __name__ == '__main__':
    arguments = {}
    arguments['client_name'] = 'google'
    arguments['client_url'] = 'www.google.com'
    set_cloud_scheduler(arguments)
    
