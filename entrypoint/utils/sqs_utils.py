# type: ignore
import boto3
import json
import traceback
from typing import List, Dict, Union
from botocore.exceptions import ConnectTimeoutError, ClientError

from config import EVENTS_SQS, INTERACTION_SQS, AWS_CONFIGS, ENV, TEAMS_EVENTS_SQS, TEAMS_INTERACTION_SQS

import uuid


class EventsQueueProducer:

    def __init__(self):
        self.get_sqs_session()

    def __obtain_sts_creds__(self):
        """Uses aws security token service service to get token with limited privilages for a limited time
        """
        self.sts_client = boto3.client(
            "sts",
            aws_access_key_id=AWS_CONFIGS.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_CONFIGS.AWS_SECRET_ACCESS_KEY)

        self.assumed_role = self.sts_client.assume_role(
            RoleArn=AWS_CONFIGS.QUEUE_ROLE_ARN,
            RoleSessionName=EVENTS_SQS.QUEUE_ROLE_SESSION)

        self.creds = self.assumed_role.get("Credentials")

    def get_sqs_session(self):
        """Based on the ENV_NAME initializes the client for the sqs"""
        if ENV.ENV_NAME in ["STG", "CLIENT"]:
            self.__obtain_sts_creds__()
            resource_params = dict(
                aws_access_key_id=self.creds.get("AccessKeyId"),
                aws_secret_access_key=self.creds.get("SecretAccessKey"),
                aws_session_token=self.creds.get("SessionToken"),
                region_name=AWS_CONFIGS.REGION)
            self.sqs_res = boto3.client('sqs', **resource_params)

        else:
            self.sqs_res = boto3.client("sqs",
                                        endpoint_url=EVENTS_SQS.ENDPOINT_URL,
                                        region_name="")

    def produce(self, payload: Union[List, Dict]):
        # print("QUEUE: ", EVENTS_SQS.QUEUE_URL)
        try:
            response = self.sqs_res.send_message(
                QueueUrl=EVENTS_SQS.QUEUE_URL,
                MessageBody=json.dumps(payload),
                MessageGroupId="event-group",
                MessageDeduplicationId=str(uuid.uuid4()))
            return response
        except ClientError as cerr:
            print(f'EVENT PRODUCER CLIENT ERROR: {str(cerr)}')
            print(
                f'EVENT PRODUCER CLIENT ERROR TRACEBACK: {traceback.format_exc()}'
            )
            print(f'CREATING EVENT PRODUCER QUEUE CLIENT AGAIN')
            self.get_sqs_session()
            print(f'CREATED EVENT PRODUCER QUEUE CLIENT AGAIN')
            print(f'TRYING TO PRODUCE THE EVENT MESSAGE AGAIN TO THE QUEUE')
            response = self.produce(payload)
            print(f'PRODUCED THE EVENT MESSAGE AGAIN TO THE QUEUE')

        except Exception as err:
            print(f'EXCEPTION IN PRODUCING EVENT MESSAGE: {str(err)}')
            print(
                f'EXCEPTION IN PRODUCING EVENT MESSAGE TRACEBACK: {traceback.format_exc()}'
            )
            return None


class InteractionsQueueProducer:

    def __init__(self):
        self.get_sqs_session()

    def __obtain_sts_creds__(self):
        """Uses aws security token service service to get token with limited privilages for a limited time
        """
        self.sts_client = boto3.client(
            "sts",
            aws_access_key_id=AWS_CONFIGS.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_CONFIGS.AWS_SECRET_ACCESS_KEY)

        self.assumed_role = self.sts_client.assume_role(
            RoleArn=AWS_CONFIGS.QUEUE_ROLE_ARN,
            RoleSessionName=INTERACTION_SQS.QUEUE_ROLE_SESSION)

        self.creds = self.assumed_role.get("Credentials")

    def get_sqs_session(self):
        """Based on the ENV_NAME initializes the client for the sqs"""
        if ENV.ENV_NAME in ["STG", "CLIENT"]:
            self.__obtain_sts_creds__()
            resource_params = dict(
                aws_access_key_id=self.creds.get("AccessKeyId"),
                aws_secret_access_key=self.creds.get("SecretAccessKey"),
                aws_session_token=self.creds.get("SessionToken"),
                region_name=AWS_CONFIGS.REGION)
            self.sqs_res = boto3.client('sqs', **resource_params)

        else:
            self.sqs_res = boto3.client(
                "sqs",
                endpoint_url=INTERACTION_SQS.ENDPOINT_URL,
                region_name="")

    def produce(self, payload: Union[List, Dict]):

        try:
            response = self.sqs_res.send_message(
                QueueUrl=INTERACTION_SQS.QUEUE_URL,
                MessageBody=json.dumps(payload),
                MessageGroupId="interaction-group",
                MessageDeduplicationId=str(uuid.uuid4()))
            return response
        except ClientError as cerr:
            print(f'INTERACTION PRODUCER CLIENT ERROR: {str(cerr)}')
            print(
                f'INTERACTION PRODUCER CLIENT ERROR TRACEBACK: {traceback.format_exc()}'
            )
            print(f'CREATING INTERACTION PRODUCER QUEUE CLIENT AGAIN')
            self.get_sqs_session()
            print(f'CREATED INTERACTION PRODUCER QUEUE CLIENT AGAIN')
            print(
                f'TRYING TO PRODUCE THE INTERACTION MESSAGE AGAIN TO THE QUEUE'
            )
            response = self.produce(payload)
            print(f'PRODUCED THE INTERACTION MESSAGE AGAIN TO THE QUEUE')

        except Exception as err:
            print(f'EXCEPTION IN PRODUCING INTERACTION MESSAGE: {str(err)}')
            print(
                f'EXCEPTION IN PRODUCING INTERACTION MESSAGE TRACEBACK: {traceback.format_exc()}'
            )
            return None


class TeamsEventsQueueProducer:

    def __init__(self):
        self.get_sqs_session()

    def __obtain_sts_creds__(self):
        """Uses aws security token service service to get token with limited privilages for a limited time
        """
        self.sts_client = boto3.client(
            "sts",
            aws_access_key_id=AWS_CONFIGS.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_CONFIGS.AWS_SECRET_ACCESS_KEY)

        self.assumed_role = self.sts_client.assume_role(
            RoleArn=AWS_CONFIGS.QUEUE_ROLE_ARN,
            RoleSessionName=TEAMS_EVENTS_SQS.QUEUE_ROLE_SESSION)

        self.creds = self.assumed_role.get("Credentials")

    def get_sqs_session(self):
        """Based on the ENV_NAME initializes the client for the sqs"""
        if ENV.ENV_NAME in ["STG", "CLIENT"]:
            self.__obtain_sts_creds__()
            resource_params = dict(
                aws_access_key_id=self.creds.get("AccessKeyId"),
                aws_secret_access_key=self.creds.get("SecretAccessKey"),
                aws_session_token=self.creds.get("SessionToken"),
                region_name=AWS_CONFIGS.REGION)
            self.sqs_res = boto3.client('sqs', **resource_params)

        else:
            self.sqs_res = boto3.client(
                "sqs",
                endpoint_url=TEAMS_EVENTS_SQS.ENDPOINT_URL,
                region_name="")

    def produce(self, payload: Union[List, Dict]):
        # print("QUEUE: ", TEAMS_EVENTS_SQS.QUEUE_URL)
        try:
            response = self.sqs_res.send_message(
                QueueUrl=TEAMS_EVENTS_SQS.QUEUE_URL,
                MessageBody=json.dumps(payload),
                MessageGroupId="teams-event-group",
                MessageDeduplicationId=str(uuid.uuid4()))
            return response
        except ClientError as cerr:
            print(f'EVENT PRODUCER CLIENT ERROR: {str(cerr)}')
            print(
                f'EVENT PRODUCER CLIENT ERROR TRACEBACK: {traceback.format_exc()}'
            )
            print(f'CREATING EVENT PRODUCER QUEUE CLIENT AGAIN')
            self.get_sqs_session()
            print(f'CREATED EVENT PRODUCER QUEUE CLIENT AGAIN')
            print(f'TRYING TO PRODUCE THE EVENT MESSAGE AGAIN TO THE QUEUE')
            response = self.produce(payload)
            print(f'PRODUCED THE EVENT MESSAGE AGAIN TO THE QUEUE')

        except Exception as err:
            print(f'EXCEPTION IN PRODUCING EVENT MESSAGE: {str(err)}')
            print(
                f'EXCEPTION IN PRODUCING EVENT MESSAGE TRACEBACK: {traceback.format_exc()}'
            )
            return None


class TeamsInteractionsQueueProducer:

    def __init__(self):
        self.get_sqs_session()

    def __obtain_sts_creds__(self):
        """Uses aws security token service service to get token with limited privilages for a limited time
        """
        self.sts_client = boto3.client(
            "sts",
            aws_access_key_id=AWS_CONFIGS.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_CONFIGS.AWS_SECRET_ACCESS_KEY)

        self.assumed_role = self.sts_client.assume_role(
            RoleArn=AWS_CONFIGS.QUEUE_ROLE_ARN,
            RoleSessionName=TEAMS_INTERACTION_SQS.QUEUE_ROLE_SESSION)

        self.creds = self.assumed_role.get("Credentials")

    def get_sqs_session(self):
        """Based on the ENV_NAME initializes the client for the sqs"""
        if ENV.ENV_NAME in ["STG", "CLIENT"]:
            self.__obtain_sts_creds__()
            resource_params = dict(
                aws_access_key_id=self.creds.get("AccessKeyId"),
                aws_secret_access_key=self.creds.get("SecretAccessKey"),
                aws_session_token=self.creds.get("SessionToken"),
                region_name=AWS_CONFIGS.REGION)
            self.sqs_res = boto3.client('sqs', **resource_params)

        else:
            self.sqs_res = boto3.client(
                "sqs",
                endpoint_url=TEAMS_INTERACTION_SQS.ENDPOINT_URL,
                region_name="")

    def produce(self, payload: Union[List, Dict]):

        try:
            response = self.sqs_res.send_message(
                QueueUrl=TEAMS_INTERACTION_SQS.QUEUE_URL,
                MessageBody=json.dumps(payload),
                MessageGroupId="teams-interaction-group",
                MessageDeduplicationId=str(uuid.uuid4()))
            return response
        except ClientError as cerr:
            print(f'INTERACTION PRODUCER CLIENT ERROR: {str(cerr)}')
            print(
                f'INTERACTION PRODUCER CLIENT ERROR TRACEBACK: {traceback.format_exc()}'
            )
            print(f'CREATING INTERACTION PRODUCER QUEUE CLIENT AGAIN')
            self.get_sqs_session()
            print(f'CREATED INTERACTION PRODUCER QUEUE CLIENT AGAIN')
            print(
                f'TRYING TO PRODUCE THE INTERACTION MESSAGE AGAIN TO THE QUEUE'
            )
            response = self.produce(payload)
            print(f'PRODUCED THE INTERACTION MESSAGE AGAIN TO THE QUEUE')

        except Exception as err:
            print(f'EXCEPTION IN PRODUCING INTERACTION MESSAGE: {str(err)}')
            print(
                f'EXCEPTION IN PRODUCING INTERACTION MESSAGE TRACEBACK: {traceback.format_exc()}'
            )
            return None
