import boto3
import urllib3
import logging
import json
from datetime import date, timedelta
import secrets_manager

# logger
logger = logging.getLogger(__name__)

# define date for lambda
TODAY             = date.today()
YESTERDAY         = date.today() - timedelta(days=1)
FIRSTDAY_OF_MONTH = YESTERDAY.replace(day=1)

# main handler
def lambda_handler(event, context):
    dailyReport = Report.transform_to_slack_field(
        # ce_services_report = CostExplorerQuery.group_by_service(granularity="DAILY"),
        ce_account_report  = CostExplorerQuery.group_by_account(granularity="DAILY"),
        ce_total_report    = CostExplorerQuery.total(granularity="DAILY"),)
    monthlyReport = Report.transform_to_slack_field(
        # ce_services_report = CostExplorerQuery.group_by_service(granularity="MONTHLY"),
        ce_account_report  = CostExplorerQuery.group_by_account(granularity="MONTHLY"),
        ce_total_report    = CostExplorerQuery.total(granularity="MONTHLY"),)
    # send to slack
    SlackBot.send_report(dailyReport, monthlyReport)


class Report:

    @staticmethod
    def transform_to_slack_field(ce_account_report, ce_total_report):
        try:
            # parse cost from report
            accounts = Organizations.list_accounts()
            costInfo = {
                accounts[group['Keys'][0]]: float(group['Metrics']['UnblendedCost']['Amount'])
                for group in ce_account_report['ResultsByTime'][0]['Groups']
                    if '0.00' != f"{float(group['Metrics']['UnblendedCost']['Amount']):.2f}"
            }
            # cost per service
            buffer = [
                {
                    "title": key,
                    "value": f"$ {costInfo[key]:.2f}"
                }
                for key in sorted(costInfo, key=costInfo.get, reverse=True)
            ]
            # total cost
            buffer.insert(0,
                {
                    "title": "Total",
                    "value": f"$ {float(ce_total_report['ResultsByTime'][0]['Total']['UnblendedCost']['Amount']):.2f}"
                }
            )
            return buffer
        except Exception as error:
            logger.info("Failed transform report to slack field.")
            logger.exception(error)



class SlackBot:

    @staticmethod
    def send_report(dailyReport, monthlyReport):
        try:
            # build URL
            http = urllib3.PoolManager()
            # get secret from secrets manager
            secrets = secrets_manager.get_secret()
            slack_webhooking_url = secrets["webhooking_url"]
            # define headers
            header = {"Content-Type": "application/json"}
            # define body message
            body_message = {
                "channel": "#aws-cost",
                "username": "aws",
                "icon_emoji": ":aws:",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "AWS Cost Report",
                            "emoji": True
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "plain_text",
                            "text": "The summary of cost and usage per account in yesterday and this month.",
                            "emoji": True
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "For more detail, see cost explorer within organizations root account."
                        },
                        "accessory": {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "Go to cost explorer",
                                "emoji": True
                            },
                            "value": "click_me_123",
                            "url": "https://console.aws.amazon.com/cost-management/home#",
                            "action_id": "button-action"
                        }
                    }
                ],
                "attachments": [
                    {
                        "mrkdwn_in": ["text"],
                        "color": "#7CD197",
                        "title": f"{YESTERDAY} report",
                        "fields": dailyReport
                    },
                    {
                        "mrkdwn_in": ["text"],
                        "color": "#7CD197",
                        "title": f"{FIRSTDAY_OF_MONTH} ~ {YESTERDAY} report",
                        "fields": monthlyReport
                    }
                ]
            }
            # HTTP POST to "slack incoming webhooking URL"
            response = http.request("POST",
                url = slack_webhooking_url,
                body = json.dumps(body_message),
                headers = header,)
        except Exception as error:
            logger.info("Failed send message to slack channel.")
            logger.exception(error)
        else:
            logger.info("Successfully send cost report to slack channel.")

class CostExplorerQuery:

    @staticmethod
    def group_by_service(granularity):
        try:
            # define Cost Explorer
            client = boto3.client('ce')
            # type check and init start time
            if granularity == "DAILY": 
                start_time = YESTERDAY.strftime('%Y-%m-%d')
            elif granularity == "MONTHLY":
                start_time = FIRSTDAY_OF_MONTH.strftime('%Y-%m-%d')
            else:
                raise Exception("granularity should be 'DAILY' or 'MONTHLY'")
            # Cost and usage report's query statement
            query = {
                "TimePeriod": {
                    "Start": start_time,
                    "End": TODAY.strftime('%Y-%m-%d'),
                },
                "Granularity": granularity,
                "Metrics": ["UnblendedCost"],
                "GroupBy": [
                    {
                        "Type": "DIMENSION",
                        "Key": "SERVICE",
                    },
                ],
            }
            # create report and return it
            return client.get_cost_and_usage(**query)
        except Exception as error:
            logger.info("Failed CostExplorer Query group by service.")
            logger.exception(error)

    @staticmethod
    def group_by_account(granularity):
        try:
            # Init Cost Explorer
            client = boto3.client('ce')
            # type check and init start time
            if granularity == "DAILY": 
                start_time = YESTERDAY.strftime('%Y-%m-%d')
            elif granularity == "MONTHLY":
                start_time = FIRSTDAY_OF_MONTH.strftime('%Y-%m-%d')
            else:
                raise Exception("granularity should be 'DAILY' or 'MONTHLY'")
            # Cost and usage report's query statement
            query = {
                "TimePeriod": {
                    "Start": start_time,
                    "End": TODAY.strftime('%Y-%m-%d'),
                },
                "Granularity": granularity,
                "Metrics": ["UnblendedCost"],
                "GroupBy": [
                    {
                        "Type": "DIMENSION",
                        "Key": "LINKED_ACCOUNT",
                    },
                ],
            }
            # create report and return it
            return client.get_cost_and_usage(**query)
        except Exception as error:
            logger.info("Failed CostExplorer Query group by account.")
            logger.exception(error)


    @staticmethod
    def total(granularity):
        try:
            # Init Cost Explorer
            client = boto3.client('ce')
            # type check and init start time
            if granularity == "DAILY": 
                start_time = YESTERDAY.strftime('%Y-%m-%d')
            elif granularity == "MONTHLY":
                start_time = FIRSTDAY_OF_MONTH.strftime('%Y-%m-%d')
            else:
                raise Exception("granularity should be 'DAILY' or 'MONTHLY'")
            # Cost and usage report's query statement
            query = {
                "TimePeriod": {
                    "Start": start_time,
                    "End": TODAY.strftime('%Y-%m-%d'),
                },
                "Granularity": granularity,
                "Metrics": ["UnblendedCost"],
            }
            # create report and return it
            return client.get_cost_and_usage(**query)
        except Exception as error:
            logger.info("Failed CostExplorer Query total.")
            logger.exception(error)


class Organizations:

    @staticmethod
    def list_accounts():
        try:
            client = boto3.client('organizations')
            response = client.list_accounts()
            return {
                account["Id"]: account["Email"]
                for account in response["Accounts"]
            }
        except Exception as error:
            logger.info("Failed Organizations list accounts.")
            logger.exception(error)