"""
EC2 Instance Scheduler Lambda Function
Schedules clinic API instance to run 7 AM - 9 PM Central Time daily
"""
import boto3
import os
from datetime import datetime
import pytz

ec2 = boto3.client('ec2')
central_tz = pytz.timezone('America/Chicago')

def lambda_handler(event, context):
    """
    Start or stop EC2 instances based on schedule.
    
    If 'action' is provided in event, use it directly.
    Otherwise, check current Central Time and decide:
    - 7 AM - 9 PM Central: Ensure instances are running
    - Outside hours: Ensure instances are stopped
    """
    schedule_tag = os.environ.get('SCHEDULE_TAG', 'clinic-extended-hours')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    
    # If explicit action provided, use it
    if 'action' in event:
        action = event['action']
    else:
        # Determine action based on current Central Time
        now_utc = datetime.now(pytz.UTC)
        now_central = now_utc.astimezone(central_tz)
        current_hour = now_central.hour
        
        # 7 AM = 7, 9 PM = 21
        if 7 <= current_hour < 21:
            action = 'start'  # Business hours - ensure running
        else:
            action = 'stop'   # Outside hours - ensure stopped
    
    # Find instances with the schedule tag
    response = ec2.describe_instances(
        Filters=[
            {'Name': f'tag:Schedule', 'Values': [schedule_tag]},
            {'Name': 'instance-state-name', 'Values': ['running', 'stopped']}
        ]
    )
    
    instance_ids = []
    current_states = {}
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            instance_ids.append(instance_id)
            current_states[instance_id] = instance['State']['Name']
    
    if not instance_ids:
        print(f"No instances found with Schedule={schedule_tag}")
        return {
            'statusCode': 200,
            'action': action,
            'instances': [],
            'message': f'No instances found with Schedule={schedule_tag}'
        }
    
    # Perform action only if needed
    instances_to_act_on = []
    for instance_id in instance_ids:
        current_state = current_states[instance_id]
        if action == 'start' and current_state != 'running':
            instances_to_act_on.append(instance_id)
        elif action == 'stop' and current_state != 'stopped':
            instances_to_act_on.append(instance_id)
    
    if not instances_to_act_on:
        print(f"All instances already in desired state ({action})")
        return {
            'statusCode': 200,
            'action': action,
            'instances': instance_ids,
            'message': f'All instances already in desired state'
        }
    
    # Perform action
    if action == 'start':
        try:
            ec2.start_instances(InstanceIds=instances_to_act_on)
            print(f"Started {len(instances_to_act_on)} instances: {instances_to_act_on}")
            return {
                'statusCode': 200,
                'action': 'start',
                'instances': instances_to_act_on,
                'message': f'Successfully started {len(instances_to_act_on)} instance(s)'
            }
        except Exception as e:
            print(f"Error starting instances: {str(e)}")
            return {
                'statusCode': 500,
                'action': 'start',
                'error': str(e)
            }
    elif action == 'stop':
        try:
            ec2.stop_instances(InstanceIds=instances_to_act_on)
            print(f"Stopped {len(instances_to_act_on)} instances: {instances_to_act_on}")
            return {
                'statusCode': 200,
                'action': 'stop',
                'instances': instances_to_act_on,
                'message': f'Successfully stopped {len(instances_to_act_on)} instance(s)'
            }
        except Exception as e:
            print(f"Error stopping instances: {str(e)}")
            return {
                'statusCode': 500,
                'action': 'stop',
                'error': str(e)
            }
    else:
        return {
            'statusCode': 400,
            'error': f'Invalid action: {action}. Must be "start" or "stop"'
        }
