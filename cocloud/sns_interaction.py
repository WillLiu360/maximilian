import json
import boto3


class SNSInteraction:

    def __init__(self, aws_access_key, aws_secret_key, topic, region='us-east-1'):
        self.region = region
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key

        self.client = boto3.client('sns',
                                   aws_access_key_id=aws_access_key,
                                   aws_secret_access_key=aws_secret_key,
                                   region_name=region,
                                   )
        self.topic = topic

    def create_topic(self):
        response = self.client.create_topic(Name=self.sns_topic)
        topic_arn = response['TopicArn']
        print(f'Created topic ARN: {topic_arn}')
        return topic_arn

    def create_publisher(self):
        return Publisher(self)

    def create_subscriber(self, sqs_interaction):
        return Subscriber(self, sqs_interaction)


class Publisher(object):

    def __init__(self, sns_interaction):
        self.sns_interaction = sns_interaction
        self.sns_topic = str(sns_interaction.topic)
        self.sns_topic_arn = None
        self.client = self.sns_interaction.client

        print(f'---------------\nPublisher starting: {self.sns_topic}')
        topic_list = self.client.list_topics()

        for t in topic_list['Topics']:
            if self.sns_topic == t['TopicArn'][-len(self.sns_topic):]:
                self.sns_topic_arn = t['TopicArn']

        # check if topic exists
        if self.sns_topic_arn is None:
            raise ValueError('Topic does not exist')
        else:
            print(f'Topic exists')

    def post_message(self, message):
        response = self.client.publish(TopicArn=self.sns_topic_arn, Message=str(message))
        print(f'Message posted: {self.sns_topic_arn}\n{json.loads(message)}')
        return response

    def delete_topic(self):
        response = self.client.delete_topic(TopicArn=self.sns_topic_arn)
        print('SNS topic ARN deleted: {}'.format(self.sns_topic_arn))
        return response


class Subscriber(object):

    def __init__(self, sns_interaction, sqs_interaction):
        self.sns_interaction = sns_interaction
        self.sns_topic = str(sns_interaction.topic)
        self.sns_topic_arn = None
        print('---------------\nSubscriber starting:')
        sns_client = sns_interaction.client

        self.sqsInteraction = sqs_interaction
        self.sqs_subscriber = self.sqsInteraction.create_subscriber()

        # check subscriptions and subcribe to SNS
        topic_list = sns_client.list_topics()
        subscribed = False

        for t in topic_list['Topics']:
            # if self.sns_topic in t['TopicArn']:
            if self.sns_topic == t['TopicArn'][-len(self.sns_topic):]:
                self.sns_topic_arn = t['TopicArn']
                print(f'Topic ARN: {self.sns_topic_arn}')

        # check if topic exists
        if self.sns_topic_arn is None:
            raise ValueError('Topic does not exist')
        else:
            print(f'Topic exists')

        subscriber_list = sns_client.list_subscriptions_by_topic(
            TopicArn=self.sns_topic_arn)['Subscriptions']

        for s in subscriber_list:
            if s.get('Endpoint', None) == self.sqsInteraction.sqs_queue_arn:
                subscribed = True
                print(f'Already subscribed: {s}')
                break

        if not subscribed:
            sns_client.subscribe(
                TopicArn=self.sns_topic_arn, Protocol='sqs', Endpoint=self.sqsInteraction.sqs_queue_arn)
            print(f'Subscribed: {self.sqs_queue_arn}')

        # check and set policy
        queue_policy_statement = {
            "Sid": "auto-transcode",
            "Effect": "Allow",
            "Principal": {
                "AWS": "*"
            },
            "Action": "SQS:SendMessage",
            "Resource": "<SQS QUEUE ARN>",
            "Condition": {
                "StringLike": {
                    "aws:SourceArn": "<SNS TOPIC ARN>"
                }
            }
        }
        queue_attribs = self.sqsInteraction.get_queue_attribs()
        if 'Policy' in queue_attribs:
            policy = json.loads(queue_attribs['Policy'])
            print(f'Policy found: {policy}')
        else:
            policy = {'Version': '2008-10-17'}
            print('No policy')

        if 'Statement' not in policy:
            statement = queue_policy_statement
            statement['Resource'] = self.sqsInteraction.sqs_queue_arn
            statement['Condition']['StringLike']['aws:SourceArn'] = self.sns_topic_arn
            policy['Statement'] = [statement]

        self.sqsInteraction.set_policy(policy)

    def get_messages(self, num_messages=1):
        return self.sqs_subscriber.get_messages(num_messages)

    def delete_message(self, message):
        self.sqs_subscriber.delete_message(message)
