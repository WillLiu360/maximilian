import boto3
from cocore.config import Config
from past.builtins import basestring


class SESInteraction:
    """
    wrapper on boto3 ses
    """
    def __init__(self, to, subject, sender, aws_access_key, aws_secret_key, aws_region='us-east-1'):
        """
        :param to:
        :param subject:
        :return:
        """

        self.connection = boto3.client('ses',
                                        aws_access_key_id=aws_access_key,
                                        aws_secret_access_key=aws_secret_key,
                                        region_name=aws_region)

        self.to = to
        self.subject = subject
        self._html = None
        self._text = None
        self._format = 'html'
        self.def_sender = sender

    def html(self, html):
        """
        set's email html message property
        :param html:
        :return:
        """
        self._html = html

    def text(self, text):
        """
        set's email text message property
        :param text:
        :return:
        """
        self._text = text

    def send(self, from_addr=None):
        """
        sends email
        :param from_addr:
        :return:
        """
        body = self._html
        
        if isinstance(self.to, basestring):
            self.to = [self.to]
        if not from_addr:
            from_addr = self.def_sender
        if not self._html and not self._text:
            raise Exception('You must provide a text or html body.')
        if not self._html:
            self._format = 'text'
            body = self._text

        return self.connection.send_email(
            Source = from_addr,
            Destination = {
                'ToAddresses': self.to
            },
            Message = {
                'Subject':{
                    'Data': self.subject
                },
                'Body': {
                    'Text': {
                        'Data': body
                    },
                    'Html': {
                        'Data': body
                    }
                }
            }
        )

if __name__ == '__main__':
    """
    sending email sample run
    """
    conf = Config()
    aws_access_key = conf['aws']['aws_access_key']
    aws_secret_key = conf['aws']['aws_secret_key']
    aws_region = conf['aws']['aws_region']
    email = SESInteraction(conf['ses']['ses_def_recipient'],
                            'Sample Email', conf['ses']['ses_def_sender'],
                            aws_access_key, aws_secret_key, aws_region
                           )
    email.html("<b>Sample Message</b>")
    email.text('Sample Message')
    email.send()
    print('Email sent!')
