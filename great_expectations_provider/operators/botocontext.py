import logging
import boto3

logger = logging.getLogger(__name__)


class SessionContext:

    hook = None

    def __init__(self, hook=None)->None:
        self.hook = hook


    def __enter__(self):
        if self.hook is not None:        
            boto3.DEFAULT_SESSION = self.hook.get_session()  
            logger.debug(f'Setting default boto3 session: {boto3.DEFAULT_SESSION}')


        
    def __exit__(self, exception_type, exception_value, traceback):
        if self.hook is not None:
            boto3.DEFAULT_SESSION = None
            logger.debug(f'Flushing boto3 default session')
