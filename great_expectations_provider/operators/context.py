import logging
import boto3
logger = logging.getLogger(__name__)

try:
    from airflow.hooks.S3_hook import S3Hook
except ImportError:
    from airflow.providers.amazon.aws.hooks import S3Hook

class S3Context:
    hook = None

    def __init__(self, hook)->None:
        self.hook = hook
    
    def __enter__(self):
        if self.hook is not None:        
            boto3.DEFAULT_SESSION = self.hook.get_session()  
            logger.debug(f'Setting default boto3 session: {boto3.DEFAULT_SESSION}')


        
    def __exit__(self, exception_type, exception_value, traceback):
        if self.hook is not None:
            boto3.DEFAULT_SESSION = None
            logger.debug(f'Flushing boto3 default session')


class SessionContextWrap:
    '''Wrapper, meant to abstract
       specific connection tactis away from the operator

       In order to add new connection type/behaviour (e.g. BigQuery),
       create another Context Class and set corresponding switch 
    '''
    
    context = None

    def __init__(self, hook=None)->None:
        self.hook = hook

        if hook is not None and isinstance(hook, S3Hook):
            self.context = S3Context(self.hook)

