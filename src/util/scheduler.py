import random
import string
from apscheduler.schedulers.background import BackgroundScheduler

from util.misc import printer

scheduler = BackgroundScheduler()
scheduler.start()


class Scheduler:
    scheduler = scheduler

    # Private Functions

    @staticmethod
    def _job_id(length: int = 5):
        """Generates a random ID for each job

        Args:
            length (int, optional). Defaults to 5.

        Returns:
            str
        """
        letters = string.ascii_lowercase
        return "".join(random.choice(letters) for i in range(length))

    # Public Functions

    @classmethod
    def add_job(
        cls,
        function: callable,
        seconds: int,
        args: list = [],
        removal_condition: callable = None,
        verbose: bool = False,
    ) -> str:
        """Add a scheduled job in the background

        Args:
            function (callable): function to run at each job execution
            args (list): list of arguments to pass function. Defaults to [].
            seconds (int): how often to run job
            removal_condition (callable, optional): function called to decide if to remove job based on return value. Must return bool. Defaults to None.
            verbose (bool, optional). Defaults to False.

        Returns:
            str: [description]
        """
        printer("In add job")
        job_id = cls._job_id()

        def job_wrapper():
            if verbose:
                printer(f"Executing Job ID [{job_id}]")
            return_value = function(*args)
            if removal_condition and removal_condition(return_value):
                cls.scheduler.remove_job(job_id)

        cls.scheduler.add_job(job_wrapper, "interval", seconds=seconds, id=job_id)
        return job_id
