import random
import string
from apscheduler.schedulers.background import BackgroundScheduler

from util.misc import printer

scheduler = BackgroundScheduler()
scheduler.start()


class Scheduler:
    scheduler = scheduler
    jobs = {}

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
        id: str = None,
    ) -> str:
        """Add a scheduled job in the background

        Args:
            function (callable): function to run at each job execution
            args (list): list of arguments to pass function. Defaults to [].
            seconds (int): how often to run job
            removal_condition (callable, optional): function called to decide if to remove job based on return value. Must return bool. Defaults to None.
            id (str, optional): manually set job ID. Defaults to None.
        Returns:
            str: [description]
        """
        if not id:
            id = cls._job_id()

        if id in cls.jobs:
            cls[jobs][id].remove()
            del cls[jobs][id]

        def job_wrapper():
            return_value = function(*args)
            if removal_condition and removal_condition(return_value):
                cls.scheduler.remove_job(id)

        job = cls.scheduler.add_job(
            job_wrapper,
            "interval",
            seconds=seconds,
            id=id,
        )
        cls.jobs[id] = job
        return id
