import pandas as pd
import numpy as np
import json
from datetime import date, datetime, timedelta
from intervaltree import Interval, IntervalTree
from common import *

# Extract the holidays, and daily shift from the data
dict_int_to_day = {"0": "Monday", "1": "Tuesday", "2": "Wednesday", "3": "Thursday", "4": "Friday"}

def generate_operators_availability(date: datetime) -> None:
# Let's try to generate the availability of the operator day wise
    weekday: str = str(date.weekday())

    for operator in operators:
    # Reset the availability of the operator first
        operator.availability = IntervalTree()

        if len(operator.holidays[date]) or date.weekday() in {5,6}:  # Check if the day is either in the holidays or weekend
            continue

        else:

            today_shift = data["OperatorWorkHours"][operator.name][dict_int_to_day[weekday]]
            list_today_shift = list(today_shift.keys())

            # TODO: check for even number of time constraints
            beginning_slot = list_today_shift[::2]  # Extract the beginning and ending time of the different operator's daily slots
            ending_slot = list_today_shift[1::2]    # Implicitly we assume that there is an even number of time constraints (namely the beginning and the ending of the time slot)

            for begin, end in zip(beginning_slot, ending_slot):
                start_time = str(date)[:10] + " " + today_shift[begin]
                start_time = datetime.fromisoformat(start_time)
                end_time = str(date)[:10] + " " + today_shift[end]
                end_time = datetime.fromisoformat(end_time)
                operator.availability.add(Interval(start_time, end_time, "idle"))


def is_task_assignable(tree: IntervalTree, task_duration: Interval) -> bool:    #TODO: Manually implement IntervalTree.py and add this as a method of IntervalTree.
    assignable: bool = False
    for interval in tree:
        if interval.begin <= task_duration.begin and task_duration.end <= interval.end:
            assignable = True
            break
    return assignable


def generate_lab_hours(date: datetime) -> IntervalTree:
    lab_hours: IntervalTree = IntervalTree()
    generate_operators_availability(date)
    for operator in operators:
        lab_hours |= operator.availability
    return lab_hours


def get_next_available_time_for_task(time: datetime, task: Step) -> datetime:
    task_duration: Interval = Interval(time, time + task.required, task.name)
    operators_available: list = [operator for operator in operators if is_task_assignable(operator.availability, task_duration) and task.name in operator.skills]

    new_time: datetime = time
    while len(operators_available) < 2:
        lab_hours = generate_lab_hours(new_time)
        has_moved: bool = False
        for interval in sorted(lab_hours):
            if interval.begin > time:
                time = interval.begin
                has_moved = True
                break

        if has_moved == False:
            new_time += timedelta(days=1)
        else:
            task_duration: Interval = Interval(time, time + task.required, task.name)
            operators_available: list = [operator for operator in operators if is_task_assignable(operator.availability, task_duration) and task.name in operator.skills]

    return time

