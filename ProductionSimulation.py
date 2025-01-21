import pandas as pd
import numpy as np
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from intervaltree import Interval, IntervalTree
from generate_operators_availability import generate_operators_availability, get_next_available_time_for_task, \
    is_task_assignable
from TasksHierarchy import tasks_by_priority
from update_log import update_log
from common import *
from displays import Display



finished_modules_count: int = sum(S___PDB_Shipment_of_modules_to_loading_sites.log["Entry_Date"].notna())

# Initialization of the list of the differents tasks and steps to be done
tasks_to_do: list = [task['Step'] for task in data["StagesAndSteps"]]

# Initialization of the dataframe that will contain the assignments of the operators
operators_assignments: pd.DataFrame = pd.DataFrame(columns=list(Chronologically_Ordered_Steps.keys()), dtype=object)

time = simulation_start

modules_to_do: int = int(input("Number of modules to simulate (it must coincides with the values in inventory.csv) : \n"))

modules_completed = sum(S___PDB_Shipment_of_modules_to_loading_sites.log["Entry_Date"].notna())

while (time < simulation_end and modules_completed < modules_to_do):   #TODO: Compute the number of final modules expected

    generate_operators_availability(time)  # TODO: Duplication of computation, we could do it once for the day
    to_do: list = tasks_by_priority(time)
    was_a_task_assigned: bool = False
    if len(to_do) == 0:
        time += timedelta(hours=1)

    while len(to_do) > 0:

        # if two operators are available, and qualified for the task we assign them to the task
        task = to_do.pop(0)

        time = get_next_available_time_for_task(time, task)

        task_duration: Interval = Interval(time, time + task.required, task.name)

        operators_available: list = [operator for operator in operators if is_task_assignable(operator.availability,
                                                                                              task_duration) and task.name in operator.skills]

        if len(operators_available) < 2:
            print("Err")

        # For now we chose at random two of the available operators to perform the task, the law according to which we chose the operators might be reweighted according to their skills


        chosen_operators: list = np.random.choice(operators_available, 2, replace=False)
        first_operator, second_operator = chosen_operators
        first_operator.availability.chop(time, time + task.required)
        second_operator.availability.chop(time, time + task.required)
        assigned_operators = (first_operator.name, second_operator.name)
        operators_assignments.loc[time, task.name] = assigned_operators

        update_log(task, time)
        time += task.required
        was_a_task_assigned = True

        break

    modules_completed = sum(S___PDB_Shipment_of_modules_to_loading_sites.log["Entry_Date"].notna())

Display(operators_assignments)



