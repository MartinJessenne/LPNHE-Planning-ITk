import numpy as np
import pandas as pd
from datetime import datetime
from intervaltree import Interval, IntervalTree
from common import operators
from generate_operators_availability import generate_lab_hours

operators_dict: dict = {operator.name: operator for operator in operators} 

def assign_operators(time, operators_available, task, operators_assignments) -> None:
    if operators_assignments.shape[0] > 0:   # Check if there are any lines in operators_assignments
        last_assignment: pd.DataFrame = operators_assignments.tail(1)
        last_assignment_date: datetime = last_assignment.index[0]
        last_assignment_operators: tuple = last_assignment.dropna(axis=1).iloc[0].iloc[0]

        # Two things need to be checked:
        # 1. Has the last assignment been done in the same half-day
        # 2. Are the two past assigned operators still available

        todays_lab_hours: IntervalTree = generate_lab_hours(time)    # TODO: this is highly inefficient to compute the lab hours twice in the same main loop in ProductionSimulation.py. This call is normally not necessary since it's been already called once in the loop

        same_period: bool = (todays_lab_hours[last_assignment_date] == todays_lab_hours[time])
        operators_still_available: bool = (set(last_assignment_operators) <= set([operator.name for operator in operators_available]))

        if same_period and operators_still_available:
            first_operator, second_operator = chosen_operators = (operators_dict[last_assignment_operators[0]], operators_dict[last_assignment_operators[1]])
            first_operator.availability.chop(time, time + task.required)
            second_operator.availability.chop(time, time + task.required)
            assigned_operators = (first_operator.name, second_operator.name)
            operators_assignments.loc[time, task.name] = assigned_operators
        
        else:
            # If we can't reassign the past operators, we choose two new operators at random
            chosen_operators: list = np.random.choice(operators_available, 2, replace=False)
            first_operator, second_operator = chosen_operators
            first_operator.availability.chop(time, time + task.required)
            second_operator.availability.chop(time, time + task.required)
            assigned_operators = (first_operator.name, second_operator.name)
            operators_assignments.loc[time, task.name] = assigned_operators
        
    else:
        # If we can't reassign the past operators, we choose two new operators at random
        chosen_operators: list = np.random.choice(operators_available, 2, replace=False)
        first_operator, second_operator = chosen_operators
        first_operator.availability.chop(time, time + task.required)
        second_operator.availability.chop(time, time + task.required)
        assigned_operators = (first_operator.name, second_operator.name)
        operators_assignments.loc[time, task.name] = assigned_operators
