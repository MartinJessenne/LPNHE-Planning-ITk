import pandas as pd
import numpy as np
import json
from dateutil import rrule, parser
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from intervaltree import Interval, IntervalTree
from generate_operators_availability import generate_operator_availability
from TasksHierarchy import tasks_by_priority

# Definition of the different classes
@dataclass
class Operator:
    name: str
    skills: list
    holidays: IntervalTree
    availabilty: IntervalTree = field(default_factory=IntervalTree)
    shift: list = field(default_factory=list)

@dataclass
class Step:
    name: str
    step_number: int
    duration: timedelta
    capacity: int
    log: pd.DataFrame
    
# Load data from the JSON configuration file
with open("SimulatorInputs.json", "r") as file:
    data = json.load(file)

# Extract the simulation parameters
json_simulation_start: str = data['SimulationParameters']['simulation_start']
simulation_start: datetime = datetime.fromisoformat(json_simulation_start)

json_simeulation_end: str = data['SimulationParameters']['simulation_end']
simulation_end: datetime = datetime.fromisoformat(json_simeulation_end)

modules_planned_deliveries: str = data["ComponentArrivalTimes"]["bare modules"]

total_modules_number: int = 0
for delivery in modules_planned_deliveries:
    total_modules_number += modules_planned_deliveries[delivery]

# Initialize instances of the operator class
# All the operators are stored in the operators list
operators = []

# Initialize the CERN holidays to the good format
CERN_holidays_series = pd.Series(data["CERNHolidays"])
CERN_holidays_series[:] = CERN_holidays_series.apply(lambda x: [datetime.fromisoformat(date) for date in x])

for id, operator in enumerate(data["Operators"]):
    # Loads the operator's individual holidays
    individual_holidays = pd.Series(data["OperatorHolidays"][f"Operator{id + 1}"])
    individual_holidays[:] = individual_holidays.apply(lambda x: [datetime.fromisoformat(date) for date in x])

    # Merges the individual holidays with the CERN holidays
    operator_holidays = pd.concat([individual_holidays, CERN_holidays_series])
    operator_holidays[:] = operator_holidays.apply(lambda x: Interval(x[0], x[1], "holiday"))

    # Converts the pandas series to a list and then to an interval tree
    operator_holidays_list = operator_holidays.tolist()
    operator_holidays_tree = IntervalTree(operator_holidays_list)

    globals()[f"Operator{id + 1}"] = Operator(name=f"Operator{id + 1}", skills=operator, holidays=operator_holidays_tree)
    operators.append(globals()[f"Operator{id + 1}"])


# Initialization of the Step class instances, they are stored in the "Chronologically_Ordered_Steps" dict, the member log is a dataframe that will contain 
# the entry and exit dates of the modules at each step

Chronologically_Ordered_Steps = {}
for id, step in enumerate(data["StagesAndSteps"]):
    step_name = step["Step"].replace("-", "_").replace(" ", "_")
    globals()[step_name] = Step(name=step["Step"], step_number=id, duration=timedelta(minutes = step["Duration"]), capacity= step["Capacity"],log=pd.DataFrame(columns=["Entry_Date", "Exit_Date"]))
    Chronologically_Ordered_Steps |= {step["Step"]: globals()[step_name]}

time: datetime = simulation_start
finished_modules_count: int = 0 #Might be a duplicate

# Initialization of the list of the differents tasks and steps to be done
tasks_to_do: list = [task['Step'] for task in data["StagesAndSteps"]]

# Initialization of the dataframe that will contain the assignments of the operators
operators_assignments: pd.DataFrame = pd.DataFrame(columns=[tasks_to_do], index=[simulation_start])


while (time < simulation_end or finished_modules_count < total_modules_number):
    
    generate_operator_availability(time)
    to_do: list = tasks_by_priority(time)
    for task in to_do:
        # if two operators are available, and qualified for the task we assign them to the task
        if 
        # else we increment time by 15 minutes
