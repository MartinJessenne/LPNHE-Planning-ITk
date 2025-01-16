import pandas as pd
import numpy as np
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from intervaltree import Interval, IntervalTree
from generate_operators_availability import generate_operators_availability
from TasksHierarchy import tasks_by_priority
from update_log import update_log

# Definition of the different classes
@dataclass
class Operator:
    name: str
    skills: list
    holidays: IntervalTree
    availability: IntervalTree = field(default_factory=IntervalTree)
    shift: list = field(default_factory=list)   # Is it used ?

@dataclass
class Step:
    name: str
    previous_steps: list
    duration: timedelta
    required: timedelta
    capacity: int
    log: pd.DataFrame

    def __repr__(self):
        return f"Step {self.name}, duration {self.duration}, capacity {self.capacity}, log {self.log}"
    
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

    globals()[f"Operator{id + 1}"] = Operator(name=f"Operator{id + 1}", skills=data["Operators"][operator], holidays=operator_holidays_tree)
    operators.append(globals()[f"Operator{id + 1}"])


# Initialization of the Step class instances, they are stored in the "Chronologically_Ordered_Steps" dict, the member log is a dataframe that will contain 
# the entry and exit dates of the modules at each step

Chronologically_Ordered_Steps = {}
for step in data["StagesAndSteps"]:
    step_name = step["Step"].replace("-", "_").replace(" ", "_")    # Just converting the name to python valid name without blank spaces and

    if step["Previous"][0] == "None":
        globals()[step_name] = Step(name=step["Step"], previous_steps=[None], duration=timedelta(minutes = step["Duration"]),required=timedelta(minutes = step["Required"]), capacity= step["Capacity"],log=pd.DataFrame(columns=["Entry_Date", "Exit_Date"]))

    else:
        previous_steps = []
        for prev_step in step["Previous"]:
            previous_steps.append(Chronologically_Ordered_Steps[prev_step])
            globals()[step_name] = Step(name=step["Step"], previous_steps=previous_steps, duration=timedelta(minutes = step["Duration"]), required=timedelta(minutes = step["Required"]), capacity= step["Capacity"],log=pd.DataFrame(columns=["Entry_Date", "Exit_Date"]))
    Chronologically_Ordered_Steps |= {step["Step"]: globals()[step_name]}

time: datetime = simulation_start
finished_modules_count: int = sum(S___PDB_Shipment_of_modules_to_loading_sites.log["Exit_Date"].notna())

# Initialization of the list of the differents tasks and steps to be done
tasks_to_do: list = [task['Step'] for task in data["StagesAndSteps"]]

# Initialization of the dataframe that will contain the assignments of the operators
operators_assignments: pd.DataFrame = pd.DataFrame(columns=tasks_to_do, dtype=object)


while (time < simulation_end or finished_modules_count < total_modules_number):
    generate_operators_availability(time)    # TODO: Duplication of computation, we could do it once for the day
    to_do: list = tasks_by_priority(time)
    was_a_task_assigned: bool = False
    for task in to_do:
        # if two operators are available, and qualified for the task we assign them to the task
        task_duration: Interval = Interval(time, time + task.required, task.name)
        operators_available: list = [operator for operator in operators if operator.availability.overlaps(task_duration) and task.name in operator.skills]

        # For now we chose at random two of the available operators to perform the task, the law according to which we chose the operators my be reweighted according to their skills
        # Wire Bonding journée entière et 2 fois par semaine. 
        if len(operators_available) >= 2:
            chosen_operators: list = np.random.choice(operators_available, 2, replace=False)
            first_operator, second_operator = chosen_operators
            first_operator.availability.chop(time, time + task.required)
            second_operator.availability.chop(time, time + task.required)
            assigned_operators = (first_operator.name, second_operator.name)
            operators_assignments.loc[time, task.name] = assigned_operators
            print(f"A task was assigned at {time} to {assigned_operators} for the task {task.name}")
            update_log(task, time)
            print(f"task log : \n{task.log}\n\n")
            for previous_steps in task.previous_steps:
                print(f"previous step log : \n{previous_steps.log}\n\n")
            time += task.required
            was_a_task_assigned = True
            break
    
    if was_a_task_assigned == False:
        time += timedelta(minutes=60)


