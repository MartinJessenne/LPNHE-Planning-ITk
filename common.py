import pandas as pd
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from intervaltree import Interval, IntervalTree

# Definition of the different classes
@dataclass
class Operator:
    name: str
    skills: list
    holidays: IntervalTree
    availability: IntervalTree = field(default_factory=IntervalTree)
    shift: list = field(default_factory=list)  # Is it used ?


@dataclass
class Step:
    name: str
    previous_steps: list
    duration: timedelta
    required: timedelta
    capacity: int
    log: pd.DataFrame

    def __repr__(self):
        return f"Step {self.name}, duration {self.duration}, capacity {self.capacity}, log \n{self.log}"


# Load data from the JSON configuration file
with open("SimulatorInputs.json", "r") as file:
    data = json.load(file)

# Extract the simulation parameters
json_simulation_start: str = data['SimulationParameters']['simulation_start']
simulation_start: datetime = datetime.fromisoformat(json_simulation_start)

json_simulation_end: str = data['SimulationParameters']['simulation_end']
simulation_end: datetime = datetime.fromisoformat(json_simulation_end)

# modules_planned_deliveries: str = data["ComponentArrivalTimes"]["bare modules"]

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

    globals()[f"Operator{id + 1}"] = Operator(name=f"Operator{id + 1}", skills=data["Operators"][operator],
                                              holidays=operator_holidays_tree)
    operators.append(globals()[f"Operator{id + 1}"])

# Initialization of the Step class instances, they are stored in the "Chronologically_Ordered_Steps" dict, the member log is a dataframe that will contain
# the entry and exit dates of the modules at each step

Chronologically_Ordered_Steps = {}
for step in data["StagesAndSteps"]:
    step_name = step["Step"].replace("-", "_").replace(" ",
                                                       "_")  # Just converting the name to python valid name without blank spaces and

    if step["Previous"][0] == "None":
        globals()[step_name] = Step(name=step["Step"], previous_steps=[None],
                                    duration=timedelta(minutes=step["Duration"]),
                                    required=timedelta(minutes=step["Required"]), capacity=step["Capacity"],
                                    log=pd.DataFrame(columns=["Entry_Date", "Exit_Date"]))

    else:
        previous_steps = []
        for prev_step in step["Previous"]:
            previous_steps.append(Chronologically_Ordered_Steps[prev_step])
            globals()[step_name] = Step(name=step["Step"], previous_steps=previous_steps,
                                        duration=timedelta(minutes=step["Duration"]),
                                        required=timedelta(minutes=step["Required"]), capacity=step["Capacity"],
                                        log=pd.DataFrame(columns=["Entry_Date", "Exit_Date"]))
    Chronologically_Ordered_Steps |= {step["Step"]: globals()[step_name]}
