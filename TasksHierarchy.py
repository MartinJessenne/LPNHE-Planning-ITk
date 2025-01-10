import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from ObjectOrientedProductionSimulation import *

# Load the state of production csv data into each step
state_of_production = pd.read_csv("inventory.csv", sep=";", dtype={"Quantity":'Int64'}, index_col=0, parse_dates=["Launching Time"])
state_of_production.fillna({"Quantity": 0}, inplace=True)

# This piece of code fills by default the missing values of the Ready components launching time
# to simulation_start_time - duration of the task to simulate the fact that they are just ready to 
# be moved to the next step when the simulation is launched. 
for index, row in state_of_production.iterrows():   # I prefer to work with literal values of Index and Columns rather than the integer values for more reliability and legibility
    if row["Quantity"] > 0:     # We only care about the Steps where there are modules
        for task in iter(Chronologically_Ordered_Steps):
            if task in index:   # Looks for the task associated with the row TODO : suppress this loop by implementing yet another lookup table
                if pd.isna(state_of_production.loc[index, "Launching Time"]):
                    Time_ready_by_simulation_start = simulation_start - timedelta(minutes=Chronologically_Ordered_Steps[task].duration)   # In the last part we extract the duration associated with the task
                    state_of_production.loc[index, "Launching Time"] = Time_ready_by_simulation_start

# Now we fill all those initial data into the log attribute of each step
for Step in Chronologically_Ordered_Steps.values():
    row_Ready = state_of_production.loc[Step.name + " Ready"]
    row_WIP = state_of_production.loc[Step.name + " WIP"]
    df_Ready = pd.DataFrame([[row_Ready.loc["Launching Time"],pd.NaT] for _ in range(row_Ready.loc["Quantity"])],columns=["Entry_Date", "Exit_Date"])
    df_WIP = pd.DataFrame([[row_WIP.loc["Launching Time"],pd.NaT] for _ in range(row_WIP.loc["Quantity"])],columns=["Entry_Date", "Exit_Date"])
    Step.log = pd.concat([Step.log, df_Ready, df_WIP], ignore_index=True)   # TODO : maybe numpy arrays are better suited in this context, since it's a single type of datatype

Steps = list(reversed(Chronologically_Ordered_Steps.values()))[:-1]
Previous_Steps = list(reversed(Chronologically_Ordered_Steps.values()))[2:]


def tasks_by_priority(time: timedelta) -> list:

    steps_to_do = []

    for Step,Previous_Step in zip(Steps, Previous_Steps):

        time_array = np.full_like(Previous_Step.log.loc[:,"Entry_Date"], time)
        time_spent_in_task = time_array - Previous_Step.log.loc[:,"Entry_Date"]
        duration_delta = timedelta(minutes=Previous_Step.duration) 

    if sum(time_spent_in_task >= duration_delta) >= Step.capacity:
        steps_to_do.append(Step)

    return steps_to_do
        
        