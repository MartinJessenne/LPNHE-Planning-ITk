from datetime import timedelta

import numpy as np
import pandas as pd

from common import *

# Load the state of production csv data into each step
state_of_production = pd.read_csv("inventory.csv", sep=";", dtype={"Quantity": 'Int64'}, index_col=0,
                                  parse_dates=["Launching Time"])
state_of_production.fillna({"Quantity": 0}, inplace=True)

total_modules_number: int = int(sum(state_of_production.loc[:, "Quantity"]))
# for delivery in modules_planned_deliveries:
#    total_modules_number += modules_planned_deliveries[delivery]

# This piece of code fills by default the missing values of the Ready components launching time
# to simulation_start_time - duration of the task to simulate the fact that they are just ready to
# be moved to the next step when the simulation is launched.
for index, row in state_of_production.iterrows():  # I prefer to work with literal values of Index and Columns rather than the integer values for more reliability and legibility
    if row["Quantity"] > 0:  # We only care about the Steps where there are modules
        for task in iter(Chronologically_Ordered_Steps):
            if task in index:  # Looks for the task associated with the row TODO : suppress this loop by implementing yet another lookup table
                if pd.isna(state_of_production.loc[index, "Launching Time"]):
                    Time_ready_by_simulation_start = simulation_start - Chronologically_Ordered_Steps[
                        task].duration  # In the last part we extract the duration associated with the task
                    state_of_production.loc[index, "Launching Time"] = Time_ready_by_simulation_start

# Now we fill all those initial data into the log attribute of each step
for Step in Chronologically_Ordered_Steps.values():
    row_Ready = state_of_production.loc[Step.name + " Ready"]
    row_WIP = state_of_production.loc[Step.name + " WIP"]
    df_Ready = pd.DataFrame([[row_Ready.loc["Launching Time"], pd.NaT] for _ in range(row_Ready.loc["Quantity"])],
                            columns=["Entry_Date", "Exit_Date"])
    df_WIP = pd.DataFrame([[row_WIP.loc["Launching Time"], pd.NaT] for _ in range(row_WIP.loc["Quantity"])],
                          columns=["Entry_Date", "Exit_Date"])
    Step.log = pd.concat([Step.log, df_Ready, df_WIP],
                         ignore_index=True)  # TODO : maybe numpy arrays are better suited in this context, since it's a single type of datatype, in addition to that this line raises a warning


def tasks_by_priority(time: timedelta) -> list:  # Returns the list of tasks that can be done at the current time

    steps_to_do = []

    for step in list(reversed(Chronologically_Ordered_Steps.values())):

        # First step, check if the step is ready to process new modules
        time_array: np.ndarray = np.full_like(step.log.loc[:, "Entry_Date"], time)
        time_spent_in_task: pd.DataFrame = time_array - step.log.loc[:, "Entry_Date"]

        # We check if the number of modules being processed at the moment is greater than the capacity of the step

        condition = sum((time_spent_in_task < step.duration) & (pd.isna(step.log["Exit_Date"]))) >= step.capacity

        if condition:  # If the step is not ready to process new modules
            continue  # we break the loop and go to the next step

        if step.previous_steps[
            0] is None:  # If previous_steps is None. That means that this the first step
            continue  # By default the first step is always ready, in fact that means that there are not enough components left

        else:
            # Second step, we compute the reception capacity of the step
            reception_capacity = step.capacity - sum(pd.isna(
                step.log["Exit_Date"]))  # We compute the number of modules ready to be processed in the next step

            # Now we check if among the previous steps there are enough modules ready to be processed in the next step
            ready_to_be_processed: bool = True
            modules_ready_overall: int = np.inf
            for previous_step in step.previous_steps:

                previous_time_array: np.ndarray = np.full_like(previous_step.log.loc[:, "Entry_Date"], time)
                time_spent_in_previous_task: pd.DataFrame = previous_time_array - previous_step.log.loc[:,
                                                                                  "Entry_Date"]  # Displays the time spent in the previous task for each module
                modules_ready: int = sum((time_spent_in_previous_task >= previous_step.duration) & (pd.isna(
                    previous_step.log[
                        "Exit_Date"])))  # If there are not enough modules ready to be processed in the previous steps
                modules_ready_overall = min(modules_ready,
                                            modules_ready_overall)  # We take the minimum of the modules ready in the previous steps
                if modules_ready <= 0:
                    ready_to_be_processed = False

            if ready_to_be_processed:
                for _ in range(
                        min(reception_capacity, modules_ready_overall)):  # TODO: list comprehension to fasten the loop
                    steps_to_do.append(step)

    return steps_to_do
        
        