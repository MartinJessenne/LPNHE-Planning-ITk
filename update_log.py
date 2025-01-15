from ObjectOrientedProductionSimulation import *

# Once a task is assigned to operators, a module has to be withdrawn 
# from the previous step and added to the next step
def update_log(task, time):

    # First step, we remove the module from the previous step
    if task.previous_steps[0] is None:  # There is no need to update the log for the first step
        pass
    else:
        for previous_step in task.previous_steps:
            condition = (previous_step.log["Entry_Date"] + previous_step.duration <= time) & (pd.isna(previous_step.log["Exit_Date"]))    # Filter all the modules ready that have not been moved yet
                # Check if any rows match the condition
            if condition.any():
                # Get the first index that matches the condition
                first_matching_index = previous_step.log[condition].index[0]    # Take the first ready module and fill its exit date
                
                # Update the 'Exit_Date' for the first matching row
                previous_step.log.at[first_matching_index, "Exit_Date"] = time
            else:
                print("There is a big issue !!")

    # Second step, we add the module to the next step
    df = pd.DataFrame([[time, pd.NaT]], columns=["Entry_Date", "Exit_Date"])
    task.log = pd.concat([task.log, df], ignore_index=True)