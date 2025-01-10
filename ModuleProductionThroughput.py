# -------------------------------------------------------------------------
# Script Name: ModuleProductionThroughput.py
# Description: Python script designed to simulate the production throughput
# of ITk Pixel Quad Modules. The script is highly # configurable via a JSON
# file and generates several plots to visualise the module production
# pipeline, including throughput analysis, module scheduling, and operator
# workload.
# Author: Abhishek.Sharma@cern.ch
# Date: 2024-10-24
# Version: 1.0
# Gitlab: https://gitlab.cern.ch/absharma/itk-module-production-simulator
# Usage: python ModuleProductionThroughput.py
# -------------------------------------------------------------------------

import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
import json
from datetime import datetime, timedelta
import numpy as np
import time
from tqdm import tqdm
from functools import lru_cache


# Start timing the script execution
start_time = time.time()

# Load data from the JSON configuration file
with open('SimulatorInputs.json', 'r') as f:
    json_data = json.load(f)

data_list = json_data["StagesAndSteps"]

# Read simulation parameters from the JSON file
simulation_params = json_data.get("SimulationParameters", {})
num_modules = simulation_params.get("num_modules", 25)  # Default to 25 modules if not specified
simulation_start_str = simulation_params.get("simulation_start", "2024-11-01T09:00:00")
simulation_start = datetime.strptime(simulation_start_str, "%Y-%m-%dT%H:%M:%S")
transition_delay = simulation_params.get("transition_delay", 10)  # Delay between steps in minutes
parylene_shipment_weekday = simulation_params.get("parylene_shipment_weekday", 4)  # Day of the week for shipments (Friday)

# Enable or disable operator holidays
enable_operator_holidays = True

# Time increment in minutes for scheduling (adjustable granularity)
time_increment = 15  # Can be adjusted to 1, 5, 10, 15, etc.

# Extract work hours from JSON
work_hours = json_data["WorkHours"]
work_start = datetime.strptime(work_hours["work_start"], "%H:%M:%S").time()
work_end = datetime.strptime(work_hours["work_end"], "%H:%M:%S").time()
lunch_start = datetime.strptime(work_hours["lunch_start"], "%H:%M:%S").time()
lunch_end = datetime.strptime(work_hours["lunch_end"], "%H:%M:%S").time()
work_days = work_hours["work_days"]  # List of working days (e.g., [0, 1, 2, 3, 4] for Monday to Friday)

# Extract CERN holidays from JSON
cern_holidays_list = json_data["CERNHolidays"]
cern_holidays = []
for holiday in cern_holidays_list:
    start_date = datetime.strptime(holiday[0], "%Y-%m-%d").date()
    end_date = datetime.strptime(holiday[1], "%Y-%m-%d").date()
    cern_holidays.append((start_date, end_date))

# Extract operator holidays from JSON
operator_holidays_data = json_data["OperatorHolidays"]
operator_holidays = {}
for operator, holidays in operator_holidays_data.items():
    operator_holidays[operator] = []
    for holiday in holidays:
        holiday_start_date = datetime.strptime(holiday[0], "%Y-%m-%d").date()
        holiday_end_date = datetime.strptime(holiday[1], "%Y-%m-%d").date()
        operator_holidays[operator].append((holiday_start_date, holiday_end_date))
        
# Map weekdays from names to numbers (Monday=0, ..., Sunday=6)
weekday_map = {
    'Monday': 0,
    'Tuesday': 1,
    'Wednesday': 2,
    'Thursday': 3,
    'Friday': 4,
    'Saturday': 5,
    'Sunday': 6
}

# Extract operator work hours from JSON
operator_work_hours_data = json_data["OperatorWorkHours"]
operator_work_hours = {}
for operator, schedule in operator_work_hours_data.items():
    operator_work_hours[operator] = {}
    for day_name, periods in schedule.items():
        weekday = weekday_map[day_name]
        operator_work_hours[operator][weekday] = []
        for period in periods:
            period_start_time = datetime.strptime(period["start"], "%H:%M:%S").time()
            period_end_time = datetime.strptime(period["end"], "%H:%M:%S").time()
            operator_work_hours[operator][weekday].append((period_start_time, period_end_time))

# Extract operators and their assigned steps
operators_data = json_data["Operators"]
operators = {}
for operator, steps in operators_data.items():
    operators[operator] = steps

# Steps requiring operator presence throughout
steps_requiring_operator = json_data["StepsRequiringOperator"]

# Steps that can be launched by an operator
steps_launch_by_operator = json_data["StepsLaunchByOperator"]

# Extract component arrival batches from JSON
component_arrival_times_data = json_data["ComponentArrivalTimes"]
component_arrival_batches = {}
for component, arrivals in component_arrival_times_data.items():
    batches = []
    for arrival_time_str, quantity in arrivals.items():
        arrival_time = datetime.strptime(arrival_time_str, "%Y-%m-%dT%H:%M:%S")
        batches.append({'arrival_time': arrival_time, 'quantity': int(quantity)})
    # Sort batches by arrival time
    batches.sort(key=lambda x: x['arrival_time'])
    component_arrival_batches[component] = batches

# Initialize available units for each component
component_available_units = {}
for component, batches in component_arrival_batches.items():
    units = []
    for batch in batches:
        arrival_time = batch['arrival_time']
        quantity = batch['quantity']
        for _ in range(quantity):
            units.append(arrival_time)
    # Sort the units by availability time
    units.sort()
    component_available_units[component] = units

# Extract stages in order as defined in JSON
stages_in_order = []
for item in data_list:
    stage = item['Stage']
    if stage not in stages_in_order:
        stages_in_order.append(stage)

# Define stage colors for visualization (RGBA format with alpha for transparency)
# Assign colors to stages in order
color_palette = [
    'rgba(135, 206, 235, 0.6)',  # skyblue
    'rgba(255, 165, 0, 0.6)',    # orange
    'rgba(0, 128, 0, 0.6)',      # green
    'rgba(255, 0, 0, 0.6)',      # red
    'rgba(128, 0, 128, 0.6)',    # purple
    'rgba(165, 42, 42, 0.6)',    # brown
    'rgba(128, 128, 128, 0.6)',  # grey
    'rgba(0, 128, 128, 0.6)',    # teal
    'rgba(255, 192, 203, 0.6)',  # pink
    'rgba(0, 0, 255, 0.6)',      # blue
    'rgba(255, 255, 0, 0.6)'     # yellow
]
stage_colors = {}
for idx, stage in enumerate(stages_in_order):
    stage_colors[stage] = color_palette[idx % len(color_palette)]

# Initialize schedules and capacity trackers
module_tasks = []  # List to store tasks for all modules
step_schedules = {}  # Dictionary to track when each step is occupied
operator_schedules = {op: [] for op in operators}  # Schedules for operators

# Prepare data structures for steps and capacities
data = {}
capacities = {}
for item in data_list:
    stage = item['Stage']
    step = item['Step']
    duration = item['Duration']
    capacity_info = item['Capacity']
    step_key = (stage, step)

    # Build data dictionary with durations
    if stage not in data:
        data[stage] = {}
    data[stage][step] = duration

    # Build capacities dictionary
    if isinstance(capacity_info, dict):
        # Capacity changes over time
        capacity_changes = []
        for cap_str, date_str in capacity_info.items():
            cap_value = int(cap_str)
            effective_time = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
            capacity_changes.append((effective_time, cap_value))
        # Sort capacity changes by effective_time
        capacity_changes.sort(key=lambda x: x[0])
        capacities[step_key] = capacity_changes
    else:
        # Capacity is constant
        if capacity_info == 'infinite':
            capacities[step_key] = [(simulation_start, float('inf'))]
        else:
            capacities[step_key] = [(simulation_start, int(capacity_info))]

    # Initialize step schedules
    step_schedules[step_key] = []

# Function to check if a given time is a CERN holiday
@lru_cache(maxsize=None)
def is_cern_holiday(time):
    time_date = time.date()
    for start, end in cern_holidays:
        if start <= time_date <= end:
            return True
    return False

# Function to check if a given time is within working hours
@lru_cache(maxsize=None)
def is_working_time(time, operator=None):
    if is_cern_holiday(time):
        return False
    if operator and enable_operator_holidays:
        for holiday in operator_holidays.get(operator, []):
            if holiday[0] <= time.date() <= holiday[1]:
                return False
    weekday = time.weekday()
    time_of_day = time.time()
    if weekday in work_days and work_start <= time_of_day < work_end and not (lunch_start <= time_of_day < lunch_end):
        return True
    return False

# Function to check if the duration fits within working hours
# Maybe the issue might be solved here by adding a threshold for the duration
def is_within_working_hours(start_time, duration, requires_operator_presence=False):
    end_time = start_time + timedelta(minutes=duration)
    # Check if start_time and end_time are within working hours
    if not is_working_time(start_time):
        return False
    if not is_working_time(end_time - timedelta(minutes=1)):
        return False
    if start_time.time() < work_start or end_time.time() > work_end:
        return False
    if requires_operator_presence:
        # For steps requiring operator presence, ensure they don't overlap lunch break
        if (lunch_start <= start_time.time() < lunch_end) or \
           (lunch_start < end_time.time() <= lunch_end) or \
           (start_time.time() < lunch_start and end_time.time() > lunch_end):
            return False
    return True

# Function to find the next working time
def next_working_time(time, operator=None):
    while not is_working_time(time, operator):
        time += timedelta(minutes=time_increment)
    return time

# Function to find the next available time where the duration fits within working hours
def next_available_time(current_time, duration, requires_operator_presence=False):
    # Move time to the next working period where the duration fits
    current_date = current_time.date()
    max_date = simulation_start + timedelta(days=365 * 5)  # Set a maximum date limit
    while current_date < max_date.date():
        # Check if current_time is within working hours
        if current_time.time() < work_start or current_time.time() >= work_end or not is_working_time(current_time):
            # Move to the next workday at work_start
            current_date += timedelta(days=1)
            current_time = datetime.combine(current_date, work_start)
            continue
        # Check if the duration fits within working hours
        end_time = current_time + timedelta(minutes=duration)
        if end_time.time() > work_end:
            # Move to next day
            current_date += timedelta(days=1)
            current_time = datetime.combine(current_date, work_start)
            continue
        if requires_operator_presence:
            # Ensure it doesn't overlap lunch break
            if (lunch_start <= current_time.time() < lunch_end) or \
               (lunch_start < end_time.time() <= lunch_end) or \
               (current_time.time() < lunch_start and end_time.time() > lunch_end):
                # Move time to after lunch
                current_time = datetime.combine(current_time.date(), lunch_end)
                continue
        # Check if current_time and end_time are within working time
        if is_working_time(current_time) and is_working_time(end_time - timedelta(minutes=1)):
            return current_time
        else:
            current_time += timedelta(minutes=time_increment)
    # If we reach here, no suitable time was found
    return None

# Function to check if an operator is available at a given time
@lru_cache(maxsize=None)
def is_operator_available_at_time(operator, time):
    # Check general working hours and holidays
    if not is_working_time(time):
        return False
    # Check if operator is on holiday
    if enable_operator_holidays:
        if any(holiday[0] <= time.date() <= holiday[1] for holiday in operator_holidays.get(operator, [])):
            return False
    # Check operator-specific work hours
    weekday = time.weekday()
    operator_schedule = operator_work_hours.get(operator, {})
    daily_periods = operator_schedule.get(weekday, [])
    if not daily_periods:
        # Operator is not working on this day
        return False
    for period_start_time, period_end_time in daily_periods:
        if period_start_time <= time.time() < period_end_time:
            return True
    return False

# Function to find the earliest available operator
def find_operator(step_name, earliest_time, duration=0, requires_operator_presence=False):
    available_operators = []
    operator_duration = duration if requires_operator_presence else 15  # Assume 15 minutes needed for operator launch
    for op, steps in operators.items():
        # Exclude operators who cannot perform the step
        if steps == 'All_except_wire_bonding' and step_name == 'WB - Wire-bonding':
            continue
        if steps == 'All' or steps == 'All_except_wire_bonding' or step_name in steps:
            # Check if operator is available at the earliest_time
            if not is_operator_available_at_time(op, earliest_time):
                continue
            op_schedule = operator_schedules[op]
            # Check operator availability for the required duration
            is_available = all(entry['Finish'] <= earliest_time or entry['Start'] >= earliest_time + timedelta(minutes=operator_duration) for entry in op_schedule)
            if is_available:
                # For steps requiring operator presence, ensure availability throughout the duration
                if requires_operator_presence:
                    operator_end_time = earliest_time + timedelta(minutes=operator_duration)
                    current_time = earliest_time
                    while current_time < operator_end_time:
                        if not is_operator_available_at_time(op, current_time):
                            break
                        current_time += timedelta(minutes=time_increment)
                    else:
                        available_operators.append(op)
                        continue
                    # Operator not available for the full duration
                    continue
                else:
                    available_operators.append(op)
    if available_operators:
        return available_operators[0]  # Choose the first available operator
    else:
        return None

# Function to get capacity at a given time for a step
def get_capacity_at_time(step_key, time):
    capacity_changes = capacities[step_key]
    # Find the capacity effective at the given time
    for i in range(len(capacity_changes)):
        effective_time, cap_value = capacity_changes[i]
        if time < effective_time:
            if i == 0:
                # Time is before the first capacity change
                return cap_value
            else:
                # Return the capacity from the previous change
                return capacity_changes[i - 1][1]
    # If time is after all capacity changes, return the last capacity value
    return capacity_changes[-1][1]

# Function to find the earliest possible start time for a module's step
def find_module_start_time(step_schedule, duration, earliest_possible_start, step_key, requires_operator, launch_by_operator, step_name):
    time = earliest_possible_start

    requires_operator_presence = requires_operator

    if requires_operator_presence:
        max_continuous_working_minutes = 5 * 60  # Maximum continuous working period in minutes
        if duration > max_continuous_working_minutes:
            raise ValueError(f"Cannot schedule step '{step_name}' requiring operator presence throughout with duration {duration} minutes longer than maximum continuous working period of {max_continuous_working_minutes} minutes.")

    while True:
        # Adjust to next working time if operator is required or step needs to be launched
        if requires_operator or launch_by_operator:
            time = next_working_time(time, operator=None)
        # For steps requiring operator presence, ensure the entire duration fits within working hours
        if requires_operator_presence:
            if not is_within_working_hours(time, duration, requires_operator_presence):
                # Move time to next available working period
                time = next_available_time(time, duration, requires_operator_presence)
                if time is None:
                    raise ValueError(f"Cannot schedule step '{step_name}' requiring operator presence throughout with duration {duration} minutes.")
                continue
        elif launch_by_operator:
            # Ensure start time is within working hours
            if not is_working_time(time):
                time = next_working_time(time, operator=None)
        # Get the capacity at the current time
        capacity = get_capacity_at_time(step_key, time)
        # Check if the step is available at this time
        concurrent_tasks = sum(1 for s, e in step_schedule if not (e <= time or s >= time + timedelta(minutes=duration)))
        if concurrent_tasks < capacity:
            # If operator is required, check operator availability
            if requires_operator_presence or launch_by_operator:
                operator = find_operator(step_name, time, duration if requires_operator_presence else 15, requires_operator_presence)
                if operator:
                    return time, operator
                else:
                    # Operator not available at this time, move to next available time
                    time = next_working_time(time + timedelta(minutes=time_increment), operator=None)
                    continue
            else:
                return time, None
        else:
            # Jump to the earliest finish time among overlapping tasks
            overlapping_tasks = [(s, e) for s, e in step_schedule if not (e <= time or s >= time + timedelta(minutes=duration))]
            if overlapping_tasks:
                earliest_end_time = min(e for s, e in overlapping_tasks)
                time = earliest_end_time
            else:
                time += timedelta(minutes=time_increment)
    return time, None

# Generate Parylene shipment times (every specified weekday)
def generate_parylene_shipment_times(initial_time):
    shipment_times = []
    current_time = initial_time
    while current_time < simulation_start + timedelta(days=365*5):
        if current_time.weekday() == parylene_shipment_weekday:
            shipment_times.append(current_time.replace(hour=9, minute=0))
            current_time += timedelta(days=7)
        else:
            current_time += timedelta(days=1)
    return shipment_times

next_shipment_times = generate_parylene_shipment_times(simulation_start)

# Function to reserve a component unit
def reserve_component_unit(component, earliest_time):
    units = component_available_units[component]
    for idx, unit_time in enumerate(units):
        if unit_time <= earliest_time:
            # Reserve this unit
            reserved_unit_time = units.pop(idx)
            return earliest_time  # Return the module's earliest possible time
        elif unit_time > earliest_time:
            # Wait until the unit is available
            reserved_unit_time = units.pop(idx)
            return unit_time
    # If no units are available after earliest_time
    raise ValueError(f"No available units of {component} after {earliest_time}")

# Main scheduling loop
print("Scheduling modules...")
for module_id in tqdm(range(1, num_modules + 1)):
    module_ready_time = simulation_start
    components_reserved = {'bare modules': False, 'flexes': False}
    
    # For each module, process all steps
    for item_index, item in enumerate(data_list):
        stage = item['Stage']
        step = item['Step']
        duration = item['Duration']
        step_key = (stage, step)
        step_schedule = step_schedules[step_key]
        requires_operator = step in steps_requiring_operator
        launch_by_operator = step in steps_launch_by_operator

        # Determine if a component is needed at this step
        component_needed = None
        if 'bare modules' in step.lower():
            component_needed = 'bare modules'
        elif 'flexes' in step.lower():
            component_needed = 'flexes'

        # Wait until the previous step is completed
        if item_index == 0:
            # First step, module can proceed after component arrival
            if component_needed and not components_reserved[component_needed]:
                try:
                    component_available_time = reserve_component_unit(component_needed, module_ready_time)
                    module_ready_time = max(module_ready_time, component_available_time)
                    components_reserved[component_needed] = True
                except ValueError as e:
                    print(f"Error scheduling module {module_id}, step '{step}': {e}")
                    break  # Skip to next module
            else:
                module_ready_time = max(module_ready_time, simulation_start)
        else:
            # Add transition delay
            module_ready_time = module_ready_time + timedelta(minutes=transition_delay)
            # Check if component is needed at this step
            if component_needed and not components_reserved[component_needed]:
                try:
                    component_available_time = reserve_component_unit(component_needed, module_ready_time)
                    module_ready_time = max(module_ready_time, component_available_time)
                    components_reserved[component_needed] = True
                except ValueError as e:
                    print(f"Error scheduling module {module_id}, step '{step}': {e}")
                    break  # Skip to next module

        # Find when the step is available
        earliest_possible_start = module_ready_time

        # Special handling for Parylene Transit
        if step == 'Transit' and stage == 'Parylene':
            # Find the next shipment time after earliest_possible_start
            shipment_times_after_ready = [t for t in next_shipment_times if t >= earliest_possible_start]
            if shipment_times_after_ready:
                batch_start_time = shipment_times_after_ready[0]
            else:
                batch_start_time = earliest_possible_start
            operator = None
        else:
            # Find when the step is available
            try:
                batch_start_time, operator = find_module_start_time(
                    step_schedule,
                    duration,
                    earliest_possible_start,
                    step_key,
                    requires_operator,
                    launch_by_operator,
                    step
                )
            except ValueError as e:
                print(f"Error scheduling module {module_id}, step '{step}': {e}")
                break  # Skip to next module

        finish_time = batch_start_time + timedelta(minutes=duration)

        # Schedule task for the module
        task = {
            'Module': f"Module {module_id}",
            'Stage': stage,
            'Task': step,
            'Start': batch_start_time,
            'Finish': finish_time,
            'Operator': operator
        }
        module_tasks.append(task)

        # Update module's current time
        module_ready_time = finish_time

        # Update step schedule
        step_schedule.append((batch_start_time, finish_time))

        # Schedule operator time if required
        if (requires_operator or launch_by_operator) and operator:
            if requires_operator:
                operator_time = timedelta(minutes=duration)
            else:
                operator_time = timedelta(minutes=15)
            operator_end_time = batch_start_time + operator_time
            # Assign operator
            operator_schedules[operator].append({
                'Start': batch_start_time,
                'Finish': operator_end_time,
                'Step': step,
                'Stage': stage
            })

# Check if any tasks were scheduled
if not module_tasks:
    print("No modules were successfully scheduled. Exiting.")
    exit()

# Convert times to numerical values (timestamps in milliseconds)
for task in module_tasks:
    task['Start_ms'] = task['Start'].timestamp() * 1000  # Convert to milliseconds
    task['Finish_ms'] = task['Finish'].timestamp() * 1000
    task['Duration_ms'] = task['Finish_ms'] - task['Start_ms']
    task['Step_Full'] = f"{task['Stage']} - {task['Task']}"

# Convert module_tasks to DataFrame
throughput_df = pd.DataFrame(module_tasks)

# Generate Throughput Analysis Over Time Plot
print("Generating Throughput Analysis Over Time Plot...")
# Add FinishDate to throughput_df
throughput_df['FinishDate'] = throughput_df['Finish'].dt.normalize()

# Identify the final step in the process
final_step = data_list[-1]
final_stage = final_step['Stage']
final_task = final_step['Step']

# Filter tasks that are the final step
final_tasks = throughput_df[(throughput_df['Stage'] == final_stage) & (throughput_df['Task'] == final_task)]

# Extract finish dates of modules
module_finish_times = final_tasks.groupby('Module')['Finish'].max().reset_index()
module_finish_times['FinishDate'] = module_finish_times['Finish'].dt.normalize()  # Use datetime

# Generate a date range from the earliest to latest finish date
earliest_arrival_date = min(batch['arrival_time'].date() for batches in component_arrival_batches.values() for batch in batches)
start_date = min(simulation_start.date(), earliest_arrival_date)

date_range = pd.date_range(start=start_date, end=module_finish_times['FinishDate'].max())

# Create a DataFrame with all dates and cumulative modules completed
cumulative_modules = []
cumulative_count = 0
for date in date_range:
    daily_count = module_finish_times[module_finish_times['FinishDate'] == date].shape[0]
    cumulative_count += daily_count
    cumulative_modules.append({'Date': date, 'CumulativeModules': cumulative_count})

throughput_counts = pd.DataFrame(cumulative_modules)

# Calculate modules that have passed a specific step ('PDB Upload of Sensor IV')
pdb_upload_sensor_iv_tasks = throughput_df[
    (throughput_df['Stage'] == 'Reception') &
    (throughput_df['Task'] == 'Rec - PDB Upload of Sensor IV')
]

# Extract finish dates of modules for this step
pdb_module_finish_times = pdb_upload_sensor_iv_tasks.groupby('Module')['Finish'].max().reset_index()
pdb_module_finish_times['FinishDate'] = pdb_module_finish_times['Finish'].dt.normalize()

# Generate a date range for this step
pdb_date_range = pd.date_range(start=start_date, end=pdb_module_finish_times['FinishDate'].max())

# Create a DataFrame with cumulative modules for this step
pdb_cumulative_modules = []
pdb_cumulative_count = 0
for date in pdb_date_range:
    daily_count = pdb_module_finish_times[pdb_module_finish_times['FinishDate'] == date].shape[0]
    pdb_cumulative_count += daily_count
    pdb_cumulative_modules.append({'Date': date, 'CumulativePDBModules': pdb_cumulative_count})

pdb_throughput_counts = pd.DataFrame(pdb_cumulative_modules)

# Merge the cumulative counts
full_date_range = pd.date_range(
    start=start_date,
    end=max(throughput_counts['Date'].max(), pdb_throughput_counts['Date'].max())
)

# Create a DataFrame with all dates
full_counts_df = pd.DataFrame({'Date': full_date_range})

# Merge the cumulative counts
full_counts_df = full_counts_df.merge(throughput_counts, on='Date', how='left')
full_counts_df = full_counts_df.merge(pdb_throughput_counts, on='Date', how='left')

# Fill NaN values with previous values (since cumulative counts)
full_counts_df['CumulativeModules'] = full_counts_df['CumulativeModules'].ffill().fillna(0)
full_counts_df['CumulativePDBModules'] = full_counts_df['CumulativePDBModules'].ffill().fillna(0)

# Compute cumulative available bare modules over time
# Build a DataFrame with component arrivals
component_arrivals_list = []
for component, batches in component_arrival_batches.items():
    for batch in batches:
        arrival_time = batch['arrival_time']
        quantity = batch['quantity']
        component_arrivals_list.append({'Component': component, 'ArrivalTime': arrival_time, 'Quantity': quantity})

component_arrivals_df = pd.DataFrame(component_arrivals_list)

# Extract arrival dates and quantities for bare modules
bare_module_arrivals = component_arrivals_df[component_arrivals_df['Component'] == 'bare modules'][['ArrivalTime', 'Quantity']].copy()
bare_module_arrivals['Date'] = bare_module_arrivals['ArrivalTime'].dt.normalize()
bare_module_arrivals = bare_module_arrivals.groupby('Date')['Quantity'].sum().reset_index()
# Compute cumulative sum
bare_module_arrivals = bare_module_arrivals.sort_values('Date')
bare_module_arrivals['AvailableBareModules'] = bare_module_arrivals['Quantity'].cumsum()

# Merge with full_counts_df
full_counts_df = full_counts_df.merge(bare_module_arrivals[['Date', 'AvailableBareModules']], on='Date', how='left')
# Fill NaN values
full_counts_df['AvailableBareModules'] = full_counts_df['AvailableBareModules'].ffill().fillna(0)

# Calculate the number of modules that have not yet passed the specific step
full_counts_df['ModulesNotPassedPDBUploadSensorIV'] = (
    full_counts_df['AvailableBareModules'] - full_counts_df['CumulativePDBModules']
).clip(lower=0)  # Ensure no negative values

# Prepare data for cumulative modules completed per stage
stage_completion = throughput_df.groupby(['Module', 'Stage'])['Finish'].max().reset_index()
stage_completion['FinishDate'] = stage_completion['Finish'].dt.normalize()

# Create a pivot table with stages as columns and dates as index
stage_pivot = stage_completion.pivot_table(index='FinishDate', columns='Stage', values='Module', aggfunc='count')

# Generate a complete date range covering all relevant dates
all_dates = pd.date_range(start=start_date, end=full_counts_df['Date'].max())

# Reindex stage_pivot with all_dates to ensure all dates are included
stage_pivot = stage_pivot.reindex(all_dates).fillna(0)

# Compute cumulative sum
stage_pivot = stage_pivot.cumsum()

# Merge stage completion data into full_counts_df
full_counts_df = full_counts_df.set_index('Date')
for stage in stage_pivot.columns:
    full_counts_df[f'Cumulative_{stage}'] = stage_pivot[stage]
full_counts_df = full_counts_df.reset_index()

# Generate the Throughput Analysis Over Time plot
throughput_fig = go.Figure()

# Existing traces
throughput_fig.add_trace(go.Scatter(
    x=full_counts_df['Date'],
    y=full_counts_df['CumulativeModules'],
    mode='lines+markers',
    name='Cumulative Modules Completed',
    line=dict(width=4)
))

throughput_fig.add_trace(go.Scatter(
    x=full_counts_df['Date'],
    y=full_counts_df['ModulesNotPassedPDBUploadSensorIV'],
    mode='lines+markers',
    name='Bare Modules still in Reception',
    line=dict(width=4)
))

# Add cumulative modules completed per stage
for stage in stages_in_order:
    column_name = f'Cumulative_{stage}'
    if column_name in full_counts_df.columns:
        throughput_fig.add_trace(go.Scatter(
            x=full_counts_df['Date'],
            y=full_counts_df[column_name],
            mode='lines+markers',
            name=f'# {stage} Completed',
            line=dict(color=stage_colors[stage]),
            #stackgroup='one'  # Optional: Uncomment to stack the areas
        ))

# Calculate date range in days
date_range_in_days = (full_counts_df['Date'].max() - full_counts_df['Date'].min()).days

# Set dtick dynamically based on the range
if date_range_in_days > 180:  # If more than 6 months, use monthly intervals
    dtick_value = 'M1'  # One month intervals
elif date_range_in_days > 30:  # If more than a month, use weekly intervals
    dtick_value = 7 * 86400000  # One week intervals in milliseconds
else:
    dtick_value = 86400000  # One day intervals in milliseconds

throughput_fig.update_layout(
    title='Throughput Analysis Over Time',
    xaxis_title='Date',
    yaxis_title='Number of Modules',
    xaxis=dict(
        tickformat='%Y-%m-%d',
        dtick=dtick_value,  # Dynamically set based on range
        tickangle=45,
        rangeslider=dict(visible=True),
    ),
    yaxis=dict(
        rangemode='nonnegative',
        fixedrange=False
    ),
    hovermode='x unified',
    legend=dict(traceorder='normal')  # Preserve the order of legend entries
)

throughput_fig.show()

# Generate Number of Modules in Each Stage Over Time Plot
print("Generating Number of Modules in Each Stage Over Time Plot...")
# Calculate the number of modules in each stage over time
modules_in_stage_over_time = []

# Get unique dates from module tasks
all_dates = pd.date_range(start=start_date, end=throughput_df['Finish'].max().normalize())

for date in all_dates:
    date = pd.Timestamp(date)
    date_plus_one = date + pd.Timedelta(days=1)
    # Filter tasks that are ongoing on this date
    tasks_on_date = throughput_df[
        (throughput_df['Start'] < date_plus_one) & (throughput_df['Finish'] >= date)
    ]
    # For each stage, count unique modules
    stage_counts = tasks_on_date.groupby('Stage')['Module'].nunique().reset_index()
    stage_counts['Date'] = date
    modules_in_stage_over_time.append(stage_counts)

# Combine into a single DataFrame
modules_in_stage_over_time_df = pd.concat(modules_in_stage_over_time, ignore_index=True)

# Pivot the DataFrame to have stages as columns
modules_in_stage_pivot = modules_in_stage_over_time_df.pivot_table(
    index='Date',
    columns='Stage',
    values='Module',
    aggfunc='sum',
    fill_value=0
)

# Ensure all stages are included
for stage in stages_in_order:
    if stage not in modules_in_stage_pivot.columns:
        modules_in_stage_pivot[stage] = 0

# Sort columns according to stages_in_order
modules_in_stage_pivot = modules_in_stage_pivot[stages_in_order]

# Plot the data
stage_over_time_fig = go.Figure()

for stage in stages_in_order:
    if stage in modules_in_stage_pivot.columns:
        stage_over_time_fig.add_trace(go.Scatter(
            x=modules_in_stage_pivot.index,
            y=modules_in_stage_pivot[stage],
            mode='lines',
            name=stage,
            stackgroup='one',
            line=dict(color=stage_colors[stage])
        ))

stage_over_time_fig.update_layout(
    title='Number of Modules in Each Stage Over Time',
    xaxis_title='Date',
    yaxis_title='Number of Modules',
    xaxis=dict(
        tickformat='%Y-%m-%d',
        tickangle=45,
        rangeslider=dict(visible=True),
    ),
    yaxis=dict(
        rangemode='nonnegative',
        fixedrange=False
    ),
    hovermode='x unified',
    legend=dict(traceorder='normal')  # Preserve the order of legend entries
)

stage_over_time_fig.show()

# Generate Gantt Chart of Module Processing
print("Generating Gantt Chart of Module Processing...")
# Sort tasks by module and start time
module_tasks_sorted = sorted(module_tasks, key=lambda x: (int(x['Module'].split()[1]), x['Start']))
module_gantt_df = pd.DataFrame(module_tasks_sorted)

# Assign colors to stages
stage_list = module_gantt_df['Stage'].unique()
color_map = {stage: stage_colors.get(stage, 'rgb(0,0,0)') for stage in stage_list}

# Create Gantt chart
fig_gantt = px.timeline(
    module_gantt_df,
    x_start="Start",
    x_end="Finish",
    y="Module",
    color="Stage",
    color_discrete_map=color_map,
    category_orders={"Stage": stages_in_order}  # Use stages_in_order
)

fig_gantt.update_layout(
    title='Gantt Chart of Module Processing',
    xaxis_title='Time',
    yaxis_title='Module',
    xaxis=dict(
        tickformat='%Y-%m-%d %H:%M',
        rangeslider=dict(visible=True),
    ),
    yaxis=dict(autorange="reversed"),  # Modules in ascending order
    hovermode='x unified',
    legend=dict(traceorder='normal')  # Preserve the order of legend entries
)

fig_gantt.show()

# Generate Operator Workload Plot
print("Generating Operator Workload Plot...")
operator_tasks_list = []
for operator, tasks in operator_schedules.items():
    for task in tasks:
        operator_tasks_list.append({
            'Operator': operator,
            'Start': task['Start'],
            'Finish': task['Finish'],
            'Step': task['Step'],
            'Stage': task['Stage']
        })

operator_df = pd.DataFrame(operator_tasks_list)

if not operator_df.empty:
    operator_df['Duration'] = (operator_df['Finish'] - operator_df['Start']).dt.total_seconds() / 60.0  # Duration in minutes

    fig_operator = px.timeline(
        operator_df,
        x_start="Start",
        x_end="Finish",
        y="Operator",
        color="Stage",
        hover_data=['Step'],
        color_discrete_map=stage_colors,
        category_orders={"Stage": stages_in_order}
    )

    fig_operator.update_layout(
        title='Operator Workload Over Time',
        xaxis_title='Time',
        yaxis_title='Operator',
        xaxis=dict(
            tickformat='%Y-%m-%d %H:%M',
            rangeslider=dict(visible=True),
        ),
        hovermode='x unified',
        legend=dict(traceorder='normal')  # Preserve the order of legend entries
    )

    fig_operator.show()
else:
    print("No operator tasks were scheduled.")

# Print script execution time
end_time = time.time()
execution_time = end_time - start_time

hours, rem = divmod(execution_time, 3600)
minutes, seconds = divmod(rem, 60)
print(f"Script execution time: {int(hours)} hours, {int(minutes)} minutes, {seconds:.2f} seconds")
