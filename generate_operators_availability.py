from ObjectOrientedProductionSimulation import *

# Extract the holidays, and daily shift from the data
dict_int_to_day = {"0": "Monday", "1": "Tuesday", "2": "Wednesday", "3": "Thursday", "4": "Friday"}

def generate_operators_availability(date) -> None:
    
    whole_day = Interval(date, date + timedelta(days=1))    #TODO: big issue with whole_day, need to bound it to the working hours of the laboratory
    for operator in operators:

        if operator.holidays.overlaps(whole_day) or date.weekday() in {5,6}:  # Check if the day is either in the holidays or weekend
            continue  

        else:
        
            today_shift = data["OperatorWorkHours"][operator.name][dict_int_to_day[str(date.weekday())]]
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


