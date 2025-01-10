from ObjectOrientedProductionSimulation import *

# Extract the holidays, and daily shift from the data
dict_int_to_day = {"0": "Monday", "1": "Tuesday", "2": "Wednesday", "3": "Thursday", "4": "Friday", "5": "Saturday", "6": "Sunday"}
today_shift = data["OperatorWorkHours"]["Operator1"][dict_int_to_day[str(date.weekday())]]
list_today_shift = list(today_shift.keys())

def generate_operators_availability(date) -> None:
    whole_day = Interval(date, date + timedelta(days=1))
    for operator in operators:
        if operator.holidays.overlaps(whole_day) or date.weekday() in {5,6}:  # Check if the day is either in the holidays or weekend
            break
        else:
            for i in range(len(list_today_shift)):
                if i == len(list_today_shift) - 1 : # We expect an even number of constraints on daily shift (i.e. start time and end time)
                    break
                else :
                    start_time = str(date)[:10] + " " + today_shift[list_today_shift[i]]
                    start_time = datetime.fromisoformat(start_time)
                    end_time = str(date)[:10] + " " + today_shift[list_today_shift[i + 1]]
                    end_time = datetime.fromisoformat(end_time)
                    operator.availability.add(Interval(start_time, end_time, "idle"))


