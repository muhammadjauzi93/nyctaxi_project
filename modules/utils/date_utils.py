from datetime import date
from dateutil.relativedelta import relativedelta

# Obtains the year-month for the specified number of months prior to the current month in yyyy-MM format

def get_target_yyyymm(months_ago: int) -> str:
    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime('%Y-%m')

def get_month_start_n_months_ago(months_ago: int) -> date:
    """Get the first day of the month for a date that is 'n' number of months ago.

    Args:
        months_ago (int): The number of months to go back from the current date.

    Returns:
        date: The first day of the month for the calculated date.
    """
    
    return date.today().replace(day=1) - relativedelta(months=months_ago)