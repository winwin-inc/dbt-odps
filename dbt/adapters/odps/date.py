import re
from datetime import timedelta, datetime, timezone
from dateutil.relativedelta import relativedelta
import pytz

LOCAL_TIMEZONE = pytz.timezone("Asia/Shanghai")
MAX_DATE = '9999-12-31'


class LocalDate(object):
    def __init__(self, date):
        """

        :type date: datetime
        """
        self.date = date.astimezone(LOCAL_TIMEZONE)

    def year(self):
        return self.date.year

    def month(self):
        return self.date.month

    def week(self):
        return self.date.weekday()

    def day(self):
        return self.date.day

    def get_date(self):
        return self.date

    def get_utc(self):
        return self.date.astimezone(timezone.utc)

    def fmt(self, fmt="%Y%m%d"):
        return self.date.strftime(fmt)

    def format(self, fmt="%Y%m%d"):
        return self.date.strftime(fmt)

    def to_date_string(self):
        return self.format('%Y-%m-%d')

    @staticmethod
    def today():
        return LocalDate(datetime.now())

    @staticmethod
    def yesterday():
        return LocalDate(datetime.now() - timedelta(days=1))

    def __eq__(self, other):
        return self.format() == other.format()

    def __str__(self):
        return self.format()

    def __hash__(self):
        return hash(self.format())

    def add_days(self, days=1):
        return LocalDate(self.date + timedelta(days=days))

    def sub_days(self, days=1):
        return self.add_days(-1 * days)

    def add_months(self, months=1):
        return LocalDate(self.date + relativedelta(months=months))

    def sub_months(self, months=1):
        return self.add_months(-1 * months)

    def add_weeks(self, weeks=1):
        return LocalDate(self.date + timedelta(days=7 * weeks))

    def sub_weeks(self, weeks=1):
        return LocalDate(self.date + timedelta(days=-7 * weeks))

    def start_of_month(self):
        return LocalDate(self.date.replace(day=1))

    def end_of_month(self):
        return LocalDate((self.date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1))

    def is_end_of_month(self):
        return self.date.day == self.end_of_month().day()

    def start_of_week(self):
        return LocalDate(self.date - timedelta(days=self.date.weekday()))

    def end_of_week(self):
        return LocalDate(self.date + timedelta(days=6 - self.date.weekday()))

    def start_of_quarter(self):
        return LocalDate(self.date.replace(month=(self.month() + 2) // 3 * 3 - 2, day=1))

    def end_of_quarter(self):
        return LocalDate(self.date.replace(month=(self.month() + 2) // 3 * 3, day=1))


def parse_date(datestr: str) -> LocalDate:
    if re.match(r'^\d{4}-\d{2}-\d{2}$', datestr):
        return LocalDate(datetime.strptime(datestr, '%Y-%m-%d'))
    elif re.match(r'^\d{8}$', datestr):
        return LocalDate(datetime.strptime(datestr, '%Y%m%d'))
    else:   # iso date
        return LocalDate(datetime.fromisoformat(datestr))

def local(date) -> LocalDate:
    """
    Jinjia2 filter for date

    :type date: datetime.datetime
    """
    return LocalDate(date)


def today() -> LocalDate:
    return LocalDate.today()


def yesterday() -> LocalDate:
    return LocalDate.yesterday()


def fmt(date, fmt="%Y%m%d"):
    return LocalDate(date).format(fmt)


def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    """
    Get a datetime object representing `n` days ago. By default the time is
    set to midnight.
    :param n: days
    :type n: int

    :param hour: hour
    :type hour: int

    :type minute: int

    :param microsecond:
    :param second:
    """
    today = datetime.now().replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)
    return today - timedelta(days=n)
