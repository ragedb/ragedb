class Date(str):
    def __new__(cls, year, month, day):
        val_str = f"{year:04d}-{month:02d}-{day:02d}"
        return super().__new__(cls, val_str)

class DateTime(str):
    def __new__(cls, year, month, day, hour=0, minute=0, second=0, tz=None):
        val_str = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}"
        if tz:
            val_str += "Z"
        return super().__new__(cls, val_str)

def date(year, month, day):
    return Date(year, month, day)

def datetime(year, month, day, hour=0, minute=0, second=0, tz=None):
    return DateTime(year, month, day, hour=hour, minute=minute, second=second, tz=tz)
