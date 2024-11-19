class CSHarvesterException(Exception):
    pass


class LabelError(CSHarvesterException):
    pass


class ConcurrentHarvesting(CSHarvesterException):
    pass
