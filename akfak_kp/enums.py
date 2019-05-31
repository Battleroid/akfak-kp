from enum import IntEnum


class Level(IntEnum):
    """
    Status enum normal through disaster level (for feeding to Zabbix)
    """

    normal = 0
    average = 1
    high = 2
    disaster = 3

    def __repr__(self):
        return f'<{self.name}: {self.value}>'

    def __str__(self):
        return f'{self.name}: {self.value}'
