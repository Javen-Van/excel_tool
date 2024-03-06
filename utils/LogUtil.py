def info(log: str, *args):
    statement = '[INFO]' + log
    print(statement.format(args))


def error(log: str, *args):
    statement = '[ERROR]' + log
    print(statement.format(args))
