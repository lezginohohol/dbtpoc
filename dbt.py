import sys, os, argparse

commands = {
    "READ" : "read spreadsheet and create Json schemas and data files",
    "CONFIG" : "configuration commands",
    "WRITE" : "write to database"
}

commands = {}

class CommandType(type):
    def __init__(cls, name, bases, attrs):
        super(CommandType, cls).__init__(name, bases, attrs)
        name = getattr(cls, name, cls.__name__.lower())
        cls.name = name
        if name != 'command':
            commands[name] = cls

Command = CommandType('Command', (object,), {'run': lambda self, args: None})

class Help(Command):
    """Display the list of available commands"""
    def run(self, args):
        print("Available commands:\n")
        names = list(commands)
        padding = max([len(k) for k in names]) + 2
        for key in sorted(names):
            name = key.ljust(padding, ' ')
            doc = (commands[k].__doc__ or '').strip()
            print("    %s%s" % (name, doc))
        print ("\nUse '%s <command> --help' for individual command help" % sys.argv[0].split(os.path.sep)-1)


parser = argparse.ArgumentParser("Read spreadsheet write to DB")
parser.add_argument("command", nargs= '*', metavar = 'commandargs', type=str,
                    help = "subprogram with names. Awailable subprograms:"
                           "READ : read spreadsheet and create Json schemas and data files."
                           "       "
                           "CONFIG : configuration commands. credentials defaults etc"
                           "WRITE : write to database")


def main():
    args = sys.argv[1:]
    command = args[0]
    args = args[1:]

    if command in commands:
        o = commands[command]()
        o.run(args)
    else:
        sys.exit('Unknow command %r' % (command,))