import os
import subprocess
import getpass
import re
import sys
from pathlib import Path

import celery.exceptions
import psutil
from celery import Celery
from celery.schedules import crontab

import celeryconfig

BASE_DIR = Path(__file__).resolve().parent
TABS_DIR = BASE_DIR / "crontabs"

app = Celery("cron", broker='redis://localhost:6379/0')
app.config_from_object(celeryconfig.__name__)


def get_current_path(user: str) -> 'Path':
    """
    Get and return path to user crontab file
    :param user: string with username
    :return: Path to file
    """

    return TABS_DIR / f"{user}_crontab.txt"


def redact_file(user: str, number=None) -> None:
    """
    Change task from user file. If number not None ,
    try to find task with this number,
    else change all tasks.
    In all situation save new data
    :param user: string with username
    :param number: number of task
    :return: None
    """

    path = get_current_path(user)

    with open(path, 'r') as file:
        lines = file.readlines()

    tasks = lines[3:]
    try:
        if number:
            print(f"Your task with number {number} is: {tasks[number - 1]}")
            tasks[number - 1] = create_crontab_row(user)
        else:
            for i in range(len(tasks)):
                print(f"Your task is: {tasks[i]}")
                tasks[i] = create_crontab_row(user)
    except IndexError:
        print("You don't have task with this index")
    finally:
        lines = lines[:3] + tasks

        with open(path, 'w') as f:
            f.writelines(lines)


def add_crontab_row(user: str, task_string: str) -> None:
    """
    Write string with task into user crontab file
    :param user: string with username
    :param task_string: string with task
    :return:
    """
    path = get_current_path(user)

    with open(path, "a") as f:
        f.write(task_string)


def create_crontab_row(user: str) -> str:
    """
    Write on console ,and take data from user.
    Return string for crontab file
    :param user: string with username
    :return: string with task for crontab file
    """

    task_string = ""
    try:
        minutes = input("Minutes(from 1 to 59 or * if nothing): ")

        minutes_pattern = r'^\*|[1-9]|[1-5][0-9]$'
        if not re.match(minutes_pattern, minutes):
            raise ValueError("Must be number from 1 to 59 or *")

        hours = input("Hours(from 0 to 23 or * if nothing): ")

        hours_pattern = r'^\*|[1-9]|1[0-9]|2[0-3]$'
        if not re.match(hours_pattern, hours):
            raise ValueError("Must be number from 0 to 23 or *")

        day_of_month = input("Day of month(from 1 to 31 or * if nothing): ")

        days_pattern = r'^\*|[1-9]|1[0-9]|2[0-9]|3[0-1]$'
        if not re.match(days_pattern, day_of_month):
            raise ValueError("Must be number from 1 to 31 or *")

        month = input("Month(from 1 to 12 or * if nothing): ")

        month_pattern = r'^\*|[1-9]|1[0-2]$'
        if not re.match(month_pattern, month):
            raise ValueError("Must be number from 1 to 12 or *")

        day_of_weak = input("Day of weak(from 1 to 7 or * if nothing): ")

        day_of_weak_pattern = r'^\*|[1-7]$'
        if not re.match(day_of_weak_pattern, day_of_weak):
            raise ValueError("Must be number from 1 to 7 or *")

        command = input("Your command: ")

        if not command:
            raise ValueError("Command length must be bigger than 0 ")
    except ValueError as e:
        print(f"\n{e}\n")
    else:
        task_string = f"{minutes} {hours} {day_of_month} {month} {day_of_weak} {user} {command} \n"

    return task_string


def delete_user_crontab_file(user: str) -> None:
    """
    Open user crontab file, delete all tasks
    and write back
    :param user: string with username
    :return: None
    """

    path = get_current_path(user)

    with open(path, 'r') as file:
        lines = file.readlines()

    updated_lines = lines[:3]

    with open(path, 'w') as file:
        file.writelines(updated_lines)


def take_user_tasks(user: str) -> map or None:
    """
    Open user crontab file, if tasks exist,
    update and return , else None
    :param user: string with username
    :return: map object or None
    """

    path = get_current_path(user)

    with open(path, "r") as f:
        lines = f.readlines()[3:]

        if lines:
            tasks = map(lambda x: x.replace("\n", ""), lines)
        else:
            tasks = None

    return tasks


def create_crontab_file(path: 'Path') -> None:
    """
    Create crontab file in folder 'crontabs'
    for user with base structure
    :param path: string with path to 'crontabs' folder
    :return: None
    """

    structure = f"PATH = {path}\n\n" \
                f"# m h dom mon dow user command \n"

    with open(path, "w+") as f:
        f.writelines(structure)


def add_celery_task(task_name: str, minute: str, hour: str, day_of_month: str,
                    month_of_year: str, day_of_weak: str, command: str) -> None:
    """
    Create new celery task on beat config
    :param task_name: string with name of task
    :param minute: minute from 1 to 59 or *
    :param hour: hour from 0 to 23 or *
    :param day_of_month: day from 1 to 31 or *
    :param month_of_year: month from 1 to 12 or *
    :param day_of_weak: day of weak from 1 to 7 or *
    :param command: string with console command
    :return: None
    """

    app.conf.beat_schedule[task_name] = {
        "task": f"{task_name}",
        'schedule': crontab(minute=minute, hour=hour,
                            day_of_month=day_of_month, month_of_year=month_of_year, day_of_week=day_of_weak),
        "args": (command,),
    }


def create_celery_task(user: str, task_number: int, task: str) -> None:
    """
    Create function for celery task, with name and user
    :param user: string with username
    :param task_number: number of task
    :param task: string with task like '* * * * * user echo Hello World'
    :return: None
    """

    task = task.split(' ')
    task_name = f"task_{user}_{task_number}"

    @app.task(name=task_name, user=user)
    def user_task(command):
        subprocess.run(command, shell=True)

    add_celery_task(task_name, task[0], task[1], task[2], task[3], task[4], " ".join(task[6:]))


def check_tasks_from_file(user: str) -> None:
    """
    Check user crontab fyle on exist tasks,
    If tasks exist run function 'create_celery_task',
    else do nothing
    :param user: string with username
    :return: None
    """

    tasks = take_user_tasks(user)

    if tasks:
        i = 1
        for task in tasks:
            create_celery_task(user, i, task)
            i += 1


def check_crontab_files(users_list: list) -> None:
    """
    Get list of usernames and check ,
    if user have crontab file ,
    run function 'check_tasks_from_file',
    else create file for user
    :param users_list: list of usernames
    :return: None
    """

    for user in users_list:
        path = get_current_path(user)

        if Path(path).is_file():
            check_tasks_from_file(user)
        else:
            create_crontab_file(path)


def take_users() -> list:
    """
    Take all system users, and take names
    :return: List of usernames
    """

    user_list = psutil.users()

    users_names = map(lambda x: x.name, user_list)

    return list(users_names)


def print_help_info() -> None:
    """
    Write into console information about
    this program,all functions/flags/args
    :return: None and write text into console
    """

    print(f"-python/py cron.py [flag] [atr] \n "
          f"   python/py cron.py - create in folder 'crontabs' special files for all users \n"
          f"                   -l - check all your tasks \n"
          f"                   -r - delete all your tasks \n"
          f"                   -a - add new task\n"
          f"                   -e [number] - if have number,change task with this number,\n"
          f"                                 if not all your tasks\n"
          f"                   -h - help info about this program")


def start_workers() -> None:
    """
    After start, try to delete all workers,
    if that was working, and , after that,
    create workers,for all tasks, with argument beat
    :return: None and create workers
    """

    try:
        os.system("celery control shutdown")
    except celery.exceptions.CeleryError:
        pass
    finally:
        app.worker_main(["worker", '--beat'])


def main() -> None:
    """
    Main method are using like controller,
    take main info,like command/flags/args
    and redirect to funcs
    :return: None, or write text into the terminal(console)
    """

    command = ' '.join(sys.argv[1:])
    current_user = getpass.getuser()

    match command:
        case "":
            users = take_users()
            check_crontab_files(users)

            if app.conf.beat_schedule:
                start_workers()

        case "-l":
            tasks = take_user_tasks(current_user)

            if tasks:
                for task in tasks:
                    print(task)
            else:
                print("Your crontab file is empty")

        case "-r":
            delete_user_crontab_file(current_user)
            print("Success!")
        case "-a":
            task_string = create_crontab_row(current_user)

            if task_string:
                add_crontab_row(current_user, task_string)

        case s if s.startswith("-e"):
            try:
                args = command.split(" ")

                number = None

                if len(args) == 2:
                    number = int(args[-1])
                elif len(args) > 2:
                    raise AttributeError("python cron.py -e [arg] take only one argument!")

            except ValueError:
                print("\n Argument must be a number \n")
            except AttributeError as e:
                print(f"\n{e}\n")
            else:
                redact_file(current_user, number)

        case "-h":
            print_help_info()
        case "-k":
            pass
        case _:
            print("Incorrect command or flag! \n")
            print_help_info()


if __name__ == "__main__":
    main()
