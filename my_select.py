from sqlalchemy import func, desc, and_, distinct, select
from prettytable import PrettyTable
import sys

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    print("1.Знайти 5 студентів із найбільшим середнім балом з усіх предметів.")

    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    table = PrettyTable(['Студент', 'Середній бал'])
    for row in result:
        table.add_row(row)
    return table


def select_2():
    print("2.Знайти студента із найвищим середнім балом з певного предмета.")

    result = session.query(Discipline.name, Student.fullname, func.round(func.avg(Grade.grade), 2)) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == 8) \
        .group_by(Discipline.name, Student.fullname) \
        .order_by(func.avg(Grade.grade).desc()) \
        .first()

    discipline_name, fullname, avg_grade = result
    table = PrettyTable(['Предмет', 'Студент', 'Найвищій середній бал'])
    table.add_row([discipline_name, fullname, avg_grade])
    return table


def select_3():
    print("3.Знайти середній бал у групах з певного предмета.")

    result = session.query(Group.name, Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .join(Discipline) \
        .filter(Discipline.id == 8) \
        .group_by(Group.name, Discipline.name) \
        .order_by(Group.name) \
        .all()
    table = PrettyTable(['Група', 'Предмет', 'Середній бал'])
    for row in result:
        table.add_row(row)
    return table


def select_4():
    print("4.Знайти середній бал на потоці (по всій таблиці оцінок).")

    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .group_by(Group.name) \
        .order_by(Group.name) \
        .all()
    table = PrettyTable(['Група', 'Середній бал на потоці'])
    for row in result:
        table.add_row(row)
    return table


def select_5():
    print("5.Знайти, які курси читає певний викладач.")

    result = session.query(Discipline.name, Teacher.fullname) \
        .select_from(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == 1) \
        .all()
    table = PrettyTable(['Предмет', 'Викладач'])
    for row in result:
        table.add_row(row)
    return table


def select_6():
    print("6.Знайти список студентів у певній групі.")

    result = session.query(Group.name, Student.fullname) \
        .select_from(Student) \
        .join(Group) \
        .filter(Group.id == 1) \
        .all()
    table = PrettyTable(['Група', 'Студент'])
    for row in result:
        table.add_row(row)
    return table


def select_7():
    print("7.Знайти оцінки студентів в окремій групі з певного предмета.")

    result = session.query(Group.name, Student.fullname, Grade.grade) \
        .select_from(Student) \
        .join(Grade) \
        .join(Discipline) \
        .filter(Discipline.id == 7) \
        .join(Group) \
        .filter(Group.id == 3) \
        .all()
    table = PrettyTable(['Група', 'Студент', 'Всі оцінки'])
    for row in result:
        table.add_row(row)
    return table


def select_8():
    print('8.Знайти середній бал, який ставить певний викладач зі своїх предметів.')

    result = session.query(Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 2)) \
        .select_from(Teacher) \
        .join(Discipline) \
        .join(Grade) \
        .where(Teacher.id == 1) \
        .group_by(Teacher.fullname, Discipline.name) \
        .all()

    table = PrettyTable(['Викладач', 'Предмети', 'Середній бал'])
    for row in result:
        table.add_row(row)
    return table


def select_9():
    print("9.Знайти список курсів, які відвідує певний студент.")

    result = session.query(Student.fullname, Discipline.name) \
        .select_from(Student) \
        .join(Grade) \
        .join(Discipline) \
        .filter(Student.id == 27) \
        .group_by(Student.fullname, Discipline.id) \
        .all()
    table = PrettyTable(['Студент', 'Предмети'])
    for row in result:
        table.add_row(row)
    return table


def select_10():
    print("10.Список курсів, які певному студенту читає певний викладач.")

    result = session.query(Discipline.name, Student.fullname, Teacher.fullname) \
        .select_from(Student) \
        .join(Grade, Grade.student_id == Student.id) \
        .join(Discipline, Discipline.id == Grade.discipline_id) \
        .join(Teacher, Teacher.id == Discipline.teacher_id) \
        .filter(Student.id == 5, Teacher.id == 5) \
        .distinct(Discipline.name) \
        .all()
    table = PrettyTable(['Предмети', 'Студент', 'Викладач'])
    for row in result:
        table.add_row(row)
    return table


def select_11():
    print("11.Середній бал, який певний викладач ставить певному студентові.")

    result = session.query(Teacher.fullname, Student.fullname, func.round(func.avg(Grade.grade), 2)) \
        .select_from(Teacher) \
        .join(Discipline, Teacher.id == Discipline.teacher_id) \
        .join(Grade, Discipline.id == Grade.discipline_id) \
        .join(Student, Grade.student_id == Student.id) \
        .filter(Student.id == 1, Teacher.id == 1) \
        .group_by(Teacher.fullname, Student.fullname) \
        .all()
    table = PrettyTable(['Викладач', 'Студент', 'Середній бал'])
    for row in result:
        table.add_row(row)
    return table


def select_12():
    print("12.Знайти студентів у певній групі з певного предмета на останньому занятті.")

    group_id = 2
    dis_id = 2
    # Знаходимо останнє заняття
    subq = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == dis_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1)).scalar_subquery()

    result = session.query(Student.fullname, Discipline.name, Group.name, Grade.grade, Grade.date_of) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(and_(Grade.discipline_id == dis_id, Group.id == group_id, Grade.date_of == subq)) \
        .order_by(desc(Grade.date_of)).all()
    table = PrettyTable(['Студенти', 'Предмет', 'Група', 'Оцінка', 'Дата останнього заняття'])
    for row in result:
        table.add_row(row)
    return table


if __name__ == '__main__':
    print(select_1())
    print(select_2())
    print(select_3())
    print(select_4())
    print(select_5())
    print(select_6())
    print(select_7())
    print(select_8())
    print(select_9())
    print(select_10())
    print(select_11())
    print(select_12())
