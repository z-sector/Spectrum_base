# -*- coding: utf-8 -*-

import os
import psycopg2

CONFIG = "host='localhost' dbname='markers_db' user='admin' password='admin'"
NAME_FILE = 'result.txt'


def find_files(catalog, expansion):
    list_files = []
    for root, dirs, files in os.walk(catalog):
        list_files += [os.path.join(root, name) for name in files if os.path.splitext(name)[1] == '.' + expansion]
    return list_files


def create_file(list_files):
    with open(NAME_FILE, 'w', encoding='utf-8') as result:
        for it_file in list_files:
            with open(it_file, 'rb') as file:
                it = 0
                while True:
                    file.seek(it)
                    chars = file.read(2)
                    if chars == '':
                        break
                    if chars[0] == 255 and chars[1] == 243:
                        break
                    it += 1
                file.seek(it + 6)
                for line in file:
                    str_line = line[2:-1] if line[0] == 30 else line[:-1]
                    s = str_line.decode('windows-1251')
                    result.write(it_file + '#' + s.rstrip() + '\n')

    return True


def create_tables():
    commands = (
        """
        CREATE TABLE public.marker (
            id INTEGER PRIMARY KEY,
            name TEXT,
            note TEXT,
            cassete INTEGER,
            row INTEGER,
            col INTEGER,
            eng TEXT
        );
        """,
        """ CREATE TABLE public.group (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES public.group(id),  
                name TEXT
                );
        """,
        """
        CREATE TABLE public.group_marker (
                group_id INTEGER NOT NULL,
                marker_id INTEGER NOT NULL,
                PRIMARY KEY (group_id, marker_id),
                FOREIGN KEY (group_id) REFERENCES public.group (id),
                FOREIGN KEY (marker_id) REFERENCES public.marker (id) ON UPDATE CASCADE ON DELETE CASCADE      
        );
        """)
    conn = None
    try:
        conn = psycopg2.connect(CONFIG)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return True


def delete_tables():
    commands = (
        '''
        DROP TABLE public.group_marker;
        ''',
        '''
        DROP TABLE public.marker;
        ''',
        '''
        DROP TABLE public.group;
        ''')
    conn = None
    try:
        conn = psycopg2.connect(CONFIG)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return True


def insert_marker(marker_id, data_list):
    values = data_list[2].split('.')
    if len(values) == 3:
        cass, row, col = values
    elif len(values) == 2:
        cass, row = values
        col = 'null'
    elif len(values) == 1 and values[-1] != '':
        cass = values[-1]
        row = 'null'
        col = 'null'
    else:
        cass = 'null'
        row = 'null'
        col = 'null'
    if data_list[0] == " " and data_list[1] == " ":
        raise RuntimeError("Incorrect", data_list)
    name = f"$${data_list[0]}$$" if data_list != ' ' else f"$${data_list[1]}$$"
    note = f"$${data_list[1]}$$" if data_list != ' ' else f"null"
    if data_list[0] == ' ':
        data_list[0] = data_list[1]
    if data_list[1] == ' ':
        data_list[1] = 'null'
    sql = f"INSERT INTO public.marker VALUES({marker_id}, {name}, {note}, {cass}, {row}, {col}) " \
          f"RETURNING id;"
    conn = None
    current_id = None
    try:
        conn = psycopg2.connect(CONFIG)
        cur = conn.cursor()
        cur.execute(sql)
        current_id = cur.fetchone()[0] + 1
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return current_id


def insert_group(group_id, path):
    list_group = []
    groups = path.split('\\')[1:]
    conn = None
    current_id = None
    try:
        conn = psycopg2.connect(CONFIG)
        cur = conn.cursor()
        parent_id = None
        current_id = group_id
        for group in groups:
            name = f"$${group}$$"
            sql = f"SELECT id, parent_id FROM public.group WHERE name = {name} AND "
            if not parent_id:
                sql += "parent_id is NULL"
            else:
                sql += f"parent_id = {parent_id}"
            cur.execute(sql)
            results_id = cur.fetchall()
            if not results_id:
                if parent_id:
                    sql = f"INSERT INTO public.group VALUES({current_id}, {parent_id}, {name}) RETURNING id;"
                else:
                    sql = f"INSERT INTO public.group VALUES({current_id}, NULL, {name}) RETURNING id;"
                cur.execute(sql)
                parent_id = cur.fetchone()[0]
                list_group.append(current_id)
                current_id = parent_id + 1
            else:
                list_group.append(results_id[-1][0])
                parent_id = results_id[-1][0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return current_id, list_group


def insert_group_marker(list_group, marker_id):
    conn = None
    try:
        conn = psycopg2.connect(CONFIG)
        cur = conn.cursor()
        for group_id in list_group:
            sql = f"INSERT INTO public.group_marker VALUES({group_id}, {marker_id});"
            cur.execute(sql)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return True


def insert_cass(marker_id, cass, row, col):
    file_name = 'EnergoBase\\' + 'Кассета_' + f'{int(cass)}\\' + f'К{cass}{row}{col}.eng'
    with open(file_name, 'rb') as eng_byte:
        data = eng_byte.read()

    conn = None
    try:
        conn = psycopg2.connect(CONFIG)
        cur = conn.cursor()
        sql = "UPDATE public.marker SET eng = %s WHERE id = %s;"
        cur.execute(sql, (data, marker_id))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return True


def my_parser(my_int):
    result = str(my_int)
    if len(result) == 1:
        result = '0' + result
    return result


if __name__ == "__main__":
    list_files = find_files('SystemBase', 'nbs')
    create_file(list_files)

    # if you do not want to delete and re-create the tables, then these functions do not call.
    delete_tables()
    create_tables()

    with open(NAME_FILE, 'r', encoding='utf-8') as file:
        marker_id = 1
        result_group = (1, [])
        for line in file:
            curr_marker_id = marker_id
            marker_id = insert_marker(marker_id, line.rstrip().split('#')[1:])
            result_group = insert_group(result_group[0], line.split('#')[0])
            insert_group_marker(result_group[1], curr_marker_id)
            print(curr_marker_id, result_group)

    with open(NAME_FILE, 'r', encoding='utf-8') as file:
        line = file.readline().rstrip()
        marker_id = 1
        while line:
            s = line.split('#')[3]
            values = s.split('.')
            if len(values) == 3:
                values = list(map(int, values))
                cass, row, col = values
            else:
                line = file.readline().rstrip()
                marker_id += 1
                continue
            cass = my_parser(cass)
            row = my_parser(row)
            col = my_parser(col)
            print(marker_id, cass, row, col)
            try:
                insert_cass(marker_id, cass, row, col)
            except FileNotFoundError as error:
                print(error, '---->', marker_id)
            line = file.readline().rstrip()
            marker_id += 1

    # An example of selecting binary data from a table
    # conn = None
    # try:
    #     conn = psycopg2.connect(CONFIG)
    #     cur = conn.cursor()
    #     sql = f"SELECT eng FROM public.marker WHERE id = 26476;"
    #     cur.execute(sql)
    #     temp = cur.fetchone()
    #     print(bytes(temp[0]))
    #     print(type(temp[0]))
    #     cur.close()
    # except (Exception, psycopg2.DatabaseError) as error:
    #     print(error)
    # finally:
    #     if conn is not None:
    #         conn.close()
