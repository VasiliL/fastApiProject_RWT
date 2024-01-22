from my_psql import cars


def update_persons():
    for table in ('_reference300', '_reference155', '_inforg10632'):
        _obj = cars.BItable(table)
        _obj.db1c_sync()


if __name__ == '__main__':
    update_persons()
