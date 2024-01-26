from my_psql import cars


def update_data(data):
    tables = {'persons': ('_reference300', '_reference155', '_inforg10632'),
              'cars': ('_reference262', '_reference211', '_reference259'),
              'cargo': ('_reference124',),
              'routes': ('_reference207',)}
    if data in tables:
        for table in tables[data]:
            _obj = cars.BItable(table)
            _obj.db1c_sync()
        return {"message": f"{data} updated"}
    else:
        return {"message": f"Can not update {data}"}


if __name__ == '__main__':
    update_data('routes')
