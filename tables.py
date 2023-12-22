from my_psql import cars

# define persons table
columns = {
    '_idrref': 'bytea'
    , '_version': 'integer'
    , '_marked': 'boolean'
    , '_predefinedid': 'bytea'
    , '_parentidrref': 'bytea'
    , '_folder': 'boolean'
    , '_fld8840': 'timestamp without time zone'
    , '_fld8842rref': 'bytea'
    , '_fld8846rref': 'bytea'
    , '_fld8841': 'text'
    , '_fld8845': 'text'
    , '_fld8843': 'text'
    , '_code': 'text'
    , '_description': 'text'
    , '_fld8844': 'text'
}
persons = cars.BItable(table_name='_reference300', columns=columns, db1c_table_name='_reference300')
