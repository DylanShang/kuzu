import datetime
import pytest


def test_bool_param(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.isStudent = $1 AND a.isWorker = $k RETURN COUNT(*)",
                          [("1", False), ("k", False)])
    assert result.hasNext()
    assert result.getNext() == [1]
    assert not result.hasNext()
    result.close()


def test_int_param(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.age < $AGE RETURN COUNT(*)", [("AGE", 1)])
    assert result.hasNext()
    assert result.getNext() == [0]
    assert not result.hasNext()
    result.close()


def test_double_param(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.eyeSight = $E RETURN COUNT(*)", [("E", 5.0)])
    assert result.hasNext()
    assert result.getNext() == [2]
    assert not result.hasNext()
    result.close()


def test_str_param(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN concat(a.fName, $S);", [("S", "HH")])
    assert result.hasNext()
    assert result.getNext() == ["AliceHH"]
    assert not result.hasNext()
    result.close()


def test_date_param(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.birthdate = $1 RETURN COUNT(*);",
                          [("1", datetime.date(1900, 1, 1))])
    assert result.hasNext()
    assert result.getNext() == [2]
    assert not result.hasNext()
    result.close()


def test_timestamp_param(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);",
                          [("1", datetime.datetime(2011, 8, 20, 11, 25, 30))])
    assert result.hasNext()
    assert result.getNext() == [1]
    assert not result.hasNext()
    result.close()


def test_param_error1(establish_connection):
    conn, db = establish_connection
    with pytest.raises(RuntimeError, match="Parameter name must be of type string but get <class 'int'>"):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", [(1, 1)])


def test_param_error2(establish_connection):
    conn, db = establish_connection
    with pytest.raises(RuntimeError, match="Each parameter must be in the form of <name, val>"):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", ["asd"])


def test_param_error3(establish_connection):
    conn, db = establish_connection
    with pytest.raises(RuntimeError, match="Each parameter must be in the form of <name, val>"):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", [("asd", 1, 1)])
