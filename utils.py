import six
import time
import json
import traceback
import MySQLdb
from MySQLdb import OperationalError


def text_prepare(string, _null='NULL'):
  """
  Prepare text for db insert
  :param string:
  :param _null:
  :return:
  """
  if string is None:
    return _null
  return json.dumps(string, encoding='utf-8', ensure_ascii=False)


def db_connect(host=None, db_name=None, user=None, password=None, dict_cursor=False, **kwargs):
  """
  Connect to database
  :param host:
  :param user:
  :param password:
  :param db_name:
  :param dict_cursor:
  :param kwargs:
  :return:
  """
  if dict_cursor:
    db = MySQLdb.connect(
      host=host, user=user, password=password, db=db_name, use_unicode=True, charset="utf8mb4", cursorclass=MySQLdb.cursors.DictCursor
    )
  else:
    db = MySQLdb.connect(
      host=host, user=user, password=password, db=db_name, use_unicode=True, charset="utf8mb4", **kwargs
    )
  cursor = db.cursor()
  return db, cursor


def db_disconnect(db=None, cursor=None):
  """
  Disconnect from database
  :param db:
  :param cursor:
  :return:
  """
  try:
    cursor.close()
  except Exception:
    pass
  try:
    db.close()
  except Exception:
    pass


def db_reconnect(db=None, cursor=None):
  """
  Reconnect to database
  :param db:
  :param cursor:
  :return:
  """
  db_disconnect(db, cursor)
  return db_connect()


def db_execute_sql(db=None, cursor=None, queries=None, commit=False, retries=0, print_q=False, select=False, execute_q_v=False, commit_step=100):
  """
  Execute SQL command
  :param db:
  :param cursor:
  :param queries:
  :param commit:
  :param retries:
  :param print_q:
  :param select:
  :param execute_q_v:
  :param commit_step:
  :return:
  """
  assert isinstance(queries, list), 'queries must be a list of queries'
  assert isinstance(commit, bool), 'commit must be boolean'
  assert retries >= 0, 'Retries must be natural number'

  if cursor is None:
    print('Cursor is None.. Will try to reconnect!!!')
    db, cursor = db_reconnect(db, cursor)

  if select:
    db_data = []
  n = 0
  for i, q in enumerate(queries):
    try:
      if print_q:
        print(q)
      if execute_q_v:
        n += cursor.execute(q[0], q[1])
      else: n += cursor.execute(q)
      if commit and n % commit_step == 0:
        db.commit()
      if select:
        db_data.append(cursor.fetchall())

    except OperationalError:
      print('OpErr')
      traceback.print_exc()
      retries += 1
      time.sleep(10)
      db, cursor = db_reconnect(db, cursor)
      if retries < 3:
        return db_execute_sql(
          db=db, cursor=cursor, queries=queries, commit=commit, retries=retries, execute_q_v=execute_q_v
        )
    except MySQLdb.Error as e:
      print(e)
      if str(e) == 'cursor closed':
        print(f'Cursor closed, Reconnect!!!')
        db, cursor = db_reconnect(db, cursor)
        return db_execute_sql(
          db=db, cursor=cursor, queries=queries, commit=commit, retries=retries, execute_q_v=execute_q_v
        )
      try:
        print(f"MySQL Error {e.args[0]}: {e.args[1]}")
      except IndexError as e:
        print(f"MySQL Error: {str(e)}")
    except Warning as warn:
      if 'duplicate' in str(warn).lower():
        pass
      else:
        try:
          print(f"MySQL Warning {i} {warn.args[0]}: {warn.args[1]}")
        except IndexError:
          print(f"MySQL Warning: {i}-> {warn}")

  if commit:
    db.commit()

  if select:
    return db_data, db, cursor
  else:
    pass
  return n, db, cursor


def db_insert_sql(table=None, data=None, ignore=True, dtxt=False, insert=False, commit=False, db=None, cursor=None,
                 print_q=False, update=None, show_warning=True):
  """
  SQL Insert data 
  :param table: 
  :param data: 
  :param ignore: 
  :param dtxt: 
  :param insert: 
  :param commit: 
  :param db: 
  :param cursor: 
  :param print_q: 
  :param update: 
  :param show_warning: 
  :return: 
  """

  assert isinstance(update, six.string_types) or isinstance(update, type(None)) \
         or isinstance(update, six.integer_types), 'update must be collumn for identify'
  assert isinstance(table, six.string_types), 'Table must be string'
  assert isinstance(data, dict), 'Data must be dictionary'
  assert isinstance(ignore, bool), 'ignore must be boolean'
  assert isinstance(dtxt, bool), 'dtxt must be bool'
  assert isinstance(insert, bool), 'insert must be boolean'
  assert isinstance(commit, bool), 'commit must be boolean'
  assert isinstance(db, object), 'db must be object'
  assert isinstance(cursor, object), 'cursor must be object'
  assert isinstance(print_q, bool), 'print_q must be boolean'
  assert isinstance(update, (six.string_types, type(None))), 'update must be string or empty'
  assert (update is None or update in data), 'Update must be in data dict'
  if ignore:
    ignore = 'IGNORE'
  else:
    ignore = ''

  for k, v in six.iteritems(data):
    if isinstance(v, (dict, list, tuple)):
      data[k] = text_prepare(v)

  if update is None:
    tmp_data = []
    all_keys = data.keys()
    q = '''INSERT %s INTO %s (%s)''' % (ignore, table, ','.join(all_keys))
    q += ''' VALUES (''' + (' %s,' * len(all_keys))[1:-1] + ')'
    for k in all_keys:
      val = data[k]
      tmp_data.append(val)

  else:
    upd_val = data.pop(update)
    where = 'WHERE %s=%s LIMIT 1' % (update, '%s')
    tmp_data = []
    all_keys = data.keys()
    q = '''UPDATE %s SET %s %s''' % (table, ', '.join(['%s=%s' % (_, '%s') for _ in all_keys]), where)
    for k in all_keys:
      val = data[k]
      tmp_data.append(val)
    tmp_data.append(upd_val)

  if print_q:
    print('"{}", {}'.format(q, tuple(tmp_data)))
  if insert is False:
    return q, tuple(tmp_data)

  c = 0
  try:
    c = cursor.execute(q, tuple(tmp_data))

  except MySQLdb.IntegrityError as error:
    if 'duplicate entry' in str(error).lower():
      pass
    else:
      try:
        print("MySQL IntegrityError [%d]: %s" % (error.args[0], error.args[1]))
      except IndexError:
        print("MySQL IntegrityError: %s" % str(error))
  except Warning as warn:
    if 'duplicate' in str(warn).lower():
      pass
    else:
      if show_warning:
        try:
          print("MySQL Warning [%d]: %s" % (warn.args[0], warn.args[1]))
        except IndexError:
          print("MySQL Warning: %s" % str(warn))
  except Exception as e:
    print(q, tuple(tmp_data))
    traceback.print_exc()
    return c
  if commit:
    db.commit()
  return c

