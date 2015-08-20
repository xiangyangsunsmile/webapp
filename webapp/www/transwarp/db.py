#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2015-08-19 14:08:03
# @Author  : Your Name (you@example.org)
# @Link    : http://example.org
# @Version : $Id$

'''
Database opereation module
'''
import time
import uuid
import functools
import threading
import logging

# Dict object


class Dict(dict):

    """
	Simple dict but support assess as x.y style
    """

    def __init__(self, names=(), values=()):
        super(Dict, self).__init__()
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(key, value):
        self[key] = value


def next_id(t=None):
    '''
    Return next id as 50-char string

    Args:
            t: unix timestamp,defacult to None and using time.time() 
    '''
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)


def _profiling(start, sql=''):
    # time.sleep(1)
    t = time.time() - start
    if t > 0.1:
        logging.warning("[PROFILING][DB] %s: %s" % (t, sql))
    else:
        logging.info("[PROFILING][DB] %s: %s" % (t, sql))


class DBError(Exception):
    pass


class MultiColumnsError():
    pass

# global engine object
engine = None


class _Engine(object):

    def __init__(self, connect):
        super(_Engine, self).__init__()
        self._connect = connect

    def connect(self):
        return self._connect()


def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    print "1"
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defacults = dict(use_unicode=True, charset='utf8',collation='utf8_general_ci', autocommit=False)
    for k, v in defacults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    engine = _Engine(lambda: mysql.connector.connect(**params))
    logging.info('Init mysql engine <%s> is ok' % hex(id(engine)))

#对数据库连接（cursor,commit,roolback,close）及基本操作进行封装
class _LasyConnection(object):
	
	def __init__(self):
		super(_LasyConnection, self).__init__()
		self.connection=None 

	def cursor(self):
		if self.connection is None:
			connection=engine.connect()
			self.connection=connection
			logging.info("open connection <%s>" % hex(id(connection)))
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()

	def roolback():
		self.connection.roolback()

	def cleanup(self):
		if self.connection:
			connection=self.connection
			self.connection=None
			connection.close()
			logging.info("close connection <%s>" % hex(id(connection)))

#创建threading.local对象
class _DbCtx(threading.local):
	'''
	thread local object that holds connection info 
	'''
	def __init__(self):
		self.connection=None#数据库连接
		self.transaction=None#事务

	def is_init(self):
		return not self.connection is None

	def init(self):
		self.connection=_LasyConnection()#open a connection
		self.transaction=0
		logging.info("open lazy connection...")

	def cleanup(self):
		self.connection.cleanup()
		self.connection=None

	def cursor(self):
		return self.connection.cursor()

#继承threading.local, 对于每一个线程都是不一样的,因此，需要数据库连接时，使用该类的对象来创建
_db_ctx=_DbCtx()

#=================以下是数据库连接==================
#创建上下文管理器,供with语句使用，通过with语句让数据库自动连接和关闭
class _ConnectionCtx(object):
	'''
	_ConnectionCtx object that can open and close connection  context.
	_ConnectionCtx object can be nested and the only the most outer connection  has effect.
	'''

	def __enter__(self):
		global _db_ctx
		self.should_cleanup=False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup=True
		return self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def connection():
	'''
	Return _ConnectionCtx object that can be used by 'with ' statement
	'''
	return _ConnectionCtx()

#使用带装饰器的方法，让其能够共用同一个数据库连接.如：select_one=with_connection(select_one)
def with_connection(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx() :
			return func(*args,**kw)
	return _wrapper
#==============以上是数据库连接======================
#==============以下是事务处理，知识点同数据库连接====
class _TransactionCtx(object):
	'''
	_transactionCtx object that can handle transaction

	with _transactionCtx():
		pass
	'''
	def __enter__(self):
		global _db_ctx
		self.should.close_conn=False
		if not _db_ctx.is_init():
			_db_ctx().init()
			self.should_close_conn=True
		_db_ctx.transaction=_db_ctx.transaction+1
		logging.info("begin transaction..." if _db_ctx.transaction==1 else 'join current transaction...')
		return self
	
	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transaction=_db_ctx.transaction-1
		try:
			if _db_ctx.transaction==0:
				if exctype==None:
					self.commit()
				else:
					self.roolback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		logging.info("commit transaction...")
		try:
			_db_ctx.connection.commit()
			logging.info("commit ok...")
		except :
			logging.warning("commit failed ,try roolback... ")
			_db_ctx.connection.roolback()
			logging.warning("rollback ok...")
			raise

	def rollback(self):
		global _db_ctx
		_db_ctx.connection.roolback()

def transaction(self):
	return _TransactionCtx()

def with_transaction(func):
	'''
	a decorator that makes function around transaction
	'''
	@functools.wraps(func)
	def wrapper(*args,**kw):
		_start=time.time()
		with transaction():
			return func(*args,**kw)
		_profiling(_start)
	return wrapper

#===========以上是事务处理=================
def _select(sql,first,*args):
	'execute select SQL and return unique result or list  result'
	global _db_ctx
	cursor=None
	sql=sql.replace('?','%s')
	logging.info('SQL ：%s,ARGS: %s' % (sql ,args))
	try:
		cursor=_db_ctx.connection.cursor()
		cursor.execute(sql,args)
		if cursor.description:
			names=[ x[0]  for  x  in  cursor.description]#得到域的名字,即列名
		if first:
			values=cursor.fetchone()
			if not values:
				return None
			return Dict(names,values)
		return [Dict(names,x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()

'''
select_one=with_connection(select_one)
'''
#返回一行
@with_connection
def select_one(sql,*args):
	return _select(sql,True,*args)

@with_connection
def select_int(sql,*args):
	d=_select(sql,True,*args)
	print d
	if len(d)==1:
		raise MultiColumnsError("Except only one column")
	return d.values()[0]
#返回所有
@with_connection
def select(sql,*args):
	return _select(sql, False,*args)

@with_connection
def _update(sql,*args):
	global _db_ctx
	cursor=None
	sql=sql.replace('?','%s')
	logging.info("SQL:%s , ARGS: %s" % (sql,args))
	try:
		cursor=_db_ctx.connection.cursor()
		cursor.execute(sql,args)
		r=cursor.rowcount
		if _db_ctx.transaction==0:
			_db_ctx.connection.commit()
			logging.info("auto commit ")
		return r
	except Exception, e:
		raise e
	finally:
		if cursor:
			cursor.close()

def insert(table,**kw):

	cols,args=zip(*kw.iteritems())
	sql='insert into  %s (%s) values (%s)' % (table,','.join([ '%s' % col for col in cols]) ,','.join(['?' for i in range(len(cols))]))
	return _update(sql,)

def update(sql,*args):
	return _update(sql,*args)

if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine(user='root', password='root', database='test', host='127.0.0.1', port=3306)
	update('drop table if exists user')
	sql='create table user(id int primary key,name text,email text,passwd text,last_modified real)'
	update(sql)
	import doctest
	doctest.testmod()
	
