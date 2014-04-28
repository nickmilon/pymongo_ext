#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Created on Apr 22, 2010

@author: nickmilon
Description:    some basic classes and functions.
This module is kept lightweight with a small footprint
'''
import types
import re
from time import sleep


def enum(**enums):
    return type('Enum', (), enums)


class dictDot(dict):
    """dictionary with dot notation
       del r.Aux['vrf'] (dot notation does not work on last key on commands like del update etc. )
    """
    def prnt(self):
        for k, v in self.items():
            print k, v

    def __getattr__(self, attr):
        item = self[attr]
        if type(item) == types.DictType:
            item = dictDot(item)
        #if isinstance(item,dict):item = dictDot(item)
        return item
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def autoRetry(exceptionOrTuple, retries=3, sleepSeconds=1, BackOfFactor=1, loggerFun=None):
    """ exceptionOrTuple=exception or tuple of exceptions,
        BackOfFactor = factor to back off on each retry loggerFun i.e. logger.info
    """
    def wrapper(func):
        def fun_call(*args, **kwargs):
            tries = 0
            while tries < retries:
                try:
                    return func(*args, **kwargs)
                except exceptionOrTuple, e:
                    tries += 1
                    if loggerFun:
                        loggerFun("exception [%s] e = [%s] handled tries :%d sleeping[%f]" % \
                                  (exceptionOrTuple, e, tries, sleepSeconds * tries * BackOfFactor))
                    sleep(sleepSeconds * tries * BackOfFactor)
            raise
        return fun_call
    return wrapper


def parseJSfunFromFile(filepath, functionName):
    """
        helper function to get a js function string from a file containing js functions.
        Function must be named starting in first column and file must end with //eof//
        lazyloads re
    """
    with open(filepath) as fin:
        r = re.search("(^.*?)(?P<fun>function\s+?%s.*?)(^fun|//eof//)" \
                      % functionName, fin.read(), re.MULTILINE | re.DOTALL)
        return r.group('fun').strip() if r else False


class multiOrderedDict(object):
    '''
        deletes can't be multi
    '''
    def __init__(self, lst):
        self.lstDic = lst

    def __getitem__(self, key):
        return self._getOrSetDictItem(key)

    def __setitem__(self, key, val):
        return self._getOrSetDictItem(key, True, val)

    def __delitem__(self, key):
        return self._getOrSetDictItem(key, delete=True)

    def get(self, key, orVal=None):
        try:
            return self[key]
        except KeyError:
            return orVal

    def keys(self):
        return[i[0] for i in self.lstDic if self.isKey(i[0])]

    def values(self):
        return [self[i] for i in self.keys()]

    def isKey(self, k):
        return True

    def _getOrSetDictItem(self, key, setVal=False, newVal=None, multi=False, delete=False):
        idx = []
        for n, i in enumerate(self.lstDic):
            if i[0] == key and self.isKey(i[0]):
                idx.append(n)
                if setVal:
                    self.lstDic[n] = [i[0], newVal]
                if not multi:
                    break
        if len(idx) > 0:
            if delete:
                self.lstDic.pop(idx[0])
                return None
            rt = [self.lstDic[i][1:] for i in idx]
            if multi:
                return rt
            else:
                return rt[0][0]
        else:
            if setVal:
                self.lstDic.append([key, newVal])
                return newVal
            else:
                raise KeyError(key)

    def toDict(self):
        return dict(zip(self.keys(), self.values()))

    def toString(self):
        return str(self.toDict())
    __str__ = toString


class confFileDict(multiOrderedDict):
    def __init__(self, path, skipBlanks=True, skipRemarks=True):
        self.path = path
        with open(self.path) as fin:
            rlines = fin.readlines()
        if skipBlanks:
            rlines = [i for i in rlines if not i == '\n']
        if skipRemarks:
            rlines = [i for i in rlines if not i.startswith("#")]
        lstDic = [map(lambda x: x.strip(), i.split(" = ")) for  i in rlines]
        super(confFileDict, self).__init__(lstDic)

    def isKey(self, key):
        return key != '' and not key.startswith("#")

    def toStr(self):
        s = ''
        for i in self.lstDic:
            s += " = ".join(i) + '\n'
        return s.rstrip()

    def toFile(self, path=None):
        if not path:
            path = self.path
        with open(path, 'w') as fl:
            fl.write(self.toStr)


class SubToEvent(object):
    '''lightweight Event handler modeled after Peter Thatcher
       http://www.valuedlessons.com/2008/04/events-in-python.html
           usage:
            watcher = SubToEvent()
            def log_docs(doc):print doc
            watcher += log_docs
            watcher += lambda x:str(x)
            watcher.stop()
    '''

    def __init__(self, channelName=''):
        self.channelName = channelName
        self.handlers = set()

    def handle(self, handler):
        self.handlers.add(handler)
        return self

    def unhandle(self, handler):
        try:
            self.handlers.remove(handler)
        except:
            raise ValueError("No_such_handler")
        return self

    def fire(self, *args, **kargs):
        for handler in self.handlers:
            handler(*args, **kargs)

    def fireTopic(self, topic=None, verb=None, payload=None):
        self.fire((self.channelName, topic, verb, payload))

    def getHandlerCount(self):
        return len(self.handlers)
    __iadd__ = handle
    __isub__ = unhandle
    __call__ = fire
    __len__ = getHandlerCount
