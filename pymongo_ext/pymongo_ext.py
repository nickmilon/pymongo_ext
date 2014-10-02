#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Created:Dec 15, 2012
@author: nickmilon
Description:
'''
'''
usage examples:
    from pymongo_ext import pymongo_ext as pmext
    optional: pmext.MONGO_CONF_DEFAULTPATH = your mongo config file path i.e.:"/etc/mongodb.conf"
    cnf = pmext.mongoConfToPy()
    mcl = pmext.MdbClient(**cnf)
    dbases = mcl.client.database_names()
    dba_collections = mcl.client.local.collection_names()
    mcl.client.local.startup_log.find()[0]
    ...... ......
    res = pmext.MRfields(mcl.client.local.startup_log)
    res[1].find()[0]
    ...... ......
    mcl.close()
'''
from gevent import monkey
monkey.patch_all()
from gevent import sleep, Greenlet, GreenletExit
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from datetime import datetime
from pymongo.read_preferences import ReadPreference
from pymongo.errors import AutoReconnect, ConnectionFailure
from pymongo.cursor import _QUERY_OPTIONS
from bson.code import Code
from bson import SON
from utils import enum, dictDot
from utils import parseJSfunFromFile, confFileDict, SubToEvent, autoRetry
from os import path
unicode('').encode('idna')
#@note:  fixes bug on gevent 1.0  see https://github.com/surfly/gevent/issues/349
                            #os.environ["GEVENT_RESOLVER"] = "ares" doesn't work
#MongoClient.copy_database(self, from_name, to_name, from_host, username, password)
from __init__ import _PATHROOT
PATH_JS = _PATHROOT + "/js/mr_fun.js"
# print 'root', os.listdir(_ROOT)
def getMongoConfPath():
    l=["db.conf",'mongod.conf','mongodB.conf']
    l=map(lambda x: x if path.isfile("/etc/"+x) else False, l)
    for i in l:
        if i:return "/etc/"+i
 
MONGO_CONF_DEFAULTPATH = getMongoConfPath()
mv = enum(_gt='$gt', _lt='$lt', _all='$all', _exists='$exists', _mod='$mod', _ne='$ne', _in='$in',
         nin='$nin', _nor='$nor', _or='$or', _and='$and', _size='$size', _type='$type', _set="$set",
         _atomic='$atomic', _id='_id')


def andVal(andVal=[]):
    """just a helper for producing and queries """


def getMongoConf(path=MONGO_CONF_DEFAULTPATH):
    ''' returns a dictionary like object with mongo configuration keys and values
        and/remove/change values and keys and save it back using obj.toFile()
    '''
    return confFileDict(path)


def mongoConfToPy(path=MONGO_CONF_DEFAULTPATH):
    ''' returns a dictionary to be used as **kwargs in pymongo replicaSetClient or
        MongoClient init parameters
        tip: update this dictionary with any other requested parameters
    '''
    rt = {}
    mongoConfig = getMongoConf(path)
    if not mongoConfig.get('bind_ip'):
        mongoConfig['bind_ip'] = 'localhost'
        #@note:default to localhost
    if mongoConfig.get('replSet'):
        rt['replicaSet'] = mongoConfig['replSet']
        rt['hosts_or_uri'] = mongoConfig['bind_ip']
    else:
        rt['host'] = mongoConfig['bind_ip']
    if mongoConfig.get('port'):
        rt['port'] = mongoConfig['port']
    return rt


def updateId(collection, docOld, IdNew):
    """very dangerous if you don't know what you are doing, use it at your own risk
       be carefull on swallow copies if modifying IdNew from old document
    """
    docNew = docOld.copy()
    docNew[mv._id] = IdNew
    try:
        rt = collection.insert(docNew, j=True, w=1)
    except Exception, e:
        raise
        return False, Exception, e
        #@note:return before removing original !!!!
    else:
        return True, collection.remove({"_id": docOld['_id']}, j=True, w=1, multi=False), rt


def collectionIndexedFields(aCollection):
    return [i['key'][0][0]  for i in aCollection.index_information().values()]


def collectionFieldRange(aCollection, field_name="_id"):
    "returns minimum, maximum value of a field"
    idMin = aCollection.find_one(sort=[(field_name, 1)], fields=[field_name], as_class=dictDot)
    if idMin:
        idMin = eval("idMin." + field_name)
        #@note:  a litle tricky coz field_name can be sub field
        idMax = aCollection.find_one(sort=[(field_name, -1)], fields=[field_name], as_class=dictDot)
        idMax = eval("idMax." + field_name)
        return idMin, idMax
    else:
        return None, None


def collectionChunks(collection, chunkSize=50000, field_name="_id"):
    """ similar to not documented splitVector command
        db.runCommand({splitVector: "test.uniques", keyPattern: {dim0: 1},
        maxChunkSizeBytes: 32000000})
        Provides a range query arguments for scanning a collection in batches equals to chunkSize
        for optimization reasons first chunk size is chunksize +1
        if field_name is not specified defaults to _id if specified it must exist for all documents
        in collection and been indexed will make things faster
        usage:chk=collectionChunks(a_colection, chunkSize=500000, field_name="AUX.Foo");
              for i in chk:print i
    """
    idMin, idMax = collectionFieldRange(collection, field_name)
    curMin = idMin
    curMax = idMax
    cntChunk = 0
    while cntChunk == 0 or curMax < idMax:
        nextChunk = collection.find_one({field_name: {"$gte": curMin}}, sort=[(field_name, 1)],
                                        skip=chunkSize, as_class=dictDot)
        curMax = eval('nextChunk.' + field_name) if nextChunk else idMax
        query = {field_name: {"$gte"if cntChunk == 0 else "$gt": curMin, "$lte": curMax}}
        yield cntChunk, query
        cntChunk += 1
        curMin = curMax


def copyDb(client, fromhost, fromdb, todb=None):
    return client.admin.command(SON([("copydb", 1), ("fromhost", fromhost),
                                     ("fromdb", fromdb), ("todb", todb)]))


def copy_collection(collObjFrom, collObjTarget, filterDict={},
                    create_indexes=False, dropTarget=False, verboseLevel=1):
    #@note: we can also copy the collection using Map Reduce which is probably faster:
    #http://stackoverflow.com/questions/3581058/mongodb-map-reduce-minus-the-reduce
    if verboseLevel > 0:
        print "copy_collection:%s to %s" % (collObjFrom.name, collObjTarget.name)
    if dropTarget:
        collObjTarget.drop()
    docs = collObjFrom.find(filterDict)
    totalRecords = collObjFrom.database.command("collstats", collObjFrom.name)['count'] \
                                                if filterDict == {} else docs.count()
    for cnt, doc in enumerate(docs):
        percDone = round((cnt + 1.0) / totalRecords, 3) * 100
        if verboseLevel > 0 and percDone % 10 == 0:
            print "%% done=:%3d %% :%10d of %10d" % (percDone, cnt + 1, totalRecords)
        collObjTarget.save(doc, secure=False)
    if create_indexes:
        for k, v in collObjFrom.index_information().iteritems():
            if k != "_id_":
                idx = (v['key'][0])
                if verboseLevel > 0:
                    print "creating index:%s" % (str(idx))
                collObjTarget.create_index(idx[0], idx[1])
    return collObjTarget


def MRCommand_(MRoutDict):
    '''returns the actual command from output parameter'''
    return [i for i in ['replace', 'merge', 'reduce', 'inline'] if i in MRoutDict.viewkeys()][0]


def MRsimple(collection, FunMap, FunReduce=None, query={}, out={"replace": 'mr_tmp'}, finalize=None,
             scope={}, sort=None, jsMode=False, verbose=1):
    """ simplified generic Map Reduce
        see: http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
        returns (MR response object, results collection or results list if out={"inline":1})
        Reduce function defaults to one that increments value count
        optimize by sorting on emit fields
        see: http://edgystuff.tumblr.com/post/7624019777/optimizing-map-reduce-with-mongodb
        docs.mongodb.org/manual/reference/method/db.collection.mapReduce/#db.collection.mapReduce
        sort      i.e: sort= { "_id":1 }
        jsMode    should be False if we expect more than 500K dictinct ids
    """
    if len(out.viewkeys()) > 1:
        command = MRCommand_(out)
        out = SON([(command, out[command]), ('db', out.get('db')),
                   ('nonAtomic', out.get('nonAtomic', False))])
        #nonAtomic not allowed on replace
    FunMap = Code(FunMap, {})
    if FunReduce is None:
        FunReduce = u"""function (key, values) {var total = 0; for (var i = 0;
                        i < values.length; i++) { total += values[i]; } return total;}
                     """
    FunReduce = Code(FunReduce, {})
    if verbose > 2:
        print "Start MRsimple collection = %s"\
              "query = %s\nMap=\n%s\nReduce=\n%s\nFinalize=%s\nscope=%s sort=%s" \
              % tuple(map(str, (out, query, FunMap, FunReduce, finalize, scope, sort)))
    if sort:
        sort = SON(sort)
    r = collection.map_reduce(FunMap, FunReduce, out=out, query=query,
                              finalize=finalize, scope=scope, sort=sort, full_response=True)
    if verbose > 1:
        print  "End MRsimple collection=%s, query=%s\nresulsts=\n %s"\
                % (collection.name, str(query), str(r))
    if 'db' in out.viewkeys():
        #@note:  can be dict or SON, either way it has property viewkeys
        results = collection.database.connection[r['result']['db']][r['result']['collection']]
    else:
        results = r['results'] if  out.keys()[0] == 'inline' else collection.database[r['result']]
        #@note:  results is a list if inline else a collection
    return r, results


def tmpEduMR(colA, query={}, verbose=2):
    map_closest = parseJSfunFromFile(PATH_JS, 'map_closest')
    red_closest = parseJSfunFromFile(PATH_JS, 'red_closest')
    mr1 = MRsimple(colA, map_closest, FunReduce=red_closest, query=query,
                   out={'db': 'edu', 'replace': 'mr_closest'}, finalize=None, verbose=verbose)
    return mr1


def MRgroupOnField(collection, field_name="_id", query={},
                   out={"replace": 'mr_tmp'}, sort=None, verbose=1):
    """ group values of field using map reduce (see MRsimple)
    """
    FunMap = "function  () {emit(this.%s, 1);}" % field_name
    return MRsimple(collection, FunMap, query=query, out=out, sort=sort, verbose=verbose)


def MRorphans(colA, colAfld, colAq, colB, colBfld, colBq, out='mr_orphans',
              jsMode=False, verbose=3):
    ''' A kind of set operation on 2 collections
        Map Reduce two collection objects (colA, colB) on a common field (colAfld, colBFld)
        allowing queries (colAq, colBq)
        returns tuple (result collection, MapReduce1 results, MapReduce2 results)
        resultsCollection
               value.A = count of documents in A, value.B count of documents in B,
               sum count of documents in both A+B
               documents non existing in ColA             resultCollection.find( {'value.A':0}}
               documents existing in both ColA and ColB
                       resultCollection.find( ={'value.A':{'$gt':0}, 'value.B':{'$gt':0}})
               or if documents are unique in both collections resultCollection.find({'value.sum':2}}
        call example:
               MapOrphans(bof.TstatusesSrc, '_id', {}, colB=ag13, colBfld='_id.vl',
               colBq={'_id.kt': 'src', '_id.idsu': ''}, out='mr_join', jsMode=False, verbose=3)
    '''
    MapOrphansJs = parseJSfunFromFile(PATH_JS, 'MapOrphans')
    ReduceOrphansJs = parseJSfunFromFile(PATH_JS, 'ReduceOrphans')
    scope = {'phase': 1}
    sort = {colAfld: 1} if colAfld in collectionIndexedFields(colA) else None
    mr1 = MRsimple(colA, MapOrphansJs % (colAfld), FunReduce=ReduceOrphansJs,
                   query=colAq, out={'db': 'local', 'replace': out},
                   finalize=None, scope=scope, sort=sort, verbose=verbose, jsMode=jsMode)
    scope['phase'] = 2
    sort = {colBfld: 1} if colBfld in collectionIndexedFields(colB) else None
    mr2 = MRsimple(colB, MapOrphansJs % (colBfld), FunReduce=ReduceOrphansJs, query=colBq,
                   out={'db': 'local', 'reduce': out}, finalize=None, scope=scope, sort=sort,
                   verbose=verbose, jsMode=jsMode)
    return mr2[1], mr1[0], mr2[0]


def MRJoin(colA, colAfld, colAq, colB, colBfld, colBq, out='mr_join', jsMode=False, verbose=3):
    MapJoin = parseJSfunFromFile(PATH_JS, 'MapJoin')
    ReduseJoin = parseJSfunFromFile(PATH_JS, 'ReduceJoin')
    scope = {'phase': 1}
    sort = {colAfld: 1} if colAfld in collectionIndexedFields(colA) else None
    mr1 = MRsimple(colA, MapJoin % (colAfld), FunReduce=ReduseJoin, query=colAq,
                   out={'db': 'local', 'replace': out},
                   finalize=None, scope=scope, sort=sort, verbose=verbose, jsMode=jsMode)
    scope['phase'] = 2
    sort = {colBfld: 1} if colBfld in collectionIndexedFields(colB) else None
    mr2 = MRsimple(colB, MapJoin % (colBfld), FunReduce=ReduseJoin, query=colBq,
                   out={'db': 'local', 'reduce': out},
                   finalize=None, scope=scope, sort=sort, verbose=verbose, jsMode=jsMode)
    return mr2[1], mr1[0], mr2[0]


def MRfields(collection, query={}, out={"replace": 'mrfields.tmp'}, verbose=2, doMeta=False,
             scope={'parms': {'levelMax': -1, 'inclHeaderKeys': True, 'ExamplesMax': 2}}):
    """A utility which intentifies all field's names used by documents of a collection
    """
    FunMap = parseJSfunFromFile(PATH_JS, 'MapKeys')
    FunReduce = parseJSfunFromFile(PATH_JS, 'ReduceKeys')
    rt = MRsimple(collection, FunMap, FunReduce=FunReduce, query=query, out=out,
                  scope=scope, verbose=verbose)
    collection = rt[1]
    totalRecords = float(rt[0]['counts']['input'])
    totalCnt = 0
    if verbose > 0:
        print "calculating percentages"
    for doc in collection.find():
        cnt = doc['value']['cnt']
        percent = (cnt / totalRecords) * 100
        doc['value']['percent'] = percent
        collection.update({'_id.fields': doc['_id']['fields']},
                          {"$set": {"value.percent": percent}}, safe=True, multi=False)
        #@warning:  don't use {_id:id} does not work posibly coz diffirent subfields order
        #print collection.find_one({'_id':doc['_id']}, safe=True)
        totalCnt += cnt
    if verbose > 0:
        print "creating intexes"
    rt[1].ensure_index('_id.type', background=True)
    if doMeta:
        rtMeta = MRfieldsMeta(rt, verbose=verbose)
        return rt, rtMeta
    else:
        return rt


def MRfieldsMeta(mr_keys_results, verbose=2):
    FunMap = parseJSfunFromFile(PATH_JS, 'MetaMapKeys')
    FunReduce = parseJSfunFromFile(PATH_JS, 'MetaReduceKeys')
    totalRecords = mr_keys_results[0]['counts']['output']
    out = {'reduce': mr_keys_results[1].name}
    rt = MRsimple(mr_keys_results[1], FunMap, FunReduce=FunReduce, query={'_id.type': 'fieldsGrp'},
                  out=out, scope={'parms': {'totalRecords': totalRecords}}, verbose=verbose)
    return rt


class MongoRSClient(MongoReplicaSetClient):
    def __init__(self, parent, *args, **kwargs):
        self.parent = parent
        super(MongoRSClient, self).__init__(use_greenlets=True, *args, **kwargs)
        #@bug:  if we use use_greenlets=True monitor is not running

    def refresh(self, initial=False):
        ''' override refresh '''
        super(MongoRSClient, self).refresh(initial)
        self.parent.rs_refresh() 
class MdbClient(SubToEvent):
    '''a mongo client self.client is replicaSetClient
       if 'hosts_or_uri' is defined in kwargs else from MongoClient
       a dbName or None has to be provided as default to self.db
       use it in a 'with' statement or else call close() when instance is not needed
       other options  hosts_or_uri=None, max_pool_size=10, document_class=dict, tz_aware=False etc..
       see: http://api.mongodb.org/python/current/api/pymongo/mongo_replica_set_client.html
            http://api.mongodb.org/python/current/api/pymongo/mongo_client.html
    '''
    @autoRetry(ConnectionFailure)
    def __init__(self, *args, **kwargs):
        super(MdbClient, self).__init__('mongodb')
        auto = kwargs.get('auto')
        if auto:
            del kwargs['auto']
            kwauto = mongoConfToPy()
            kwauto.update(kwargs)
            #@note: update and overwrite from origina kwargs
            kwargs = kwauto
        dbName = kwargs.get('dbName')
        if dbName:
            del kwargs['dbName']
        hosts_or_uri = kwargs.get('hosts_or_uri', False)
        replicaSet = kwargs.get('replicaSet', False)
        if replicaSet is None:
            del kwargs['replicaSet']
            #@note:  delete if None so we  can call it with None
        self.client = None
        if hosts_or_uri:
            self.client = MongoReplicaSetClient(*args, **kwargs)
            self.rs_last_primary = self.client.primary
            self.rs_last_secondaries = self.client.secondaries
        else:
            #kwargs['host']=kwargs.get('host') or kwargs.get('hosts_or_uri')
            #if kwargs.get('hosts_or_uri'):del kwargs['hosts_or_uri']
            self.client = MongoClient(*args, **kwargs)
        self.db = self.client[dbName] if dbName else None
        self.setUpCollections()

    def setUpCollections(self):
        """overide in subclasses to initialize collections i.e:
                self.users=self.db['users']
                self.users.ensure_index ("foo")
        """
        return NotImplemented

    def isReplicaset(self):
        return isinstance(self.client, MongoReplicaSetClient)

    def rs_refresh(self):
        '''for some reason (probably coz monitor lives in its own thread)
           if we got an error is not raised here
        '''
        try:
            if self.client:
                self.rs_changes_check()
        except Exception, e:
            print "warning rs_rsfresh " * 4, str(e)
            #@warning: remove before production
            self.fireTopic('error', '', str(e))

    def rs_changes_check(self):
        #self.fireTopic('rs_refresh', 'start', None)
        #self({'topic': 'rs_refresh', 'action': 'start'})
        if self.rs_last_primary != self.client.primary:
            self.rs_changedPrimary()
            self.rs_last_primary = self.client.primary
        if self.rs_last_secondaries != self.client.secondaries:
            self.rs_changedSecondaries()
            self.rs_last_secondaries = self.client.secondaries
        #self.fireTopic('rs_refresh', 'end', None)

    def rs_changedPrimary(self):
        self.fireTopic('rs_change', 'primary', self.client.primary)

    def rs_changedSecondaries(self):
        self.fireTopic('rs_change', 'secondaries', self.client.secondaries)

    def dropCollections(self, db=None, startingwith=['tmp.mr', 'tmp_', 'del_']):
        if db is None:
            db = self.db
        if db:
            dbcols = db.collection_names()
            colsToRemove = [c for c in dbcols if any([c.startswith(i) for i in startingwith])]
            for c in colsToRemove:
                (db.drop_collection(c))
            return colsToRemove

    def collStats(self, collectionNameStr):
        return self.db.command("collstats", collectionNameStr)

    def CappedColSetOrGet(self, colName, dbName=None, sizeBytes=1000000,maxDocs=None):
        """http://docs.mongodb.org/manual/core/capped-collections/"""
        if dbName is None:
            dbName = self.db.name
        if dbName:
            return CappedColSetOrGet(self.client, dbName, colName, sizeBytes, maxDocs) 

    def close(self):
        if hasattr(self, 'client'):
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, tp, value, traceback):
        self.close()
        #close is alias for disconnect in simple client but seems differ  in MongoReplicaSetClient
        #self.client.disconnect()  #@note: check if it is ther in case an
        return False
        #@info False so we raised error see:http://docs.python.org/release/2.5/whatsnew/pep-343.html

    def __del__(self):
        if self:
            self.close()


class MdbClientOrInstance(object):
    def __init__(self, client=None, *args, **kwargs):
        if  not client is None:
            #@note client can be  a MdbClient, MdbClient or MongoClient instance
            self.isProxy = True
            if hasattr(client, 'client'):
                self.MdbClient = client
                self.client = client.client
            else:
                self.MdbClient = None
                self.client = client
        else:
            self.isProxy = False
            self.MdbClient = MdbClient(*args, **kwargs)
            self.client = self.MdbClient.client

    def close(self):
        if hasattr(self, 'client') and not self.isProxy and self.client:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, tp, value, traceback):
        return False

    def __del__(self):
        if hasattr(self, 'client') and self.client and not self.isProxy:
            del self.client
            #self.client.disconnect()


class MdbCl(object):
    '''a mongo client that inherits from MongoReplicaSetClient if replicaSet is specified in kwargs
       else from MongoClient
       Important: Do NOT use it as base class it does NOT support inheritance
    '''
    def __new__(cls, *args, **kwargs):
        base_class = MongoReplicaSetClient if 'replicaSet' in kwargs.keys() else MongoClient
        x = type(base_class.__name__ + '.' + cls.__name__,
                 (MdbCl, base_class), {})
        return super(MdbCl, cls).__new__(x, cls)
#    def __init__(self, hosts_or_uri=None, max_pool_size=10,
#                 document_class=dict, tz_aware=False, dbName=None, **kwargs):

    def __init__(self, *args, **kwargs):
        self.last_primay = False
        self.last_secondaries = False
        super(MdbCl, self).__init__(*args, **kwargs)
        #self.setUpCollections()

    def isReplicaset(self):
        return isinstance(self, MongoReplicaSetClient)

    def refresh(self, initial=False):
        rt = super(MdbCl, self).refresh(initial)
        self.RSetChangesCheck()
        return rt

    def RSetChangesCheck(self):
        print "checkChanges [%s][%s]" % (str(self.primary), str(self.secondaries))
        if self.last_primay != self.primary and not self.primary is False:
            self.changedPrimary()
            self.last_primay = self.primary
        if self.last_secondaries != self.secondaries and not self.secondaries is False:
            self.changedSecondaries()
            self.last_secondaries = self.secondaries

    def changedPrimary(self):
        return self.primary

    def changedSecondaries(self):
        return self.secondaries

    def setUpCollections(self):
        """overide in subclasses to initialize collections i.e:
                self.users = self.db['users']
                self.users.ensure_index ("foo")
        """
        return NotImplemented

    def dropCollections(self, db=None, startingwith=['tmp.mr', 'tmp_', 'del_']):
        if db is None:
            db = self.db
        if db:
            dbcols = db.collection_names()
            colsToRemove = [c for c in dbcols if any([c.startswith(i) for i in startingwith])]
            for c in colsToRemove:
                (db.drop_collection(c))
            return colsToRemove

    def collStats(self, collectionNameStr):
        return self.db.command("collstats", collectionNameStr)

    def CappedColSetOrGet(self, colName, dbName=None, sizeBytes=100000, maxDocs=None):
        if dbName is None:
            dbName = self.db.name
        if dbName:
            return CappedColSetOrGet(self.client, dbName, colName, sizeBytes, maxDocs)  

class SubToCapped(object):
    '''
        generic class for Subscribing to a capped collection
        runs as Greenlet so it won't block
        clients should treat all properties as strictly read_only
        set autoJoin to True to start it imediatlly or .join() the instance it creates
        set func = a function to be executed for each document received or/and override onDoc
        set retryOnDeadCursor to retry if cursor becomes dead as is the case on an epmpty collection
        untill it has at least one document stored
        i.e. to view your local  see: example test_SubToCappedOptLog
        startFrom =  A ts field value to start after or True to start after Last Doc aplies only
        to collections wtith ts field
        query a query i.e. {'topic': 'to', 'machine': 'ms'} or None
        retryOnDeadCursor must be True if the collection is empty when we start
        def test_monitorOpLog():
            mconf = mongoConfToPy()
            dbcl = MdbClient(**mconf)
            def log_docs(doc):print  " . . . "* 20, "\n%s" %(str (doc))
            mon = SubToCapped(dbcl.client.local.oplog.rs , func=log_docs)
        @note check here for creating caped collection with ts field
        http://blog.pythonisito.com/2013/04/mongodb-pubsub-with-capped-collections.html
        https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py
    '''
    def __init__(self, cappedCol, fields=None, startFrom=True, channel=None, func=None,
                 autoJoin=True, retryOnDeadCursor=False, query=None):
        if not cappedCol.options().get('capped'):
            raise ValueError("collection_is_not_capped")
            return
        #super(SubToCapped, self).__init__(channel if channel else cappedCol.full_name)
        #Greenlet.__init__(self)
        if func:
            self.onDoc = func
        self.collection = cappedCol
        self.isOpLog = cappedCol.full_name == 'local.oplog.rs'
        self.query = query
        #self.query = None if (isOpLog and startFrom) else query #@note: not compatible with
        #_QUERY_OPTIONS['oplog_replay']  see:database error: no ts field in query
        self.fields = fields
        self.retryOnDeadCursor = retryOnDeadCursor
        self.startFrom = startFrom
        self.channel = channel if channel else cappedCol.full_name
        self.id_start = None
        self.id_last = None
        self.t_start = datetime.utcnow()
        self.dt_last = None
        self.docs_fetched = 0
        self.docs_skipped = 0
        self.__stop = False
        #self.start()
        self.glet = Greenlet.spawn(self._run)
        if autoJoin:
            self.join()
        #if autoJoin:self.join()

    def join(self):
        self.glet.join()

    def oplogCursorSkipToDoc(self, Doc=None):
        if Doc is None:
            Doc = self.collection.find_one(sort=[("$natural", -1)]).hint([('$natural', -1)])
        currentTs = Doc.get('ts')
        if currentTs:
            queryTs = {'ts': {'$gt': currentTs}} if Doc else None
            cursor = self.collection.find(queryTs, tailable=True, await_data=True)
            cursor.hint([('$natural', -1)])
            cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
            return cursor
        else:
            return None

    def getCursor(self):
        lastDoc = None
        if self.startFrom is True:
            lastDoc = self.collection.find_one(sort=[("$natural", -1)], hint=([('$natural', -1)]))
        elif self.startFrom:
            lastDoc = self.collection.find_one({'ts': self.startFrom})
        if lastDoc:
            currentTs = lastDoc.get('ts')
            if currentTs:
                query = {'ts': {'$gt': currentTs}}
                if self.query:
                    query.update(self.query)
                cursor = self.collection.find(query, fields=self.fields,
                                              tailable=True, await_data=True)
                cursor.hint([('$natural', -1)])
                cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
                return cursor
        cursor = self.collection.find(self.query, fields=self.fields,
                                     tailable=True, await_data=True)
        if self.startFrom:
            self.docs_skipped = cursor.count()
            cursor.skip(self.docs_skipped)
        return cursor

    def timeSinceStart(self):
        return datetime.utcnow() - self.t_start

    def timeSinseLastDoc(self):
        if self.dt_last:
            return datetime.utcnow() - self.dt_last
        else:
            return None

    def docsPerMinuteAvg(self):
        return (self.docs_fetched / self.timeSinceStart().total_seconds()) * 60

    @autoRetry(AutoReconnect, 6, 0.5, 1)
    def _run(self):
        retry = True
        while retry and (not self.__stop):
            #print "geting cursor"
            cursor = self.getCursor()
            while cursor.alive  and (not self.__stop):
                try:
                    doc = cursor.next()
                    if not self.id_start:
                        self.id_start = doc.get('_id', True)
                    self.id_last = doc.get('_id', None)
                    self.dt_last = datetime.utcnow()
                    if self.onDoc(doc):
                        self.docs_fetched += 1
                except StopIteration:
                    sleep(0.3)
                    pass
                except Exception:
                    raise
            self.startFrom = False
            #@ since we got here, skip is meaningless
            if self.retryOnDeadCursor:
                sleep(1)
            else:
                retry = False
        self.onExit(cursor)

    def onDoc(self, doc):
        '''overide this if you wish or pass a function on init and
           return True if U want to increment docs_fetched
        '''
        return NotImplemented

    def onExit(self, cursor):
        '''overide if you wish, you can check if cursor.alive'''
        return NotImplemented

    def stop(self):
        self.__stop = True
        self.glet.kill(exception=GreenletExit, block=True, timeout=None)
        print "stoped " * 4 

def CappedColSetOrGet(client, dbName, colName, sizeBytes=1000000, maxDocs=None, autoIndexId = True):
    """
        http://docs.mongodb.org/manual/tutorial/use-capped-collections-for-fast-writes-and-reads/
        autoIndexId must be True for replication so must be True except on a stand alone mongodb or collection belongs to 'local' db 
    """
    if colName not in client[dbName].collection_names():
        return client[dbName].create_collection(colName, capped=True, size=sizeBytes, max=maxDocs, autoIndexId=autoIndexId) 
    else:
        cappedCol = client[dbName][colName]
        if not cappedCol.options().get('capped'):
            client[dbName].command({"convertToCapped": colName, 'size': sizeBytes})
        return cappedCol 
    
class  PubSub(object):
    def __init__(self, client, dbName, colName, size=10000000, maxdocs=None):
        #self.PubSubCol = client[dbName][colName]
        self.v_client, self.v_dbName, self.v_colName, self.v_size, self.v_maxdocs = client, dbName, colName, size, maxdocs
        self.AuxToolsObj = AuxTools(client, dbName=dbName, colName=colName + '_PB')
        self._CappedColSetOrGet()

    def _CappedColSetOrGet(self):
        self.PubSubCol = CappedColSetOrGet(self.v_client, self.v_dbName, self.v_colName,
                                           self.v_size, self.v_maxdocs)

    def col_reset(self):
        self.col_drop()
        self.self._CappedColSetOrGet()

    def col_drop(self):
        return self.v_client[self.v_dbName].drop_collection(self.v_colName)

    @autoRetry(AutoReconnect, 6, 1, 1)
    def _pub(self, topic, verb, payload, machine_name, **kwargs):
        ts = self.AuxToolsObj.sequence_next(self.v_colName)
        cm = {'_id': ts, 'machine': machine_name, 'topic': topic, 'verb': verb,
              'payload': payload, 'ts': ts}
        rt = self.PubSubCol.insert(cm, **kwargs)
        #self.PubSubCol.update({'_id': rt}, {"$set": {'ts': rt}})
        return rt

    def pub(self, topic, verb, payload, machine_name=None, async=True, **kwargs):
        if async:
            return Greenlet.spawn(self._pub, topic, verb, payload, machine_name, **kwargs)
            #@note: none blocking coz we call it from other threads join to execute
        else:
            return self._pub(topic, verb, payload, machine_name, **kwargs)


class AuxTools(object):
    '''based on https://github.com/rick446/MongoTools/blob/master/mongotools/sequence/sequence.py'''
    def __init__(self, client, dbName='AuxTools', colName='helpers'):
        self.client = client
        self.db = client[dbName]
        self.coll = 'helpers' if colName is None else self.db[colName]
        self.coll.self.client.read_preference = ReadPreference.PRIMARY
        #@note make sure sequence_current gets correct value

    def sequence_collection(self, collName):
        return self.coll if collName is None else self.db[collName]

    def sequence_current(self, seqName, collName=None):
        seqColl = self.sequence_collection(collName)
        doc = seqColl.find_one({'_id': seqName})
        return 0 if doc is None else doc['val']

    def sequence_next(self, seqName, collName=None, inc=1):
        seqColl = self.sequence_collection(collName)
        return seqColl.find_and_modify({'_id': seqName}, {'$inc': {'val': inc}},
                                       upsert=True, new=True)['val']
#===============================================================================
# def test_SubToCappedOptLog():
#     mconf = mongoConfToPy('mongonm01')
#     dbcl = MdbClient(**mconf)
#     def log_docs(doc):
#     print  ". . . . "* 20, "\n%s" %(str (doc))
#     SubToCapped(dbcl.client.local.oplog.rs , func=log_docs)
# if __name__ == "__main__":
#     test_SubToCappedOptLog()
#===============================================================================