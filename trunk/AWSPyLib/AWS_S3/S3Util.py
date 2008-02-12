#!/usr/bin/env python
# Copyright (c) <2008> <Kiran Bhageshpur - kiran_bhageshpur at hotmail dot com>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import os
import os.path
import time
import string
import httplib
import md5
import xml.sax
import logging

from AWSPyLib.AWS_Exception import *
from AWSPyLib.AWSConfig import Config
import AWSPyLib.AWS_S3.S3Rest as S3
import AWSPyLib.AWS_GenUtilities as Util

NUMBER_OF_RETRIES = 3

MD_SIZE               = 'size'
MD_CREATE_TIME        = 'create-time'
MD_LAST_MODIFIED_TIME = 'last-modified-time'

appLog  = logging.getLogger('app')
cbackLog = logging.getLogger('app.cback')

doc=""" 
This module contains various utility functions and classes 
required to interact with amazon S3
"""

def _default_cb_( method, bucket, key, length):
    cbackLog.info( '%s %s %s %d' % (method, bucket, key, length) )

def _download_dir_cb_ ( parent_dir, child_dir, child_file, arg ):

    conn = S3.AWSAuthConnection(Config.AWSProperties['AccessKey'], \
                                Config.AWSProperties['SecretKey'], ( Config.AWSProperties['SecureComm'] == True) )
    
    num_files = 0
    for entry in child_dir:
        entry = string.replace ( entry, Util.DELIMITER, os.sep)
        dir_path = arg[1]+os.sep+entry
        if ( os.path.exists (dir_path) == False):
            os.mkdir ( dir_path)
            appLog.debug ('Created <DIR> %s' % dir_path)
            
    for entry in child_file:
        file_name = string.replace ( entry, Util.DELIMITER, os.sep)        
        file_path = arg[1]+os.sep+file_name
        key = AWS_Key ( conn, arg[0], entry)
        fDownloaded = key.sync_download_to_file ( file_path)
        if (fDownloaded):
            appLog.debug ('Downloded %s' % entry )
            num_files += 1
        
    appLog.info ( '<%s>: Downloaded %d files' % (parent_dir, num_files) )
    
    return 

def _upload_dir_cb_(bucketName, dirname, names):
    
    conn = S3.AWSAuthConnection(Config.AWSProperties['AccessKey'], \
                                Config.AWSProperties['SecretKey'], ( Config.AWSProperties['SecureComm'] == True) )
    num_files_uploaded = 0
    for entry in names:
        
        full_path = dirname+os.sep+entry
        
        if (os.path.isfile(full_path) ):
            
            (drive, keyName) = os.path.splitdrive(full_path)
            keyName = os.path.normcase(keyName)
            keyName = os.path.normpath(keyName)
            if (keyName[0] == os.sep):
                keyName = keyName[1:]
            keyName = string.replace(keyName, os.sep, Util.DELIMITER)
                        
            key = AWS_Key( conn, bucketName, keyName )
            num_files_uploaded +=1
            key.put_object_from_file ( full_path )
            appLog.debug( '[%d] Processing [%s]: Uploaded <%s>' % (num_files_uploaded, dirname, entry) )
            time.sleep(0.00)
    
    appLog.info ('Uploaded [%d] files in <%s> to bucket=%s' % (num_files_uploaded, dirname, bucketName) )
                  
    return

def _sync_upload_dir_cb_(bucketName, dirname, names):

    conn = S3.AWSAuthConnection(Config.AWSProperties['AccessKey'], \
                                Config.AWSProperties['SecretKey'], ( Config.AWSProperties['SecureComm'] == True) )

    num_files_uploaded = 0
    
    for entry in names:
        
        full_path = dirname+os.sep+entry
        
        if (os.path.isfile(full_path) ):
            
            (drive, keyName) = os.path.splitdrive(full_path)
            keyName = os.path.normcase(keyName)
            keyName = os.path.normpath(keyName)
            if (keyName[0] == os.sep):
                keyName = keyName[1:]
                        
            key = AWS_Key( conn, bucketName, keyName )
            fUploaded = key.sync_upload_from_file (full_path)
            if fUploaded:                
                appLog.debug('Uploaded [%s]:<%s>' % (dirname, entry) )
                num_files_uploaded += 1
                time.sleep(0.00)
            
    appLog.info('Uploaded [%d] files in <%s> to %s' % (num_files_uploaded, dirname, bucketName) )
    return

class _S3Object_:
    def __init__(self, data, metadata={}):
        self.data = data
        self.metadata = metadata

class _Response_:
    def __init__(self, http_response):
        self.body = ''
        self.http_response = http_response
        # you have to do this read, even if you don't expect a body.
        # otherwise, the next request fails.
        if ( http_response.status < 300 ):
            self.body = http_response.read( )

    def _get_header_( self, header ):
        return (self.http_response.getheader( header ) )

    def _get_msg_( self ):
        return (self.http_response.msg )

    def _get_status_ ( self ):
        return (self.http_response.status)

    def _get_reason_( self ):
        return (self.http_response.reason)

    def _get_version_( self ):
        return (self.http_response.version)        

    def _get_date_ ( self ):
        return (self._get_header_('date') )

    def _get_last_modified_( self ):
        return (self._get_header_('last-modified') )

    def _get_etag_ (self ):
        return (self._get_header_('etag').strip('\"') )

    def _get_content_type_(self):
        return (self._get_header_('content-type') )

    def _get_content_length_(self):
        return (int( self._get_header_('content-length') ) )

    def _get_server_(self):
        return (self._get_header_('server'))


class _GETResponse_(_Response_):
    def __init__(self, http_response):
        _Response_.__init__(self, http_response)
        response_headers = http_response.msg   # older pythons don't have getheaders
        metadata = self._get_aws_metadata(response_headers)
        self.object = _S3Object_(self.body, metadata)

    def _get_aws_metadata(self, headers):
        metadata = {}
        for hkey in headers.keys():
            if hkey.lower().startswith(S3.METADATA_PREFIX):
                metadata[hkey[len(S3.METADATA_PREFIX):]] = headers[hkey]
                del headers[hkey]

        return metadata

    def _get_metadata_value_ (self, key ):
        if ( self.object.metadata.has_key (key) ):
            return (self.object.metadata[key])
        else:
            raise InvalidAttribute ( key )

    def _get_data_(self):
        return (self.object.data)
    
class AWS_S3:
    doc = """
        This class encapsulates operations on AWS S3 buckets.

        The method naming convention is all methods starting with lower case
        are lower level methods that use S3Rst::AWSConnectionObject to 
        interact with AWS.  Methods whose names start with uppercase are presentation
        layer methods    
        """
    def __init__(self, AWSConnectionObject):
        doc = """
            AWSConnObject: An instance of AWSConnectionObject used to communicate with S3
            """
        self.CONN = AWSConnectionObject
        self.ListOfBuckets = []
        return

    class _ListOfAllBucketsResponse_(_Response_):
        def __init__(self, http_response):
            _Response_.__init__(self, http_response)
            if self._get_status_() < 300: 
                handler = self._ListOfAllBucketsHandler_()
                xml.sax.parseString(self.body, handler)
                self.entries = handler.entries
            else:
                self.entries = []

        class _ListOfAllBucketsHandler_(xml.sax.ContentHandler):
            def __init__(self):
                self.entries = []
                self.curr_entry = None
                self.curr_text = ''

            def startElement(self, name, attrs):
                if name == 'Bucket':
                    self.curr_entry = self.Bucket()

            def endElement(self, name):
                if name == 'Name':
                    self.curr_entry.name = self.curr_text
                elif name == 'CreationDate':
                    self.curr_entry.creation_date = self.curr_text
                elif name == 'Bucket':
                    self.entries.append(self.curr_entry)

            def characters(self, content):
                self.curr_text = content

            class Bucket:
                def __init__(self, name='', creation_date=''):
                    self.name = name
                    self.creation_date = creation_date

    def get_bucket_name ( self, index ):
        return ( self.ListOfBuckets[index].name )

    def get_list_of_buckets( self ):
        doc="""
           Retrieve the list of all buckets owned by the credentials used to construct the AWSConnectionObject
           contained herein
           """
        try:
            resp = self._ListOfAllBucketsResponse_( self.CONN.make_request ( method = 'GET' ) )
            self.ListOfBuckets = resp.entries

        except Exception, e:
            logging.error ('ERROR %s' % e)
            raise

        return

    def add_bucket( self, bucketName ):
        doc="""
           Add a new bucket with the name bucketName
           """
        try:
            resp = _Response_ ( self.CONN.make_request ( method = 'PUT', bucket = bucketName ) )        

        except Exception, e:
            logging.error ('ERROR %s' % e)
            raise

        return


    def delete_bucket ( self, bucketName):
        doc="""
           Delete specified bucket, fails if bucket has any keys
           """
        try:
            resp = _Response_ ( self.CONN.make_request (method = 'DELETE', bucket = bucketName ) )

        except Exception, e:
            logging.error ('ERROR %s' % e)
            raise

        return

    def delete_bucket_recursive( self, bucketName):
        doc="""
           Recursively delete all keys in the bucket and then deletes the bucket.
           """

        try:
            BucketContent = AWS_Bucket (self.CONN, bucketName)
            BucketContent.get_list_of_keys_in_bucket( )
            for entry in BucketContent.bucketContent:
                key = AWS_Key(self.CONN, bucketName, entry['name'], None )
                key.delete_object( )
                time.sleep(0.00)

                # Now let's attempt to delete the bucket 
            self.delete_bucket(bucketName)

        except Exception, f:
            logging.error ('ERROR %s' % f)
            raise 
        
        return
    
class AWS_Bucket:
    doc="""
       This class represents a bucket and the operations to get the contents of this bucket
       """

    def __init__(self, AWSConnectionObject, bucketName, cb = _default_cb_):
        self.CONN = AWSConnectionObject
        self.bucketName = bucketName
        self.cb         = cb
        self._reinit_( )

    def _reinit_(self):
        self.bucketContent = []        
        self.root_dir= Util.my_dir( s3key=u'', name=u'',depth=0)
        self.dir_stack = []
        self.dir_stack.append(self.root_dir)
        self.count = 0
        self.keyName = ''

    class _ListBucketResponse_(_Response_):
        def __init__(self, http_response):
            _Response_.__init__(self, http_response)
            if self._get_status_( )< 300:
                handler = self._ListBucketHandler_()
                xml.sax.parseString(self.body, handler)
                self.entries = handler.entries
                self.common_prefixes = handler.common_prefixes
                self.name = handler.name
                self.marker = handler.marker
                self.prefix = handler.prefix
                self.is_truncated = handler.is_truncated
                self.delimiter = handler.delimiter
                self.max_keys = handler.max_keys
                self.next_marker = handler.next_marker
            else:
                self.entries = []

        class _ListBucketHandler_(xml.sax.ContentHandler):
            def __init__(self):
                self.entries = []
                self.curr_entry = None
                self.curr_text = ''
                self.common_prefixes = []
                self.curr_common_prefix = None
                self.name = ''
                self.marker = ''
                self.prefix = ''
                self.is_truncated = False
                self.delimiter = ''
                self.max_keys = 0
                self.next_marker = ''
                self.is_echoed_prefix_set = False

            class Owner:
                def __init__(self, id='', display_name=''):
                    self.id = id
                    self.display_name = display_name

            class ListEntry:
                def __init__(self, key='', last_modified=None, etag='', size=0, storage_class='', owner=None):
                    self.key = key
                    self.last_modified = last_modified
                    self.etag = etag
                    self.size = size
                    self.storage_class = storage_class
                    self.owner = owner

            class CommonPrefixEntry:
                def __init(self, prefix=''):
                    self.prefix = prefix

            def startElement(self, name, attrs):
                if name == 'Contents':
                    self.curr_entry = self.ListEntry()
                elif name == 'Owner':
                    self.curr_entry.owner = self.Owner()
                elif name == 'CommonPrefixes':
                    self.curr_common_prefix = self.CommonPrefixEntry()

            def endElement(self, name):
                if name == 'Contents':
                    self.entries.append(self.curr_entry)
                elif name == 'CommonPrefixes':
                    self.common_prefixes.append(self.curr_common_prefix)
                elif name == 'Key':
                    self.curr_entry.key = self.curr_text
                elif name == 'LastModified':
                    self.curr_entry.last_modified = self.curr_text
                elif name == 'ETag':
                    self.curr_entry.etag = self.curr_text
                elif name == 'Size':
                    self.curr_entry.size = int(self.curr_text)
                elif name == 'ID':
                    self.curr_entry.owner.id = self.curr_text
                elif name == 'DisplayName':
                    self.curr_entry.owner.display_name = self.curr_text
                elif name == 'StorageClass':
                    self.curr_entry.storage_class = self.curr_text
                elif name == 'Name':
                    self.name = self.curr_text
                elif name == 'Prefix' and self.is_echoed_prefix_set:
                    self.curr_common_prefix.prefix = self.curr_text
                elif name == 'Prefix':
                    self.prefix = self.curr_text
                    self.is_echoed_prefix_set = True
                elif name == 'Marker':
                    self.marker = self.curr_text
                elif name == 'IsTruncated':
                    self.is_truncated = self.curr_text == 'true'
                elif name == 'Delimiter':
                    self.delimiter = self.curr_text
                elif name == 'MaxKeys':
                    self.max_keys = int(self.curr_text)
                elif name == 'NextMarker':
                    self.next_marker = self.curr_text

                self.curr_text = ''

            def characters(self, content):
                self.curr_text += content

    def _walk_dir_ ( self, start, cb_method, arg):
        doc="""
        Walks an in-memory directory stucture starting at "start" and invokes the
        passed in cb_method on each child in the directory and passes along to the
        cb_method the arguments passed in "arg"
        
        This is a recursive function
        """
        
        parent_dir = start.s3_key        
        child_dir = []
        child_file = []
        
        keys = start.children.keys()
        for k in keys:
            entry = start.get_child( k )
            if ( entry.__name__ == 'my_dir'):
                child_dir.append ( entry.s3_key )
            elif (entry.__name__ == 'my_file'):
                child_file.append (entry.s3_key )
            else:
                raise Failed()

        cb_method ( parent_dir, child_dir, child_file, arg )
        
        for k in keys:
            entry = start.get_child(k)
            if ( entry.__name__ == 'my_dir'):
                self._walk_dir_ ( entry, cb_method, arg)
                        
        return
                
    def get_list_of_keys_in_bucket ( self ):
        doc="""
           This function does handles pagination.  Get's all keys in the bucket identified by 
           self.bucketName, 1000 at a time and store them in self.bucketContent
           """
        marker = ''
        while True:
            try:
                resp = self.CONN.make_request ( method='GET', bucket=self.bucketName, \
                                                query_args={'max-keys':100, 'marker':marker} )
            except Exception, e:
                logging.error ('ERROR %s' % e)
                raise 

            keylist = self._ListBucketResponse_( resp )

            if self.cb:
                self.cb('ALLKEYS', self.bucketName, '', self.count )

            for entry in keylist.entries:
                self.bucketContent.append( {'index': self.count, 'type': 'file', 'name':entry.key, 
                                            'size':entry.size, 'owner':entry.owner.display_name, \
                                            'last_modified':entry.last_modified, 'etag':entry.etag} )
                self.count = self.count + 1

            if (keylist.is_truncated == False):
                # we are done...
                break
            else:
                marker = entry.key

        return


    def get_keys_in_bucket_as_fstree( self, prefixDir=u'', delimiter=Util.DELIMITER):
        doc="""
           prefixDir:  The unicode string of the directory used as prefix in AWS call (see listing key)
           delimiter:  The unicode string used as delimiter in key name.

           This function does not handle pagination.  If the total number of keys exceed 1000 (AWS limit)
           we get only the first set of keys

           The keys are stored in a file structure rooted at self.root_dir.
           
           There is an implicity assumption that the keys in the S3 bucket are actually files in a FS tree structure

           However, this method retrives keys using the AWS prefixDir and delimiter mechanism as such 
           we only have a problem with pagination if there are more than a 1000 keys that roll up.

           This is a recursive function
           """
        try:
            resp = self.CONN.make_request ( method='GET', bucket=self.bucketName, \
                                            query_args={'prefix':prefixDir, 'delimiter':delimiter} )
        except S3Error, e:
            logging.error ( str(e) )
            raise
        except Exception, f:
            logging.error ('ERROR %s' % f)
            raise
        else:

            dirContents = self._ListBucketResponse_( resp )

            if self.cb:
                self.cb('KEYLIST', self.bucketName, prefixDir, self.count )

            topdir = self.dir_stack.pop( )

            for fileEntry in dirContents.entries:
                full_name = fileEntry.key
                j = full_name.rfind(Util.DELIMITER)
                    
                file_name = full_name[j+1:]
                f = Util.my_file( fileEntry.key, file_name, topdir.depth+1, fileEntry.size, \
                                  fileEntry.owner.display_name, fileEntry.last_modified, fileEntry.etag, prefixDir )

                topdir.add_child ( f )
                self.count = self.count + 1

            for dirEntry in dirContents.common_prefixes:
                full_name = dirEntry.prefix[:-1]
                j = full_name.rfind(Util.DELIMITER)
                
                dir_name = full_name[j+1:]
                
                d = Util.my_dir ( full_name, dir_name, topdir.depth+1, prefixDir)
                
                topdir.add_child ( d )
                self.count = self.count + 1
                
            for dirEntry in dirContents.common_prefixes:
                full_name = dirEntry.prefix[:-1]
                j = full_name.rfind(Util.DELIMITER)
                
                dir_name = full_name[j+1:]                    
                d = topdir.get_child( dir_name)
                if not d:
                    raise Failed()

                self.dir_stack.append(d)
                self.get_keys_in_bucket_as_fstree( dirEntry.prefix, delimiter )

            return
                   
    def upload_dir( self, root_dir ):
        doc="""
        Uploads all files under root_dir up to the bucket identified by self.bucketName recursively
        """
        
        if root_dir == '':
            raise InvalidAttribute('root_dir')        
        elif ( os.path.isdir ( root_dir ) != True):
            raise InvalidAttribute('root_dir')
        
        os.path.walk ( root_dir, _upload_dir_cb_, self.bucketName)
        
        return

    def sync_upload_dir ( self, root_dir):
        doc="""
        Recursively walks the files system tree starting at root_dir and uploads all those
        files under root_dir that do not match the content in S3.
        
        An entire FS directory can be uploaded for the first time using this method
        but it would be more inefficient that using upload_dir since sync_upload_from_file
        first checks with AWS to see if the object exists and what it's properties (last modified
        date, hash, size) are
        
        This method is preferable when there have been sparse changes to the local copies
        """
        if root_dir == '':
            raise InvalidAttribute('root_dir')
        elif ( os.path.isdir ( root_dir ) != True):
            raise InvalidAttribute('root_dir')
        
        os.path.walk ( root_dir, _sync_upload_dir_cb_, self.bucketName)

        return

    def download_dir( self, root_dir):
        doc="""
        Downloads content of self.bucketName into a FS tree structure starting at root_dir
        
        Assumes that the keys represent a FS 
        
        If the files already exist, then they are downloaded again ONLY if there is a difference between 
        what's on S3 and what's already present locally
        """
        if root_dir == '':
            raise InvalidAttribute('root_dir')        

        if (os.path.exists(root_dir) == True) and (os.path.isdir(root_dir) == True):
            self._reinit_( )
            self.get_keys_in_bucket_as_fstree( )
            root_dir = os.path.normcase(root_dir)
            root_dir = os.path.normpath(root_dir)
            self._walk_dir_ ( self.root_dir, _download_dir_cb_, [self.bucketName, root_dir])
        
        return
                    
class AWS_Key:
    doc = """
        This class encapsulates operations on a specific AWS S3 bucket contents

        The method naming convention is all methods starting with lower case
        are lower level methods that use S3Rst::AWSConnectionObject to 
        interact with AWS.  Methods whose names start with uppercase are presentation
        layer methods    
        """

    def __init__ ( self, AWSConnectionObject, bucketName, keyName, cb=_default_cb_):
        doc = """
           AWSConnObject: An instance of AWSConnectionObject used to communicate with S3
           bucketName: Name of the bucket
           keyName: Name of the key
           """
        self.bucketName = bucketName
        self.keyName = keyName
        self.CONN= AWSConnectionObject
        self.cb = cb


    def _stream_data_to_file_ (self, fileName, file_size):
        doc = """
           Private method that retrieves (if it exists!) the S3 object identified by bucketName+keyName 
           to the file identified by fileName
           """

        if not fileName:
            raise InvalidAttribute ( 'fileName')

        numTries = 0
        fp = 0
        start = 0
        end = Util.CHUNK_SIZE-1
        resp = 0
        local_hash = 0

        if (Config.AWSProperties['CheckHash'] == True):
            fileHash = md5.new( )
        

        try:
            while True:                           
                byteRange = "bytes=%d-%d" % (start, end)

                resp = _GETResponse_( self.CONN.make_request ( 'GET', bucket=self.bucketName, key=self.keyName, \
                                                               headers={"Range":byteRange} ) )

                status = resp._get_status_( )
                reason = resp._get_reason_( )

                if (resp._get_status_( ) < 400 ):
                    appLog.debug('Got %d bytes' % resp._get_content_length_( ) )
                        
                if self.cb:
                    self.cb ('GET', self.bucketName, self.keyName, resp._get_content_length_() )

                if not fp:
                    fp = open (fileName, 'wb')


                fp.write(resp._get_data_( ))
                
                if (Config.AWSProperties['CheckHash'] == True):
                    fileHash.update ( resp._get_data_( ) )
                    
                start = fp.tell()
                end = start + Util.CHUNK_SIZE-1
                if (end > file_size):
                    end = file_size

                if ( start >= file_size):
                    # Handle the case where the file size is a multiple of Util.CHUNK_SIZE                    
                    break                                                        
                if ( resp._get_content_length_( )  !=  Util.CHUNK_SIZE):
                    break

        except Exception, f:
            logging.error ('ERROR %s' % f)
            raise
        finally:
            if fp:
                fp.flush( )
                fp.close( )
                
            if (Config.AWSProperties['CheckHash'] == True):
                local_hash = fileHash.hexdigest( )

        return (resp, local_hash)

    def _stream_data_from_file_(self, fileName ):
        doc="""
           Private method that uploads (if it exists!) to the S3 object identified by bucketName+keyName 
           to the contents in the file identified by fileName
           """
        fp = 0
        resp = 0
        hash = 0
        metadata = {}
        headers = {}

        try:
            if ( os.path.exists ( fileName ) == False):
                raise S3NoSuchFile( fileName )
 
            if (Config.AWSProperties['CheckHash'] == True):            
                fileHash = md5.new( )
                
            fp = open (fileName, 'rb')

            # Seek to the end of the file to figure out how big it is 
            fp.seek(0, os.SEEK_END)
            size = fp.tell( )
            # Reset fp
            fp.seek(0, os.SEEK_SET)

            headers['content-type'] = 'application/octet-stream'
            headers['content-length'] = size
            headers['expect'] = '100-Continue'
            headers['connection'] = 'keep-alive'

            file_info = os.stat ( fileName )
            metadata[MD_LAST_MODIFIED_TIME] =  \
                    time.strftime( Util.TIME_FORMAT, time.gmtime( file_info.st_mtime ) )
            metadata[MD_CREATE_TIME] = \
                    time.strftime( Util.TIME_FORMAT, time.gmtime( file_info.st_ctime ) )                    
            metadata[MD_SIZE] = str(file_info.st_size)

            if (self.CONN.is_secure):
                connection = httplib.HTTPSConnection("%s:%d" % (self.CONN.server, self.CONN.port))
            else:
                connection = httplib.HTTPConnection("%s:%d" % (self.CONN.server, self.CONN.port))

            self.CONN.prepare_message ( 'PUT', self.bucketName, self.keyName, {}, headers, metadata )


            S3.commLog.debug('RQST MSG:%s --details follow--\n\t%s - %s:%d\n\tHeaders:%s' % \
                             ('PUT', self.CONN.path, connection.host, connection.port, self.CONN.final_headers) )


            connection.putrequest('PUT', self.CONN.path )

            for k in self.CONN.final_headers.keys( ):
                connection.putheader( k, self.CONN.final_headers[k] )

            connection.endheaders( )

            while True:
                bytes = fp.read(Util.CHUNK_SIZE)
                if not bytes:
                    break
 
                if (Config.AWSProperties['CheckHash'] == True):
                    fileHash.update ( bytes )
                    
                length = len(bytes)

                connection.send(bytes)
                if (size == 0):
                    S3.commLog.debug ('Zero length file(%s), uploaded to %s(%s)' % \
                                      (fileName, self.bucketName, self.keyName) )                    
                    break

            if (size == 0):
                # Handle the case of a zero length file
                length = size

            if (Config.AWSProperties['CheckHash'] == True):
                hash = fileHash.hexdigest( )
                
            resp = _Response_( connection.getresponse( ) )

            S3.commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                             (resp._get_status_(), resp._get_reason_(), resp._get_msg_()  ) )

            if (resp._get_status_( ) < 400 ):
                S3.commLog.debug('Sent %d bytes' % length )
                if self.cb:
                    self.cb ('PUT', self.bucketName, self.keyName, length)

            if ( resp._get_status_( ) >= 400):
                raise S3Error(resp._get_status_( ), resp._get_reason_(), 'PUT', self.bucketName, self.keyName )

        except Exception, f:
            logging.error ('ERROR %s' % f )
            raise
        finally:
            if fp:
                fp.close( )

        return (resp, hash)

    def put_object_from_file (self, fileName):
        doc="""
           Streams a local file idenfied by fileName to the S3 object identified 
           by bucketName+keyName.  Includes retries in case of transmission errors
           """

        numTries = fp = start = end = 0

        while numTries < NUMBER_OF_RETRIES:

            try:
                resp, h = self._stream_data_from_file_( fileName )

                if (Config.AWSProperties['CheckHash'] == True):
                    if ( resp._get_etag_( ) != h ):
                        logging.error( 'Invalid hash. Local:%s, Remote:%s on uploading file %s. Retry Count=%d' % \
                                       (h, resp._get_etag_( ), fileName, numTries ) )
                        numTries += 1
                else:
                    break

            except S3NoSuchFile, e:
                logging.error ( str(e) )
                raise                
            except Exception, f:
                logging.error ('ERROR %s' % f)
                numTries += 1            

        return


    def get_object_info ( self ):
        doc="""
           Get's all available information about the S3 object identified by self.bucketName +
           self.keyName and returns a _GETResponse_ object or raises S3NotFound exception
           """

        try:
            resp = _GETResponse_ ( self.CONN.make_request ( 'HEAD', bucket=self.bucketName, key=self.keyName ) )

        except S3Error, e:
            logging.debug ( str(e) )
            raise S3NotFound(self.bucketName, self.keyName)
        except Exception, f:
            logging.debug('ERROR %s' % f)
            raise

        return ( resp )


    def get_object_to_file (self, fileName):
        doc="""
           Retrieves (if it exists!) the S3 object identified by self.bucketName +
           self.keyName to the file identified by fileName.  Includes retries in case 
           of transmission errors.  
           Raise an exception S3NotFound if the object is not found on S3
           """

        numTries = 0
        while ( numTries < NUMBER_OF_RETRIES ):        
            try:
                # Check to see if the object exists
                object_info  = self.get_object_info( )
                if ( object_info._get_content_length_( ) > 0 ):
                    resp, h = self._stream_data_to_file_( fileName, object_info._get_content_length_( ) )

                    if (Config.AWSProperties['CheckHash'] == True):
                        if ( resp._get_etag_( ) != h ):
                            logging.warning('Invalid hash. Local:%s, Remote:%s on uploading file %s. Retry Count=%d' % \
                                            (local_hash, resp._get_etag_( ), fileName, numTries ))
                            numTries += 1
                    else:
                        break
                else:
                    # Handle the case of a zero-length file
                    fp = open ( fileName, 'wb')
                    fp.flush()
                    fp.close()
                    break;
            except S3NotFound, e:
                logging.debug( str(e) )
                raise
            except Exception, f:
                logging.error ('ERROR %s' % f)
                numTries += 1

        return

    def sync_upload_from_file ( self, fileName ):
        doc="""
           Uploads file specified by fileName if it is different than what exists on S3
           Returns True if the file was actually uploaded.
           """
        fUpload = True
        try:
            object_info  = self.get_object_info( )
            file_info = os.stat( fileName)

            if ( file_info.st_size == int ( object_info._get_metadata_value_( MD_SIZE ) ) ):
                # The file sizes are the same, next check the last modified times
                last_modified = time.strftime( Util.TIME_FORMAT, time.gmtime( file_info.st_mtime ) )
                if (time.strftime( Util.TIME_FORMAT, time.gmtime( file_info.st_mtime ) ) == \
                    object_info._get_metadata_value_ (MD_LAST_MODIFIED_TIME) ):
                    # The last modified times are also identical, let's check the hash as a last resort
                    hash = Util.getHashFromFileName ( fileName )        
                    if (hash == object_info._get_etag_( ) ):
                        fUpload = False

            if (fUpload):
                self.put_object_from_file ( fileName )

        except Exception, f:
            logging.debug ('ERROR %s' % f)
            raise
        else:
            fSuccess = True
        finally:
            pass

        return ( fUpload )

    def sync_download_to_file( self, fileName ):
        doc="""
           DOWNLOADS to file specified by fileName if it is different than what exists on S3
           Returns True if the file was actually downloaded.
           """
        fDownload = True
        try:
            object_info  = self.get_object_info( )
            if (os.path.exists ( fileName) == True):
                file_info = os.stat( fileName)

                if ( file_info.st_size == int ( object_info._get_metadata_value_( MD_SIZE ) ) ):
                    # The file sizes are the same, next check the last modified times
                    last_modified = time.strftime( Util.TIME_FORMAT, time.gmtime( file_info.st_mtime ) )
                    if (time.strftime( Util.TIME_FORMAT, time.gmtime( file_info.st_mtime ) ) == \
                        object_info._get_metadata_value_ (MD_LAST_MODIFIED_TIME) ):
                        # The last modified times are also identical, let's check the hash as a last resort
                        hash = Util.getHashFromFileName ( fileName )        
                        if (hash == object_info._get_etag_( ) ):
                            fDownload = False

            else:
                fDownload = True
                
            if (fDownload):
                self.get_object_to_file ( fileName )

        except Exception, f:            
            logging.error ('ERROR %s' % f)
            raise
        else:
            fSuccess = True
        finally:
            pass

        return ( fDownload )

    def delete_object (self ):
        doc="""
           Delete the S3 object (if it exists).
           """
        try:
            resp = _Response_ ( self.CONN.make_request ( 'DELETE', self.bucketName, self.keyName ) )

        except Exception, f:
            logging.error ('ERROR %s' % f)
            raise

        return
