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

class S3Error(Exception):
    
    def __init__(self, status, reason, method, bucket, key):
        self.status = status
        self.reason = reason
        self.method = method
        self.bucket = bucket
        self.key = key

    def __str__(self):
        return 'Error %d(%s). %s on bucket=%s, key=%s\n' % \
               (self.status, self.reason, self.method, self.bucket, self.key)

class SDBError(Exception):
    
    def __init__(self, status, reason, action):
        self.status = status
        self.reason = reason
        self.action = action

    def __str__(self):
        return 'Error %d(%s) on action=%s\n' % \
               (self.status, self.reason, self.action)

class InvalidAttribute(Exception):
    
    def __init__( self, a):
        self.attrib = a
            
    def __str__(self):
        return 'Attribute=%s is Invalid!\n' % (self.attrib)

class S3NotFound (Exception):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key    = key
        
    def __str__(self):
        return ( 'Object %s + %s not found\n' % (self.bucket, self.key ) )

class S3NoSuchFile(Exception):
    def __init__(self, file):
        self.fileName = file

    def __str__(self):
        return ('File %s not found\n' % (self.fileName) )
    
class Failed(Exception):
    def __init__(self):
        pass
        
    def __str__(self):
        return ('Operation failed\n' )