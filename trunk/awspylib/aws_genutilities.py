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

import os
import sys
import md5
import string


from awspylib.aws_exception import *


doc=""" 
This module contains various utility functions and classes
"""

CHUNK_SIZE = 65536
SDB_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
S3_TIME_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
DELIMITER = u'|'
indent = 0

class my_dir:
    def __init__ (self, s3key, name, depth, parent=u''):
        self.s3_key = s3key
        self.name = name
        self.depth = depth
        self.parent_name = parent
        self.children = {}
        self.__name__ = my_dir.__name__

    def add_child( self, child):

        if ( (child.__name__ != 'my_dir') and (child.__name__ != 'my_file') ):
            raise InvalidAttribute ( child)
        self.children[child.name] = child

    def get_child(self, key):

        if self.children.has_key( key ):
            return self.children[key]

        return None

    def delete_child(self, key):
        if self.children.has_key( key ):
            del self.children[key]
        

class my_file:
    def __init__ (self, s3key, name, depth, size, owner, last_modified, hash, parent= u''):
        self.s3_key = s3key
        self.name = name
        self.depth = depth
        self.size = size
        self.owner = owner
        self.last_modified = last_modified
        self.hash = hash	
        self.parent_name = parent
        self.__name__ = my_file.__name__


def get_dir_depth( dirName ):
    doc="""
       This function takes a string that represents a directory or file
       and returns the depth of the leaf node.
       For example: 'a/b/c/d' returns 4 as does 'a/b/c/d/'
       """

    fDone = False
    depth = 0

    #strip out the trailing '/', if any
    dirName = dirName.rstrip(SEPARATOR)

    while not fDone:
        i = dirName.rfind(SEPARATOR)
        if ( i <= 0):
            fDone = True
        else:
            dirName = dirName[0:i]
            depth = depth + 1
    return (depth)


def print_tree( flatTree, fPrintDetails = False ):
    doc="""
       Takes a ordered list of keys and prints it our in a directory like fashion
       Assumes that the keyList is a flattened directory
       """
    indent = 0;
    count = 0
    for entry in flatTree:
        if ( entry['type'] == 'dir' ):
            dir = entry['name']
            j = dir.rfind(SEPARATOR)
            indent = get_dir_depth ( dir[:] )               
            print '  '*indent, '[%d] %s' % (entry['index'], dir[j+1:])
        elif ( entry['type'] == 'file' ):
            file = entry['name']
            i = file.rfind( SEPARATOR)
            indent = get_dir_depth ( file[:] )
            if ( fPrintDetails):
                print '  '*indent, '[%d] %s %d %s' % (entry['index'], file[i+1:], entry['size'], entry['last_modified'])
            else:
                print '  '*indent, '[%d] %s' % (entry['index'], file[i+1:])

        count += 1

    return (count)

def print_dir_list(list, fPrintDetails=False):
    index = 0
    for entry in list:
        if (fPrintDetails):
            print '[%d] %s \t %s \t %s' % (index, entry['name'],  entry['last_modified'], entry['owner'])
        else:
            print '[%d] %s' % (index, entry['name'])        
        index = index + 1

    return ( index - 1)
    
def print_list ( list, fPrintDetails = False ):
    doc="""
       Takes list of entries and pretty prints it out with indices
       Returns the number of entries printed
       """
    index = 0
    for entry in list:
        if (fPrintDetails):
            print '[%d] %s \t %s' % (index, entry.name,  entry.creation_date)
        else:
            print '[%d] %s' % (index, entry.name)        
        index = index + 1

    return ( index - 1)

def get_string_input ( prompt ):
    while True:
        input = raw_input(prompt)
        if (input != ''):
            break
        
    return (input)


def get_digit_input( low_range = 0, high_range = 99 ):
    fDone = False
    while not fDone:
        prompt = 'Enter[' + str(low_range) + ':' + str(high_range) + ']-->'
        userInput = raw_input( prompt )
        if ( userInput.isdigit( ) == True ):
            if ( int(userInput) > high_range) or (int(userInput) < low_range ):
                print 'ERROR! Your entered:%s' % userInput
            else:
                fDone = True
        else:
            print 'ERROR! Your entered:%s' % userInput            

    return ( int(userInput) )


def get_input ( config, low_range = 0, high_range = 99):
    action = None
    value = None
    while True:
        prompt = 'Enter[%d-%d]. %s-->' % (low_range, high_range, config)
        userInput = raw_input( prompt )
        for entry in config:

            if (userInput.lower( ) == entry[0]):
                action = entry[1]
                break
        if action:
            break
        if ( userInput.isdigit( ) == True ):
            if ( low_range <= int(userInput) <= high_range ):
                value = int(userInput)
                break	    
            else:
                print 'ERROR! Your entered:%s' % userInput		
        else:
            print 'ERROR! Your entered:%s' % userInput            

    return (action, value)

def get_hash_from_filename ( fileName ):

    fp = 0
    try:
        hash = md5.new( )
        fp = open (fileName, 'rb')

        while True:
            bytes = fp.read (CHUNK_SIZE)
            if not bytes:
                break

            hash.update( bytes)

    except IOError:
        print '%s: IOError on file=%s' % (__name__, fileName )
    except Exception:
        print '%s: Unhandled exception' % __name__
    finally:
        if fp: fp.close( )
        return ( hash.hexdigest( ) )

def get_hash_from_file ( fp ):
    try:

        while True:
            bytes = fp.read(CHUNK_SIZE)
            if not bytes:
                break

            hash.update (bytes )

    except IOError:
        print '%s: IOError on file=%s' % (__name__, fileName )
    except Exception:
        print '%s: Unhandled exception' % __name__
    finally:
        return ( hash.hexdigest( ) )


def INFO(object, spacing=10, collapse=1):
    doc="""
    Print methods and doc strings.
    Takes module, class, list, dictionary, or string.
    """
    methodList = [e for e in dir(object) if callable(getattr(object, e))]
    processFunc = collapse and (lambda s: " ".join(s.split())) or (lambda s: s)
    print "\n".join(["%s %s" %
                     (method.ljust(spacing),
                      processFunc(str(getattr(object, method).__doc__)))
                     for method in methodList])
def find_file(file):
    doc="""
    Finds a file in PYTHONPATH.
    Returns the full path-name of file, if found, else returns None
    """
    for dirname in sys.path:
        possible_file_name = os.path.join(dirname, file)
        if (os.path.isfile(possible_file_name) ):
            return possible_file_name
        
    return None

def delete_directory( path ):
    doc="""
    Take a "root" path and recursiverly deletes everything under it
    Equivalent of am "rm -rf"
    """
    if (os.path.exists(path) == True):
        for root, dirs, files in os.walk ( path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name) )
                          
            for name in dirs:
                os.rmdir(os.path.join(root, name) )
            
        os.rmdir(path)
        
def make_test_file( filename, file_size = 0):
    doc="""
    Creates a test file of specified name and size whose content is random
    """
    import random

    if (file_size == 0):
        file_size = random.randint(100, 100000)
    
    len = 0
    fp = open(filename, 'wb')
    while len < file_size:
        fp.write(random.choice('0123456789abcdefghijklmnopqrstuvwxyz') )
        len += 1
    fp.flush()
    fp.close( )            
    return

def sort_dictionary ( dict ):
    doc="""
    Sorts a dictionary of key, value pairs of type string only
    """
    keys = dict.keys()
    keys.sort( cmp=lambda x,y: cmp( x.lower(), y.lower() ) )
    return [(k, dict[k]) for k in keys]