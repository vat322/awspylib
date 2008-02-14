Copyright (c) 2008, Kiran Bhageshpur <kiran_bhageshpur@hotmail.com>

http://code.google.com/p/awspylib

2/12/2008
awspylib is a python based library to interact with amazon web services.  Currently, it only supports amazon's Simple 
Storage Service (S3). 

awspylib has been developed and tested ih python2.5.1 on both Windows XP and Ubuntu 7.1 (Gusty Gibbons).  It has also
been tested with am EC3 ubuntu7.1 image.

The main use case focus for the development of awspylib has been more "back end" focused.  The current implementation of
the interface to S3 is really focused on allowing the backup and synchronization of a local file system to S3.

The goal is to next integrate awspylib with PyNotify to create a low cost, lightweight way to back up changes to local 
file systems on a EC2 image to S3 and to automatically restore these changes on the restart of an EC2 instance

Included with the library is a CLI based tool (s3_cli.py)

User documentation can be found at awspylib/docs/






