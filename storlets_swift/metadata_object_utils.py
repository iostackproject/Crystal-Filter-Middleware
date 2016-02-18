from swift.common.exceptions import DiskFileNotExist
from swift.common.exceptions import DiskFileXattrNotSupported
from swift.common.exceptions import DiskFileNoSpace
from swift.obj.diskfile import _get_filename
from swift.common.swob import Request
import xattr
import logging
import pickle
import errno
import os
import time
import subprocess

PICKLE_PROTOCOL = 2
METADATA_KEY = 'user.swift.iostack'

def read_metadata(fd, md_key = None):
    """
    Helper function to read the pickled metadata from an object file.
    :param fd: file descriptor or filename to load the metadata from
    :returns: dictionary of metadata
    """
    if md_key:
        meta_key = md_key
    else:
        meta_key = METADATA_KEY

    metadata = ''
    key = 0
    try:
        while True:
            metadata += xattr.getxattr(fd, '%s%s' % (meta_key,
                                                     (key or '')))
            key += 1
    except (IOError, OSError) as e:
        if metadata =='':
            return False
        for err in 'ENOTSUP', 'EOPNOTSUPP':
            if hasattr(errno, err) and e.errno == getattr(errno, err):
                msg = "Filesystem at %s does not support xattr" % \
                      _get_filename(fd)
                logging.exception(msg)
                raise DiskFileXattrNotSupported(e)
        if e.errno == errno.ENOENT:
            raise DiskFileNotExist()
    return pickle.loads(metadata)


def write_metadata(fd, metadata, xattr_size=65536, md_key = None):
    """
    Helper function to write pickled metadata for an object file.
    :param fd: file descriptor or filename to write the metadata
    :param metadata: metadata to write
    """

    if md_key:
        meta_key = md_key
    else:
        meta_key = METADATA_KEY

    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        try:
            xattr.setxattr(fd, '%s%s' % (meta_key, key or ''),
                           metastr[:xattr_size])
            metastr = metastr[xattr_size:]
            key += 1
        except IOError as e:
            for err in 'ENOTSUP', 'EOPNOTSUPP':
                if hasattr(errno, err) and e.errno == getattr(errno, err):
                    msg = "Filesystem at %s does not support xattr" % \
                          _get_filename(fd)
                    logging.exception(msg)
                    raise DiskFileXattrNotSupported(e)
            if e.errno in (errno.ENOSPC, errno.EDQUOT):
                msg = "No space left on device for %s" % _get_filename(fd)
                logging.exception(msg)
                raise DiskFileNoSpace()
            raise


def put_metadata(orig_resp, storlets_name_list):
    fd = orig_resp.app_iter._fp
    try:
        object_metadata = read_metadata(fd)

        if not object_metadata:
            object_metadata = storlets_name_list
        else:
            object_metadata = object_metadata + storlets_name_list

        write_metadata(fd, object_metadata)
    except:
        return False
    return True

def get_metadata(orig_resp):
    fd = orig_resp.app_iter._fp
    try:
        controller_md = read_metadata(fd)
    except:
        return None
    return controller_md
