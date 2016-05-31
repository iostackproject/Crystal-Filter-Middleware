'''===========================================================================
19-Oct-2015    josep.sampe    Initial implementation.
==========================================================================='''
from swift.common.exceptions import DiskFileNotExist
from swift.common.exceptions import DiskFileXattrNotSupported
from swift.common.exceptions import DiskFileNoSpace
from swift.obj.diskfile import _get_filename
import operator
import logging
import pickle
import errno
import xattr

PICKLE_PROTOCOL = 2
METADATA_KEY = 'user.swift.iostack'

mappings = {'>': operator.gt, '>=': operator.ge,
            '==': operator.eq, '<=': operator.le, '<': operator.lt,
            '!=': operator.ne, "OR": operator.or_, "AND": operator.and_}


def read_metadata(fd, md_key=None):
    """
    Helper function to read the pickled metadata from an object file.
    :param fd: file descriptor or filename to load the metadata from
    :param md_key: metadata key to be read from object file
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
        if metadata == '':
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


def write_metadata(fd, metadata, xattr_size=65536, md_key=None):
    """
    Helper function to write pickled metadata for an object file.
    :param fd: file descriptor or filename to write the metadata
    :param md_key: metadata key to be write to object file
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


def put_metadata(req, iostack_md, app):

    get_req = req.copy_get()
    get_resp = get_req.get_response(app)

    fd = get_resp.app_iter._fp
    file_path = get_resp.app_iter._data_file.rsplit('/', 1)[0]

    for key in iostack_md["storlet-exec-list"]:
        current_params = iostack_md["storlet-exec-list"][key]['params']
        if current_params:
            iostack_md["storlet-exec-list"][key]['params'] = current_params+\
                                                             ','+'reverse=True'
        else:
            iostack_md["storlet-exec-list"][key]['params'] = 'reverse=True'
        
        iostack_md["storlet-exec-list"][key]['execution_server'] = \
            iostack_md["storlet-exec-list"][key]['execution_server_reverse']
        iostack_md["storlet-exec-list"][key].pop('execution_server_reverse')

    print (iostack_md)
    print (file_path)
    
    try:
        write_metadata(fd, iostack_md)
    except:
        return False
    return True

def get_metadata(orig_resp):
    controller_md = {} 
    try:
        fd = orig_resp.app_iter._fp
        controller_md = read_metadata(fd)
    except AttributeError as e:
        print("Failed: Attempting to do a range request (non-supported): " + str(e))
    if not controller_md:
        return {}
    return controller_md
