import requests
import hashlib
import xml.etree.ElementTree
import os


class NaipiS3connection(object):
    urls_and_etags = {}

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._connection_url = 'http://{0}:{1}'.format(self._host, self._port)

    def create_basic_request(self,
                             method='',
                             url='',
                             connection='',
                             payload=None,
                             headers={},
                             bucket='1',
                             directory='',
                             params={},
                             filename='',
                             print_url=False):
        '''A basic request template, used by the other functions. Wraps 'requests' module.
        Input is formated in this function.'''
        if directory != '' and directory[0] != '/':
            directory = '/' + directory
        if filename != '':
            filename = '/' + filename
        if url == '':
            url = '{0}/{1}{2}{3}'.format(connection, bucket, directory, filename)
        if print_url:
            print (url)
        # if PUT, store the url & the MD5 in a dict:
        if method == 'PUT' and headers == {} and payload != None:
            self.store_etag(url, payload)
        response = requests.request(method, url, data=payload, headers=headers, params=params)
        response.raise_for_status()
        return response

    def put_object(self,
                   url='',
                   payload='',
                   bucket='1',
                   directory='',
                   filename='', print_url=False):
        '''Puts a simple object, with 'payload' as its content.'''
        return self.create_basic_request(method='PUT',
                                         url=url, connection=self._connection_url, payload=payload,
                                         bucket=bucket, directory=directory, filename=filename, print_url=print_url)

    def put_append_object(self,
                          payload,
                          url='',
                          bucket='1',
                          directory='',
                          filename='',
                          print_url=False):
        '''Put-append a simple object, with 'payload' as its content.'''
        return self.create_basic_request(method='PUT',
                                         url=url, connection=self._connection_url, payload=payload,
                                         headers={'range': '-1'},
                                         bucket=bucket, directory=directory, filename=filename, print_url=print_url)

    def create_bucket(self, bucketname='1', print_url=False):
        '''Creates a bucket. If the bucket exists, does nothing'''
        return self.create_basic_request(method='PUT', connection=self._connection_url, bucket=bucketname,
                                         print_url=print_url)

    def create_directory(self,
                         url='',
                         bucket='1',
                         directory='',
                         print_url=False):
        '''Not supported yet, due to a known open bug'''
        return self.create_basic_request(method='PUT', connection=self._connection_url, url=url, bucket=bucket,
                                         directory=directory,
                                         print_url=print_url)

    def put_file(self, fd, url='',
                 bucket='1',
                 directory='',
                 filename='', print_url=False):
        temp = fd
        filedata = fd.read()
        fd = temp
        return self.create_basic_request(method='PUT',
                                         url=url, connection=self._connection_url, payload=filedata,
                                         bucket=bucket, directory=directory, filename=filename, print_url=print_url)

    def store_etag(self, url, data=''):
        '''Stores the md5/Etag of an uploaded object. The Etag is saved in a local dictionary.'''
        etag = hashlib.md5(data).hexdigest()
        NaipiS3connection.urls_and_etags[url] = etag

    def GET_object(self,
                   url='',
                   bucket='1',
                   directory='',
                   filename='', print_url=False):
        '''Gets the contents of an object, then calculate the MD5.
        Then compares the old (saved) MD5 with the new one.
        Returns a tuple: (response,Etag_before,Etag_after)'''
        response = self.create_basic_request(method='GET',
                                             url=url, connection=self._connection_url, payload='', headers={},
                                             bucket=bucket, directory=directory, filename=filename, print_url=print_url)
        response.raise_for_status()
        if url == '':
            url = '{0}/{1}{2}/{3}'.format(self._connection_url, bucket, directory, filename)
        Etag_after = hashlib.md5(response.content).hexdigest()
        # might fail - key not found - should use try/except
        Etag_before = NaipiS3connection.urls_and_etags[url]
        return response, Etag_before, Etag_after

    def get_all_buckets(self):
        '''returns a list of bucket names.'''
        tree = xml.etree.ElementTree
        bucketlist = []
        response = self.create_basic_request(method='GET', connection=self._connection_url, bucket='')
        # load response XML data into an Element Tree structure:
        xmldata = tree.fromstring(response.content)
        # extract all buckets
        buckets = xmldata.getchildren()[0]
        for bucket in buckets:  # for each bucket, get its name:
            bucketlist.append(bucket[0].text)
        return bucketlist

    def parse_response_to_contentlist(self, response):
        '''Parse a response XML to a contentlist object.'''
        # hard coded: remove 'xmlns='http://s3.iguaz.io/doc/2015-04-01/''
        tree = xml.etree.ElementTree
        response_data = response.content.replace('xmlns="http://s3.iguaz.io/doc/2015-04-01/"', '')
        dom = tree.fromstring(response_data)
        dirlist = []
        keylist = []
        # get bucket name:
        bucketname = dom.find('Name').text
        # get directory:
        if dom.find('Prefix').text == None:
            current_dir = ''
        else:
            current_dir = dom.find('Prefix').text
        # get 'IsTruncated':
        if dom.find('IsTruncated').text == 'false':
            IsTruncated = False
        else:
            IsTruncated = True
        # get a list of all directories:
        for directory in dom.findall('CommonPrefixes/Prefix'):
            dirlist.append(directory.text)
        # get a list of all keys:
        for entry in dom.findall('Contents'):
            key_tmp = key(entry[0].text, entry[1].text, entry[2].text, self._connection_url, bucketname, current_dir)
            keylist.append(key_tmp)

        return contentlist(bucketname, self._connection_url, current_dir, IsTruncated, dirlist, keylist)

    def get_bucket_contents(self, bucket):
        '''Returns the contents of a bucket.'''
        response = self.create_basic_request(method='GET', connection=self._connection_url, bucket=bucket)
        return self.parse_response_to_contentlist(response)

    def read_dir(self, bucket, directory='', marker='', maxkeys='', print_url=False):
        '''Returns the contents of a directory. Supports 'marker' and 'max-keys'. '''
        params = {}
        if directory != '':
            if directory[0] != '/':
                directory = '/' + directory
            params['prefix'] = directory
        if marker != '':
            params['marker'] = marker
        if maxkeys != '':
            params['max-keys'] = maxkeys
        response = self.create_basic_request(method='GET', connection=self._connection_url, bucket=bucket,
                                             params=params,
                                             print_url=print_url)
        return self.parse_response_to_contentlist(response)

    def delete_object(self,
                      url='',
                      bucket='1',
                      directory='',
                      filename='', print_url=False):
        '''Deletes an object.'''
        return self.create_basic_request(method='DELETE',
                                         url=url, connection=self._connection_url, payload='',
                                         bucket=bucket, directory=directory, filename=filename, print_url=print_url)


class contentlist():
    _bucketname = ''
    _connection = ''
    _directory = ''
    _keycount = ''
    _dircount = ''
    _is_truncated = False
    _dirlist = []
    _keylist = []

    def __init__(self, bucketname, connection, directory, is_truncated, dirlist, keylist):
        self._bucketname = bucketname
        self._connection = connection
        self._is_truncated = is_truncated
        self._directory = directory
        self._dirlist = dirlist
        self._keylist = keylist
        self._keycount = len(keylist)
        self._dircount = len(dirlist)

    def __repr__(self):
        return '<data of {}/{}{} : keys-{} dirs-{}>'.format(self._connection,
                                                            self._bucketname,
                                                            self._directory,
                                                            self._keycount,
                                                            self._dircount)

    @property
    def bucketname(self):
        return self._bucketname

    @property
    def connection(self):
        return self._connection

    @property
    def is_truncated(self):
        return self._is_truncated

    @property
    def directory(self):
        return self._directory

    @property
    def dirlist(self):
        return self._dirlist

    @property
    def keylist(self):
        return self._keylist

    @property
    def keycount(self):
        return self._keycount

    @property
    def dircount(self):
        return self._dircount


class key():
    keyname = ''
    size = ''
    LastModified = ''
    url = ''

    def __init__(self, keyname, size, LastModified, connection, bucket, directory):
        self.keyname = keyname
        self.size = size
        self.LastModified = LastModified
        self.url = connection + '/' + bucket + directory + '/' + self.keyname

    def __repr__(self):
        return '<key: {}>'.format(self.keyname)
