import requests
import hashlib
import xml.etree.ElementTree as ET
import os


##Notes
# should change host to ctx.clients[0].address
# check URL validity
# Catch "ConnectionError"


class NaipiS3connection():
    etags = {}

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.conn = "http://{0}:{1}".format(self.host, self.port)

    def create_basic_request(self, method="",
                             url="",
                             connection="",
                             payload=None,
                             headers={},
                             bucket="1",
                             directory="",
                             params={},
                             filename="",
                             printURL=False):
        """A basic request template, used by the other functions. Wraps "requests" module.
        Input is formated in this function."""
        if directory != "" and directory[0] != "/":
            directory = "/" + directory
        if filename != '':
            filename = "/" + filename
        if url == "":
            url = "{0}/{1}{2}{3}".format(connection, bucket, directory, filename)
        if printURL:
            print (url)
        # if PUT, store the url & the MD5 in a dict:
        if method == "PUT" and headers == {}:
            self.storeEtag(url, payload)
        response = requests.request(method, url, data=payload, headers=headers, params=params)
        response.raise_for_status()
        return response

    def PUT_object(self, url="",
                   payload="",
                   bucket="1",
                   directory="",
                   filename="", printURL=False):
        """Puts a simple object, with "payload" as its content."""
        return self.create_basic_request(method="PUT",
                                         url=url, connection=self.conn, payload=payload,
                                         bucket=bucket, directory=directory, filename=filename, printURL=printURL)

    def PUT_Append_object(self, payload,
                          url="",
                          bucket="1",
                          directory="",
                          filename="",
                          printURL=False):
        """Put-append a simple object, with "payload" as its content."""
        return self.create_basic_request(method="PUT",
                                         url=url, connection=self.conn, payload=payload, headers={'range': "-1"},
                                         bucket=bucket, directory=directory, filename=filename, printURL=printURL)

    def create_bucket(self, bucketname="1", printURL=False):
        """Creates a bucket. If the bucket exists, does nothing"""
        return self.create_basic_request(method="PUT", connection=self.conn, bucket=bucketname, printURL=printURL)

    def create_directory(self, url="",
                         bucket="1",
                         directory="",
                         printURL=False):
        """Not supported yet, due to a known open bug"""
        return self.create_basic_request(method="PUT", connection=self.conn, url=url, bucket=bucket,
                                         directory=directory,
                                         printURL=printURL)

    def PUT_file(self, fd, url="",
                 bucket="1",
                 directory="",
                 filename="", printURL=False):
        temp = fd
        filedata = fd.read()
        fd = temp
        return self.create_basic_request(method="PUT",
                                         url=url, connection=self.conn, payload=filedata,
                                         bucket=bucket, directory=directory, filename=filename, printURL=printURL)

    def storeEtag(self, url, data=""):
        """Stores the md5/Etag of an uploaded object. The Etag is saved in a local dictionary."""
        etag = hashlib.md5(data).hexdigest()
        self.etags[url] = etag

    def GET_object(self, url="",
                   bucket="1",
                   directory="",
                   filename="", printURL=False):
        """Gets the contents of an object, then calculate the MD5.
        Then compares the old (saved) MD5 with the new one.
        Returns a tuple: (response,Etag_before,Etag_after)"""
        response = self.create_basic_request(method="GET",
                                             url=url, connection=self.conn, payload="", headers={},
                                             bucket=bucket, directory=directory, filename=filename, printURL=printURL)
        response.raise_for_status()
        if url == "":
            url = "{0}/{1}{2}/{3}".format(self.conn, bucket, directory, filename)
        Etag_after = hashlib.md5(response.content).hexdigest()
        # might fail - key not found - should use try/except
        Etag_before = self.etags[url]
        return response, Etag_before, Etag_after

    def get_all_buckets(self):
        """returns a list of bucket names."""
        bucketlist = []
        response = self.create_basic_request(method="GET", connection=self.conn, bucket="")
        # load response XML data into an Element Tree structure:
        xmldata = ET.fromstring(response.content)
        # extract all buckets
        buckets = xmldata.getchildren()[0]
        for bucket in buckets:  # for each bucket, get its name:
            bucketlist.append(bucket[0].text)
        return bucketlist

    def parse_response_to_contentlist(self, response):
        """Parse a response XML to a contentlist object."""
        # hard coded: remove 'xmlns="http://s3.iguaz.io/doc/2015-04-01/"'
        response_data = response.content.replace('xmlns="http://s3.iguaz.io/doc/2015-04-01/"', '')
        dom = ET.fromstring(response_data)
        dirlist = []
        keylist = []
        # get bucket name:
        bucketname = dom.find("Name").text
        # get directory:
        if dom.find("Prefix").text == None:
            current_dir = ""
        else:
            current_dir = dom.find("Prefix").text
        # get 'IsTruncated':
        if dom.find("IsTruncated").text == 'false':
            IsTruncated = False
        else:
            IsTruncated = True
        # get a list of all directories:
        for directory in dom.findall('CommonPrefixes/Prefix'):
            dirlist.append(directory.text)
        # get a list of all keys:
        for entry in dom.findall('Contents'):
            key_tmp = key(entry[0].text, entry[1].text, entry[2].text, self.conn, bucketname, current_dir)
            keylist.append(key_tmp)

        return contentlist(bucketname, self.conn, current_dir, IsTruncated, dirlist, keylist)

    def get_bucket_contents(self, bucket):
        """Returns the contents of a bucket."""
        response = self.create_basic_request(method="GET", connection=self.conn, bucket=bucket)
        return self.parse_response_to_contentlist(response)

    def read_dir(self, bucket, directory="", marker="", maxkeys="", printURL=False):
        """Returns the contents of a directory. Supports "marker" and "max-keys". """
        params = {}
        if directory != "":
            if directory[0] != "/":
                directory = "/"+directory
            params["prefix"] = directory
        if marker != "":
            params["marker"] = marker
        if maxkeys != "":
            params["max-keys"] = maxkeys
        response = self.create_basic_request(method="GET", connection=self.conn, bucket=bucket, params=params,
                                             printURL=printURL)
        return self.parse_response_to_contentlist(response)

    def DELETE_object(self, url="",
                      bucket="1",
                      directory="",
                      filename="", printURL=False):
        """Deletes an object."""
        return self.create_basic_request(method="DELETE",
                                         url=url, connection=self.conn, payload="",
                                         bucket=bucket, directory=directory, filename=filename, printURL=printURL)


class contentlist():
    bucketname = ""
    connection = ""
    directory = ""
    keycount = ""
    dircount = ""
    IsTruncated = False
    dirlist = []
    keylist = []

    def __init__(self, bucketname, connection, directory, IsTruncated, dirlist, keylist):
        self.bucketname = bucketname
        self.connection = connection
        self.IsTruncated = IsTruncated
        self.directory = directory
        self.dirlist = dirlist
        self.keylist = keylist
        self.keycount = len(keylist)
        self.dircount = len(dirlist)

    def __repr__(self):
        return "<data of {}/{}{} : keys-{} dirs-{}>".format(self.connection, self.bucketname, self.directory,
                                                            self.keycount,
                                                            self.dircount)


class key():
    keyname = ""
    size = ""
    LastModified = ""
    url = ""

    def __init__(self, keyname, size, LastModified, connection, bucket, directory):
        self.keyname = keyname
        self.size = size
        self.LastModified = LastModified
        self.url = connection + '/' + bucket + directory + '/' + self.keyname

    def __repr__(self):
        return "<key: {}>".format(self.keyname)
