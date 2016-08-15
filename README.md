# Pure-Storage-Enhanced-Snapshot-Facility
<b>Enhances the Pure Storage Snapshot Capabilities</b><br>

<p>The Pure Storage Array Enhanced Snapshot Facility is a customized suite of three (3) basic custom Python based tools enhancing the Protection capability that ships with the Pure Storage array. The facility exploits the REST interface and utilizes the Python API provided by Pure Storage and is currently only tested to run on Unix/Linux platforms (but should work similarly on Windows). Therefore, Python, and the Pure Storage Python API is a prerequisite.

<pre>
Prerequisite
Requires Python and installation of the Pure Storage Python Automation toolkit:
http://pythonhosted.org//purestorage/
To install the rest-client above you need pip:
  if pip is not installed:
    https://bootstrap.pypa.io/get-pip.py; python get-pip.py
  if requests is not installed:
    pip install requests
  then
    pip install purestorage

Since I work on an AIX host behind a corporate firewall, I found getting the pip installation problematic so
my workaround was to install git from github.com on my windows box, clone and upload to the AIX host. These
three packages were required:
git clone https://github.com/kennethreitz/requests
git clone https://github.com/jaraco/setuptools
git clone https://github.com/purestorage/rest-client.git

Then I cd'd into each directory (in the same order as above) and as root ran:
python setup.py install

Another option that should work (and doesn't require root) is to copy the purestorage directory (under rest-client
which contains two files, __init__.py and purestorage.py) ...
and the requests directory (under requests) to a subdirectory under the common installation directory for purerest.py
----------------------------------------------------------------
Installation
1) Copy snapsched.py, snapcopy.py, and snaplist.py to any empty directory, e.g., /usr/local/purestorage
2) Change directory to the destination, e.g., cd /usr/local/purestoage
3) Run:
  ln -s snapshed.py snapsched
  ln -s snapcopy.py snapcopy
  ln -s snaplist.py snaplist
4) Create a pure.ini file in /etc. Reccomend permissions
   of 600 since it contains the api tokens of your arrays.
5) Create a snapsched.ini file in /etc
----------------------------------------------------------------
If you find that you get warning like:
InsecurePlatformWarning: A true SSLContext object is not available. This prevents urllib3 from configuring
SSL appropriately and may cause certain SSL connections to fail. For more information, see
https://urllib3.readthedocs.org/en/latest/security.html#insecureplatformwarning.
then you likely are using an older version of python or requests. If you cannot resolve the problem
and want to suppress the warnings, uncomment line 34 in purerest.py:
#requests.packages.urllib3.disable_warnings()
</pre>
