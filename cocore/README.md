# Datacoco3 - Core

Equinox Common Code Utility for Python 3 with minimal dependencies and easy installation!

Includes utilities for logging, config files, and batchy

## Installing the latest version
```
pip install -e git+https://bitbucket.org/equinoxfitness/datacoco3.core.git#egg=cocore
```

## Configuration:  Password Encryption

The coco config class uses base64 encryption.  Any option named pwd or password will be assumed base 64 encrypted.   To derive the encrypted password for your config, launch python shell and run the following command:

```
>>> import base64
>>> print base64.b64encode("password")
cGFzc3dvcmQ=
```

In python3 if you get the error `TypeError: a bytes-like object is required, not 'str'` do this.

```
>>> import base64
>>> print(base64.b64encode(b'password'))
b'cGFzc3dvcmQ=
```

## Development

#### Getting Started

It is recommended to use the steps below to set up a virtual environment for development:

```
python3 -m venv <virtual env name>
source venv/bin/activate
pip install -r requirements.txt
```
#### Testing
To run the testing suite, simply run the command: `tox`




