Contributing Guidelines
=======================

When contributing to this repository, please first discuss the change
you wish to make via issue or email with the owners of this repository
before making a change.

Issues
------

First off, issues may arise when not ran inside a virtual environment.
Therefore, make sure to follow the installation process before
proceeding. Issues can be created
`here <https://github.com/equinoxfitness/maximilian/issues/new>`__ and
please put the appropriate label.

Local development setup
-----------------------

maximilian requires Python 3.6+

::

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements-dev.txt

Please install `pre-commit <https://pre-commit.com>`__ git hooks to use
`Black <https://black.readthedocs.io/en/stable/>`__ autoformatting and
flake8 PEP8 validations by running:

::

    pre-commit install

Tests
-----

Run `tox <https://tox.readthedocs.io/en/latest/>`__ to validate tests
are working

::

    tox

To run tox for multiple Python 3 versions, you can use
`pyenv <https://github.com/pyenv/pyenv>`__ to install and manage
different Python versions locally.

If you see an error indicating a missing Python version, ie:
``SKIPPED: InterpreterNotFound: python3.7``

-  See if you have the version specified: ``pyenv versions``
-  If not, install it: ``pyenv install 3.7.0``
-  Make available to your local directory: ``pyenv local 3.7.0``
-  Run ``tox`` again

Pull Request Process
--------------------

1. Ensure any install or build dependencies are removed before the end
   of the layer when doing a build.

2. Follow the pull request template provided.


