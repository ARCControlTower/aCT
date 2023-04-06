# aCT deployment

aCT has several python dependencies. Depending on the type of the system
(development, testing, production) the aCT deployment might require different
versions of aCT and its dependencies. Also, the dependencies like *pyarcrest*
are only available from git repository and not PyPI. Therefore, certain
method is required to specify which versions of the packages should be
installed.

A possible approach is described here.


## Production systems

Production systems can be managed in two ways:

- The latest stable versions of aCT and its dependencies are used. It makes
  sense for the stable development of aCT to follow the stable development of
  its dependencies so this can be the default way.

- aCT and all its dependencies are locked to a particular well-tested version.
  This is also often used for production setups and allows better
  reproducibility.

For git repository dependencies (like *pyarcrest*) it is reasonable to expect
that the stable versions are released in a certain branch (e. g. master) so
specifying the branch is enough. In case of locking down the versions the git
tags or commit hashes should be used.


## Testing and development systems

Testing and development systems should specify the branches where the
respective versions are (e. g. dev branch).


## pip-tools

*pip-compile* is a program from *pip-tools* package that can take the python
package project file (setup.py, setup.cfg, pyproject.toml) or a text file with
multiple packages specified (conventionally requirements.in, in pip-compatible
format) and output the requirements.txt file with all dependencies recursively
locked to a particular version. Additional version constraints can be used in
the process to affect the dependency version resolution.


## Possible admin workflow

Admin should write a requirements.in file with all packages required to run
aCT service. For production services where the latest stable versions can be
used, the requirements.in file can specify just the aCT package:

```
aCT @ git+https://github.com/ARCControlTower/aCT.git

# additionally, if serving REST interface with gunicorn
gunicorn
```

A testing or development setup that is used to test the developed features:
```
aCT @ git+https://github.com/ARCControlTower/aCT.git@external-arc

pyarcrest @ git+https://github.com/jakobmerljak/pyarcrest.git@dev
```

This specifies the *external-arc* development branch for aCT package and *dev*
branch for *pyarcrest* dependency where the required features are developed in
lockstep. Notice that pyarcrest is defined specifically to override the default
dependency specification from setup.cfg. The alternative would be to specify the
different dependency version in the setup.cfg. The problem with such alternative
is that this has to be handled properly in git before merges. Also, the admin
might want the liberty of overriding the dependency versions as is possible with
explicit entries in requirements.in.

The locked production setups can use git hashes or tags instead of the branch
names.

Once the requirements.in is specified, the *pip-compile* can be run:
`$ pip-compile requirements.in`

This will generate a requirements.txt file. The packages can then be installed
with:
`$ pip install --force-reinstall --no-deps -r requirements.txt`

When requirements.in change or newer versions of dependencies come out,
*pip-compile* should be rerun.

### Why not *pip-sync* from *pip-tools* and why the extra flags for pip?

Generally, *pip-sync* is supposed to be used with *pip-compile*. But this
introduces some surprising issues.

There is a conflict between the dependency specified in aCT setup.cfg (master
branch of pyarcrest repo) and the explicitly overriden pyarcrest package (dev
branch of pyarcrest repo) even though the package specifications of both
branches specify the same version (0.1 at the time of writing). So, there is a
potential conflict between specified versions of package as a dependency and the
explicitly specified version of the package.

E. g.
`pyarcrest @ git+https://github.com/jakobmerljak/pyarcrest.git`

from aCT's setup.cfg is conflicting with

`pyarcrest @ git+https://github.com/jakobmerljak/pyarcrest.git@dev`

from requirements.txt.

This is surprising as one could think that *pip-sync* would just install the
packages from the already compiled specification and not be complaining about
potential dependency mismatches.

Because of the mentioned problems, the regular pip is used to install the
packages from the specification. This has to be done with two additional flags.

`--no-deps` flag to avoid resolving dependencies as that causes the above
conflict. Also, *pip-compile* already resolved and locked the dependencies so
pip just needs to install the specified packages.

`--force-reinstall` flag is used because pip will not upgrade the packages from
git if they are already installed (unless their version changed, not sure; there
are many issues and discussions around this functionality). This flag is the
simplest method to ensure that all packages get properly reinstalled. The
downside is the overhead of reinstallation but at least the packages are taken
from cache and are not refetched.
