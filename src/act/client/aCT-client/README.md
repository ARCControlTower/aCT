# Install
Create virtual environment for aCT client or use existing one:
`$ python -m venv /path/to/venv`

Activate the environment:
`$ . /path/to/venv/bin/activate`

Navigate to `src/act/client/aCT-client` directory in aCT repo and run:
`$ python setup.py install`

# Configure
Create configuration file in your home directory `$HOME/.config/act-client/config.yaml`

Parameters with default values and optional parameters can be omitted. Possible parameters are:
- `server`: URL of aCT server
- `port`: port which aCT server is listening on
- `token`: location where client should store auth token
  (optional, default: `$HOME/.local/share/act-client/token`)
- `proxy`: location of proxy that client uses for authentication
  (optional, default: `/tmp/x509up_u$UID` - default location used by ARC client)
- `dcache`: path to dcache folder accessible with your proxy certificate credentials
  (optional, required for use with `--dcache` flag when using `actsub`)

Example of configuration:
``` yaml
server: https://act.vega.izum.si
port: 443
```

# Authentication
User needs to have a valid x509 proxy for authentication.
ARC client can be used to create one:
`arcproxy -S voms.server`

Then, authenticate on aCT:
`$ actproxy`

This will delegate proxy certificate on aCT server and store access
token in configured location.

# aCT client commands
After user is authenticated, the following job management commands can be used:
- `actclean`: clean finished or failed jobs
- `actfetch`: aCT server fetches failed jobs from ARC to make them available
  for download using `actget`
- `actget`: download successfully finished or fetched (`actfetch`) failed jobs
  to current directory
- `actkill`: kill jobs
- `actproxy`: authenticate or re-authenticate to be able to run other commands
- `actresub`: resubmit failed jobs
- `actstat`: print status of jobs
- `actsub`: submit given jobs

More specific information for commands can be found by using `--help` on commands.
