REST data management submission:

1. submit job but use insertJob (description can be inserted in the end
   to be picked up by client2arc) to obtain jobid
2. upload files for given jobid
3. upload job description, that has to be modified on the backend, 
   inseted in jobdescriptions table and referenced in clientjobs table
   use PATCH method

If inconsistent data files are submitted compared to job description,
how do you detect and react to this error? Is that necessary since
backend and client are developed "in house".

Use PUT method for submissions with data management for now.
If PUT request includes id parameter then the operation
of job description is going to happen. Without id parameter
only the job is created with jobname, site and id is returned
(jobdesc stays null in DB)

`curl -X PUT -F "xrsl=<arctest1.xrsl" http://localhost:8000/jobs?id=1`

help(arc.compute.SourceType)
`desclist[0].DataStaging.InputFiles[0].Sources[0].str()`
`desclist[0].DataStaging.InputFiles[0].Sources[0].plainstr()`
`desclist[0].DataStaging.InputFiles[0].Sources[0].fullstr()`

for local paths:
`desclist[0].DataStaging.InputFiles[0].Sources[0].FullPath()`
`desclist[0].DataStaging.InputFiles[0].Sources[0].ChangeFullPath()`
`desclist[0].DataStaging.InputFiles[0].Sources[0].Path()`

We shall use Sources[i].Name which is filename in xRSL, local to computing
element. The input file hierarchy on aCT will be the same as on the
computing node.

executables attribute is also important but it does not have to changed
since it works on filenames from input files.

PROBLEM WITH PROXIES: CLI programs do not necessarily have permissions to
open proxies in aCT proxies directory.

flask-socketio or rawer eventlet library for file transfer?



# proxy delegation and tokens
private key and csr are generated, entry in proxies table is created, key
is stored in PEM format in blob column of table entry. id from table
entry is used to generate JWT token. First, we can just insert blob with
private key and then when the full cert is received, fill in everything
else.


# stateless tokens article
https://yos.io/2016/01/07/stateless-authentication-with-json-web-tokens/

# cleanup of possible resources
- jobs that were never put to "tosubmit"
- proxids (how old?)

# dcache

## dev location
https://dcache.ijs.si:2880/pnfs/ijs.si/gen
https://dcache.sling.si:2880/gen.vo.sling.si
