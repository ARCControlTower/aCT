PROBLEM WITH PROXIES: CLI programs do not necessarily have permissions to
open proxies in aCT proxies directory.


# stateless tokens article
https://yos.io/2016/01/07/stateless-authentication-with-json-web-tokens/

# cleanup of possible resources
- jobs that were never put to "tosubmit"
- proxids (how old?)

# dcache

## dev location
https://dcache.ijs.si:2880/pnfs/ijs.si/gen
https://dcache.sling.si:2880/gen.vo.sling.si

# aiohttp
`ssl.PROTOCOL_TLS` works but has deprecated warning
`ssl.PROTOCOL_TLS_CLIENT` fails with an error

# webdav
for reference:
https://github.com/synodriver/aiowebdav

# VOMS
voms extensions can expire faster than cert



# produkcijska postavitev
- filtriranje certifikatov
- vzpostavitev act clienta





# VOMS protocol wiki
https://wiki.italiangrid.it/twiki/bin/view/VOMS/VOMSProtocol







# asyncio!!!!
What happens when coroutine is cancelled when aiohttp reuquest is in progress???

KeyboardInterrupt crash if ctrl-c is pressed too soon after program start,
while program is loading entry point?

Does asyncio.ensure_future go through event loop such that it has to be
tryied for CancelledError?

Does coroutine cancel impact async with session and async with response or
does it kick in when awaiting response?

## Python 3.6 asyncio keyboard interrupts
https://gist.github.com/harrisont/38ecc65aaad3481c9221417d7c64fef8

## cancel
https://stackoverflow.com/questions/56052748/python-asyncio-task-cancellation

## shield
https://duckduckgo.com/?t=ffab&q=aiohttp+shield&atb=v121-1&ia=web







# http optimization
https://julien.danjou.info/python-and-fast-http-clients/

# httpx async multipart not implemented
https://github.com/encode/httpx/issues/1015
https://github.com/encode/httpx/pull/1016


# trio keyboard interrupts
- main runs nursery with program task and sighup watcher (through
  trio signal system); watcher raises keyboardinterrupt exception
  which makes nursery cancel all tasks and respect shielding?



# thoughts on error handling
Some thoughts on the following example code:
```
@app.route('/jobs', methods=['POST'])
def create_jobs():
    errpref = 'error: POST /jobs: '
    try:
        token = getToken()
        jobs = request.get_json()
        jmgr = JobManager()
    except RESTError as e:
        print('{}: {}'.format(errpref, e))
        return {'msg': str(e)}, e.httpCode
    except BadRequest as e:  # raised for invalid JSON
        print('{}: {}'.format(errpref, e))
        return {'msg': str(e)}, 400
    except Exception as e:
        print('{}: {}'.format(errpref, e))
        return {'msg': 'Server error'}, 500

    results = []
    for job in jobs:
        result = {}

        # check job description
        if 'desc' not in job:
            print('{}: No job description given'.format(errpref))
            result['msg'] = 'No job description given'
            results.append(result)
            continue
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(job['desc'], jobdescs):
            print('{}: Invalid job description'.format(errpref))
            result['msg'] = 'Invalid job description'
            results.append(result)
            continue
        result['name'] = jobdescs[0].Identification.JobName

        if 'site' not in job:
            print('{}: No site given for'.format(errpref))
            result['msg'] = 'No site given'
            results.append(result)
            continue

        try:
            checkSite(job['site'])

            # insert job and create its data directory
            jobid = jmgr.clidb.insertJob(job['desc'], token['proxyid'], job['site'])
            jobDataDir = jmgr.getJobDataDir(jobid)
            os.makedirs(jobDataDir)
        except NoSuchSiteError:
            print('{}: Invalid site'.format(errpref))
            result['msg'] = 'Invalid site'
            results.append(result)
            continue
        except Exception as e:
            print('{}: {}'.format(errpref, e))
            result['msg'] = 'Server error'
            results.append(result)
            continue

        result['id'] = jobid
        results.append(result)

    return jsonify(results)
```

- usecase: A bunch of steps that can could (should?) exit earlier.
  This adds duplication.
  Notice repetition of append statements. Those statements could be reduced code
  was branched properly, but this would lead to a lot of indentaton.

  There is a pattern. We start from core error message. This can then be modified
  for logging system on backend response for frontent ("Server error", when we
  don't want to expose information about internal errors and state.
  + there could be a function that logs and generates flask response tuple, with
    optional kwargs to override behavior where necessary
  
  + similar could be done for JSON response objects per job. Function would log
    the message and generate or modify JSON object to be appended for current
    job iteration.
  
  + how much common code is for upper two points? Are there more generic
    patterns?
  
  (lisp macros, common lisp condition system and more low level continuation
  system come to mind to be possible to solve those problems elegantly, or
  FP monadic contexts?)

- also, look at errpref pattern to avoid duplication of common log message
  parts. Could/should this be implemented better by hooking into code to modify
  msg or perform arbitrary manipulation on input data?
