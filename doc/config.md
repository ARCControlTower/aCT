# Usage of configuration objects from aCTConfig module
To get config from aCTConfigARC.yaml or aCTConfigAPP.yaml, instantiate the
aCTConfigARC or aCTConfigAPP object respectively with optional path as a
parameter. YAML gets parsed to a graph structure made of dictionaries
(from mappings) and lists (from sequences).

aCT further transforms dictionaries to objects of type DictObj to provide more
ergonomic access to key/value pairs with object attribute syntax rather than
the syntax for dictionary. DictObj also implements special methods for
behavior similar to other container classes.

Sample usage of values that are basic python types:
```python
config = aCTConfigARC()

# regular python value
print(config.voms.proxystoredir)

# value is a list, normal iteration
for cluster in config.downtime.clusters:
    print(cluster)
```

DictObj values can be used like other containers.
```python
# dictionary like iteration
for state, timeout in config.timeouts.aCT_state:
    print(f"timeout for state {state}: {timeout}")

# check if value exists
if config.jobs.maxtimerunning:
    ...

# alternatively, when string is needed
if "maxtimerunning" in config.jobs:
    ...

# default values
maxtimerunning = config.jobs.maxtimerunning or 1234
# or
maxtimerunning = config.jobs.get("maxtimerunning", 1234)
```

# Challenges
The fundamental goal is to replace dictionary access with a nicer
attribute access:
```python
# instead of this
config["jobs"]["maxtimerunning"]

# or this
config.get("jobs", {}).get("maxtimerunning", 1234)

# to have this
config.jobs.maxtimerunning
```

When an attribute doesn't exist, an empty DictObj is returned. This is
also true for a sequence of nested attributes. The first nonexistent attribute
will return an empty DictObj. The returned empty DictObj equals to False in
boolean context. The developer should always handle the case of nonexistent
value by either checking for existence or using the default value.
