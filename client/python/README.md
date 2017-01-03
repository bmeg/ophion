# py-ophion

A python client for making [Ophion](https://github.com/bmeg/ophion) queries.

# usage

```python
import ophion

# make an Ophion instance
O = ophion.Ophion('http://gaea-host')

# create a query
query = O.query('planet')

# go step by step
query.has('atmosphere', ['nitrogen', 'oxygen'])
query.outgoing('orbits')

# or chain them together
query.values('stellar mass').limit(5)

# finally, execute the query
results = O.execute(query)
print(results)

---> [383838.11, 92222222.222, ...]
```