# Map Reduce: Simplified Data Processing on Large Clusters

A general Programming model and implementation for processing large data sets.

- **Map Func** process kv pairs to intermediate kv pairs
- **Reduce func** merges all intermediate values associated with same intermediate key


## Claim
1. Many real world tasks can be expressed in this model.
2. Programs written in this functional style are automatically parallelized and executed on a large cluster of machines.
3. Hence it id useful to provide an abstraction to application programers, such that they can just specify their `map` and `reduce` functions.
4. The paper describes the implementation of a library (run-time system), that takes cares of:
  - partitoning the input data
  - scheduling program's execution across a set of machines
  - handling machine failures
  - manage inter-machine communication
5. Allows programmers without experience with parallel and districuted computing to easily parallelize programs and utilize resources of a large dist. system.

## Key ideas
1. MapReduce raises the abstraction at which programmers can create distributed and parallel programs.i i.e. *Simple* interface
2. Common issues in creating a distributed program i.e. partitioning, scheduling, communication and handling failures are abstracted away, the library can handle these concerns. i.e. *Powerful* interface.


## Word counting Example

Let's look at a concrete example. Imagine you have a large no of text files in a directory, and you want to calculate the occurence of each word. A non-parallel implementatin might look like:

```
word_count = {}
for each file f in dir:
  for each word w in f:
    if w in word_count:
      word_count[w] += 1
    else
      word_count[w] = 1
```

This is simple enough, but offers no scope for explicit parallelism.
In a map reduce paradigm, we can define our `map` and `reduce` function for word counting program as:

```
map(string key, string value):
  // key: file name
  // value: file contents

  for each word w in file:
    EmitIntermediate(w, "1")

```

```
reduce(string key, iterator values):
  // key: a word
  // values: a list of counts

  int result = 0
  for each v in values:
    result += ParseInt(v)
  Emit(AsString(result));
```

## Understanding the data transformations

Conceptually, the map and reduce functions asupplied by the user have associated types as:
  
  map     `(k1, v1)         --> list(k2, v2)`
  
  reduce  `(k2, list(v2))   --> list(v2)`

The input keys and values are drawn from a different domain than the output keys and values. Furthermore, the intermediate keys and values are from the same domain as the output keys and values.



## Implementation




