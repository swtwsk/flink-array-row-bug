# Flink Array Row Bug

This is a reproducer of a bug reported in [FLINK-28591](https://issues.apache.org/jira/browse/FLINK-28591).

## How to reproduce

Run
```shell
mvn clean install exec:java -Dexec.mainClass="com.github.swtwsk.FlinkArrayRowBugJob"
```
and `cat` the file that got generated (named `part-...`).

---

Although in the INSERT we tried to insert
```roomsql
array[
    ('Field1', 'Value1'),
    ('Field2', 'Value2')
]
```
in the JSON that got printed to the generated file we get duplicate of the last inserted row:
```json
"bar": [
  {"foo1":"Field2", "foo2":"Value2"},
  {"foo1":"Field2", "foo2":"Value2"}
]
```
