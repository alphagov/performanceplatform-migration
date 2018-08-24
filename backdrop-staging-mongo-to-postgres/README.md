# how to use

```
cf push -f manifest.yml import-worker
# warning this will start thousands of cloudfoundry tasks
cat collections.txt | xargs -I {} bash -c 'cf run-task import-worker "python do_import.py {}" --name {}-x'
```
