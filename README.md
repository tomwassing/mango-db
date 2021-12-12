# distributed-systems

## Installation
```sh
pip install -r requirements.txt
```

## Run test
```sh
pytest
```

### Run test with logging
```sh
pytest --log-cli-level=DEBUG
```

### Run on DAS
```
cd experiments
module load python/3.6.0
prun -v -1 -np 4 python3 ./perf_exp_1_das.py
```