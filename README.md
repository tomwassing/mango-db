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
prun -v -1 -np 4 python3 ./das.py
```