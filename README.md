![MangoDB logo](logo.png)

MangoDB is a distributed key-value store, that orders its write operations before this value is read. This is an alternative to mainstream key-value stores, which normally order a stream of write operations immediately when it arrives in the system.

MangoDB is a result that is procuced from the course Distributed Systems at the VU (2021).

## Content of the program

### Experiments
The experiment folder consists of the bash scripts that are needed to run on the DAS5 envirnoment, aswell as the notebooks that are used to generate our results.

## Installation

Clone or download this repository (containing the MangoDB module) to a folder of your preference. Make sure the requirements above are installed to be able to run the program.

```sh
pip install -r requirements.txt
```

## Running MangoDB

### Running test
```sh
pytest
```

### Run test with logging
```sh
pytest --log-cli-level=DEBUG
```

### Run on DAS
```sh
cd experiments
module load python/3.6.0
prun -v -1 -np 4 python3 ./perf_exp_1_das.py
```

## Authors

* **Jurre Brandsen**
* **Bas Loyen**
* **Stylianos Rammos**
* **Tom Wassing**
* **Jasper Wessing**


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* Thanks to Sacheendra Talluri for the creation and the supervision of this project.
* Thanks to Alexandra Iosup for providing us with the source matterial necessary to complete the lab assignment.