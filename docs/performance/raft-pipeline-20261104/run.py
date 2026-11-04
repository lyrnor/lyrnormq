"""One pipeline comparison cohort; identical load and limits, explicit perf clock."""
import importlib.util
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('raft_chain_run', HERE.parent / 'raft-chain-20261103/run.py')
chain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(chain)
chain.BASE = '/opt/rmqtt-write-pipeline/20261104'
chain.OUT = Path('/Volumes/data/rmqtt/target/raft-pipeline-results-20261104')
chain.prior.BASE = chain.BASE
chain.prior.OUT = chain.OUT
chain.prior.BROKER = chain.BASE + '/rmqtt'


if __name__ == '__main__':
    chain.main()
