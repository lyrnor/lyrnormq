"""One unchanged 2000/s10k cohort for owned quorum/shared-buffer CPU comparison."""
import importlib.util
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('chain_run', HERE.parent / 'raft-chain-20261103/run.py')
chain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(chain)
chain.BASE = '/opt/lyrnormq-quorum-cpu/20261104'
chain.OUT = Path('/Volumes/data/lyrnormq/target/raft-quorum-cpu-results-20261104')
chain.prior.BASE = chain.BASE
chain.prior.OUT = chain.OUT
chain.prior.BROKER = chain.BASE + '/lyrnormq'

if __name__ == '__main__':
    chain.main()
